"""
core/mongo_cache.py - Smart In-Memory Cache Layer for MongoDB.

WORKFLOW:
    1️⃣ On first user access → Load user data from MongoDB → Store in RAM cache
    2️⃣ During runtime → Use ONLY cached data → No repeated DB queries
    3️⃣ Write-back policy → Save to MongoDB ONLY when:
       • user settings change
       • session added/removed
       • graceful shutdown

    If MongoDB is DOWN → System continues using local sessions + RAM cache.

This prevents DB overload when many users use the system simultaneously.

ARCHITECTURE:
    - CachedUser holds all per-user state in RAM
    - MongoDB is queried AT MOST ONCE per user per bot lifetime
    - Premium detection results are cached with TTL (not written to MongoDB)
    - Session string changes trigger immediate write-back (critical data)
    - Settings changes trigger immediate write-back
    - flush_all() writes all dirty records (call on shutdown)
"""

import asyncio
import logging
import time
from typing import Dict, Optional, Any, Set
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Premium status cache TTL — re-check via MTProto after this many seconds.
# This is an in-memory-only cache; premium status is NOT persisted to MongoDB.
PREMIUM_CACHE_TTL = 300  # 5 minutes


@dataclass
class CachedUser:
    """In-memory representation of a user record."""
    chat_id: int
    session_string: Optional[str] = None
    logged_in: bool = False
    is_premium: Optional[bool] = None       # None = not yet checked
    premium_checked_at: float = 0.0         # monotonic timestamp
    extra: Dict[str, Any] = field(default_factory=dict)
    dirty: bool = False                     # True = needs write-back to MongoDB


class MongoCache:
    """
    Smart in-memory cache for MongoDB user data.

    RULES:
    - MongoDB is queried AT MOST ONCE per user per bot session
    - All runtime reads come from RAM
    - Writes are flushed to MongoDB only on mutation or shutdown
    - If MongoDB is unavailable, system continues on RAM + local files

    Usage:
        from core.mongo_cache import mongo_cache

        user = await mongo_cache.get_user(chat_id)
        session = await mongo_cache.get_session(chat_id)
        await mongo_cache.set_session(chat_id, session_str, logged_in=True)
        await mongo_cache.flush_all()  # on shutdown
    """

    def __init__(self) -> None:
        self._users: Dict[int, CachedUser] = {}
        self._loaded_users: Set[int] = set()    # IDs we've attempted to load
        self._lock: Optional[asyncio.Lock] = None
        self._bound_loop_id: Optional[int] = None

    # ==================== LOCK MANAGEMENT ====================

    def _get_lock(self) -> asyncio.Lock:
        """Get or create lock bound to the current event loop.

        Handles loop changes (e.g. Colab restart) by resetting the lock.
        """
        try:
            loop_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return asyncio.Lock()

        if self._bound_loop_id is not None and self._bound_loop_id != loop_id:
            logger.debug("MongoCache: event loop changed, resetting lock")
            self._lock = None

        self._bound_loop_id = loop_id

        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    # ==================== INTERNAL LOAD ====================

    async def _ensure_loaded(self, chat_id: int) -> CachedUser:
        """Load user from MongoDB on first access, then return cached copy.

        This is the ONLY place where MongoDB is queried for user data.
        Subsequent calls return the RAM-cached CachedUser immediately.
        """
        if chat_id in self._users:
            return self._users[chat_id]

        if chat_id in self._loaded_users:
            # Already tried loading — user doesn't exist in DB. Return empty.
            user = CachedUser(chat_id=chat_id)
            self._users[chat_id] = user
            return user

        self._loaded_users.add(chat_id)

        # Attempt MongoDB load (ONE TIME)
        try:
            from database.async_db import get_sessions_collection
            coll = get_sessions_collection()
            if coll is not None:
                doc = await coll.find_one({'chat_id': chat_id})
                if doc:
                    user = CachedUser(
                        chat_id=chat_id,
                        session_string=doc.get('session'),
                        logged_in=doc.get('logged_in', False),
                        extra={
                            k: v for k, v in doc.items()
                            if k not in ('_id', 'chat_id', 'session', 'logged_in')
                        },
                    )
                    self._users[chat_id] = user
                    logger.debug("MongoCache: loaded user %d from MongoDB", chat_id)
                    return user
        except Exception as e:
            logger.debug("MongoCache: MongoDB load failed for %d: %s", chat_id, e)

        # No data found or MongoDB unavailable — create empty entry
        user = CachedUser(chat_id=chat_id)
        self._users[chat_id] = user
        return user

    # ==================== PUBLIC READ API ====================

    async def get_user(self, chat_id: int) -> CachedUser:
        """Get user data.  Loads from MongoDB on first access only."""
        async with self._get_lock():
            return await self._ensure_loaded(chat_id)

    async def get_session(self, chat_id: int) -> Optional[str]:
        """Get user's session string from RAM cache."""
        user = await self.get_user(chat_id)
        return user.session_string

    async def is_logged_in(self, chat_id: int) -> bool:
        """Check if user is logged in (RAM cache)."""
        user = await self.get_user(chat_id)
        return user.logged_in

    async def get_premium_status(self, chat_id: int) -> Optional[bool]:
        """Get cached premium status.

        Returns:
            True/False if cached and TTL is valid.
            None if never checked or TTL expired (caller should do live check).
        """
        user = await self.get_user(chat_id)
        if user.is_premium is not None:
            if time.monotonic() - user.premium_checked_at < PREMIUM_CACHE_TTL:
                return user.is_premium
        return None

    # ==================== PUBLIC WRITE API ====================

    async def set_premium_status(self, chat_id: int, is_premium: bool) -> None:
        """Cache a live premium detection result.

        Premium status is ephemeral (RAM only) — NOT written to MongoDB.
        It's re-checked via MTProto after PREMIUM_CACHE_TTL expires.
        """
        user = await self.get_user(chat_id)
        user.is_premium = is_premium
        user.premium_checked_at = time.monotonic()
        # Intentionally NOT marking dirty — premium is RAM-only cache

    async def set_session(
        self,
        chat_id: int,
        session_string: Optional[str],
        logged_in: bool,
    ) -> None:
        """Update user session.  Immediately writes back to MongoDB.

        Session changes are CRITICAL data — flushed right away.
        """
        async with self._get_lock():
            user = await self._ensure_loaded(chat_id)
            user.session_string = session_string
            user.logged_in = logged_in
            user.dirty = True

        await self._write_back_user(chat_id)

    async def update_setting(self, chat_id: int, key: str, value: Any) -> None:
        """Update a user setting.  Immediately writes back to MongoDB."""
        async with self._get_lock():
            user = await self._ensure_loaded(chat_id)
            user.extra[key] = value
            user.dirty = True

        await self._write_back_user(chat_id)

    # ==================== WRITE-BACK ====================

    async def _write_back_user(self, chat_id: int) -> None:
        """Write a single dirty user record back to MongoDB.

        If MongoDB is unavailable, data stays in RAM and will be retried
        on the next explicit flush_all() or the next mutation.
        """
        user = self._users.get(chat_id)
        if user is None or not user.dirty:
            return

        try:
            from database.async_db import get_sessions_collection
            coll = get_sessions_collection()
            if coll is None:
                return  # MongoDB unavailable — data stays in RAM

            data: Dict[str, Any] = {
                'chat_id': chat_id,
                'session': user.session_string,
                'logged_in': user.logged_in,
            }
            data.update(user.extra)

            await coll.update_one(
                {'chat_id': chat_id},
                {'$set': data},
                upsert=True,
            )
            user.dirty = False
            logger.debug("MongoCache: wrote back user %d", chat_id)
        except Exception as e:
            logger.warning("MongoCache: write-back failed for %d: %s", chat_id, e)

    async def flush_all(self) -> None:
        """Write ALL dirty users to MongoDB.  Call on graceful shutdown."""
        dirty_ids = [uid for uid, u in self._users.items() if u.dirty]
        if not dirty_ids:
            return

        logger.info("MongoCache: flushing %d dirty users to MongoDB", len(dirty_ids))
        for chat_id in dirty_ids:
            await self._write_back_user(chat_id)

    # ==================== CACHE MANAGEMENT ====================

    async def invalidate(self, chat_id: int) -> None:
        """Evict user from cache.  Next access will reload from MongoDB."""
        self._users.pop(chat_id, None)
        self._loaded_users.discard(chat_id)

    def get_stats(self) -> dict:
        """Return cache statistics."""
        return {
            'cached_users': len(self._users),
            'dirty_users': sum(1 for u in self._users.values() if u.dirty),
            'loaded_users': len(self._loaded_users),
        }


# Module singleton
mongo_cache = MongoCache()
