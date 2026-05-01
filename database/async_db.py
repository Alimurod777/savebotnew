import logging
import time
from typing import Dict, Any, Tuple

_async_db_logger = logging.getLogger(__name__)

# Simple in-memory cache to reduce DB load
# chat_id -> (expiry_time, data)
_USER_CACHE: Dict[int, Tuple[float, Any]] = {}
CACHE_TTL = 600  # 10 minutes (local-first — cache is safe longer)

try:
    import motor.motor_asyncio
    _MOTOR_AVAILABLE = True
except ImportError:
    _MOTOR_AVAILABLE = False
    _async_db_logger.warning("motor not available — MongoDB disabled, using in-memory fallback")

try:
    from config import DB_URI
except Exception:
    DB_URI = None

# LAZY INITIALIZATION - Don't create client at import time!
# This prevents "Future attached to different loop" errors
_mongo_client = None
_db = None
_sessions_collection = None
_banned_users_collection = None
_sent_albums_collection = None
_mongo_available = False


def get_mongo_client():
    """Get or create MongoDB client - lazy initialization.
    
    PATCH: Returns None if MongoDB is unavailable instead of crashing.
    """
    global _mongo_client, _db, _sessions_collection, _banned_users_collection, _sent_albums_collection, _mongo_available
    
    if not _MOTOR_AVAILABLE or not DB_URI:
        _async_db_logger.debug("MongoStatus: unavailable (no motor or DB_URI)")
        return None
    
    if _mongo_client is None:
        try:
            _mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
                DB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
            )
            _db = _mongo_client.userdb
            _sessions_collection = _db.sessions
            _banned_users_collection = _db.banned_users
            _sent_albums_collection = _db.sent_albums
            _mongo_available = True
            _async_db_logger.info("MongoStatus: connected (lazy init)")
        except Exception as e:
            _async_db_logger.warning(f"MongoStatus: connection failed — {e}. Using in-memory fallback.")
            _mongo_available = False
            return None
    
    return _mongo_client


def get_db():
    """Get database instance"""
    get_mongo_client()
    return _db


def get_sessions_collection():
    """Get sessions collection"""
    get_mongo_client()
    return _sessions_collection


def get_banned_users_collection():
    """Get banned users collection"""
    get_mongo_client()
    return _banned_users_collection


def get_sent_albums_collection():
    """Get sent albums collection"""
    get_mongo_client()
    return _sent_albums_collection


# For backward compatibility - these will be None until first access
mongo_client = None
db = None
sessions_collection = None
banned_users_collection = None
sent_albums_collection = None


class AsyncDatabase:
    """Async database operations for the bot.
    
    PATCH: All methods are MongoDB-safe — they return sensible defaults
    (None, False, [], 0) when MongoDB is unavailable, preventing crashes.
    
    FALLBACK: When MongoDB is unavailable, delegates to LocalStorage (SQLite)
    for persistent storage. This ensures data survives across restarts
    even in Colab/Kaggle environments without MongoDB.
    """
    
    _local_storage = None
    
    @classmethod
    def _get_local(cls):
        """Lazy-load local storage fallback."""
        if cls._local_storage is None:
            try:
                from database.local_storage import LocalStorage, is_local_storage_available
                if is_local_storage_available():
                    cls._local_storage = LocalStorage
                    _async_db_logger.info("LocalStorage (SQLite) fallback loaded")
                else:
                    cls._local_storage = False  # Sentinel: tried but unavailable
            except ImportError:
                cls._local_storage = False
                _async_db_logger.debug("LocalStorage module not available")
        return cls._local_storage if cls._local_storage is not False else None
    
    @staticmethod
    def _get_sessions():
        return get_sessions_collection()
    
    @staticmethod
    def _get_banned():
        return get_banned_users_collection()
    
    @staticmethod
    def _get_albums():
        return get_sent_albums_collection()
    
    # ==================== Session Operations ====================
    
    @classmethod
    async def find_user(cls, chat_id: int):
        """Find a user by chat_id.

        Priority: in-memory cache → LocalStorage (SQLite) → MongoDB.
        LOCAL-FIRST: SQLite is the primary source of truth.
        Result is cached for CACHE_TTL seconds regardless of source.
        Cache is NEVER populated with None so failed lookups re-hit the DB.
        """
        now = time.time()

        # 1. Check in-memory cache
        if chat_id in _USER_CACHE:
            expiry, data = _USER_CACHE[chat_id]
            if now < expiry:
                return data
            else:
                del _USER_CACHE[chat_id]

        # 2. Try LocalStorage FIRST (primary source)
        local = cls._get_local()
        if local:
            try:
                data = await local.find_user(chat_id)
                if data is not None:
                    _USER_CACHE[chat_id] = (now + CACHE_TTL, data)
                    return data
            except Exception as e:
                _async_db_logger.debug(f"LocalStorage find_user failed: {e}")

        # 3. Fall back to MongoDB (secondary)
        coll = get_sessions_collection()
        if coll is not None:
            try:
                data = await coll.find_one({'chat_id': chat_id})
                if data is not None:
                    _USER_CACHE[chat_id] = (now + CACHE_TTL, data)
                    # Mirror to local so next time it's found locally
                    if local:
                        try:
                            _update = {}
                            if 'session' in data:
                                _update['session'] = data['session']
                            if 'logged_in' in data:
                                _update['logged_in'] = data['logged_in']
                            if 'phone' in data:
                                _update['phone'] = data['phone']
                            if 'role' in data:
                                _update['role'] = data['role']
                            if _update:
                                await local.update_user(chat_id, _update)
                            if data.get('expecting_post_limit'):
                                await local.set_expecting_post_limit(
                                    chat_id,
                                    data.get('post_limit_data') or {},
                                )
                        except Exception:
                            pass
                return data
            except Exception as e:
                _async_db_logger.warning(f"MongoDB find_user failed: {e}")

        return None

    @classmethod
    async def update_user(cls, chat_id: int, data: dict = None, **kwargs):
        """Update user info and invalidate cache.

        Accepts EITHER a dict or keyword arguments:
            await async_db.update_user(uid, {'session': None, 'logged_in': False})
            await async_db.update_user(uid, session=None, logged_in=False)

        LOCAL-FIRST write order:
          1. Invalidate in-memory cache (always, first)
          2. Write to LocalStorage (SQLite) — PRIMARY, synchronous
          3. Write to MongoDB — BACKGROUND (fire-and-forget via sync_manager)
        """
        # Merge dict and kwargs (dict takes precedence if both given)
        update_data = {}
        if data and isinstance(data, dict):
            update_data.update(data)
        if kwargs:
            update_data.update(kwargs)
        if not update_data:
            return

        # ALWAYS invalidate cache before any DB write
        _USER_CACHE.pop(chat_id, None)
        local_update = dict(update_data)
        mongo_update = dict(update_data)

        # 1. Write to LocalStorage FIRST (primary)
        local = cls._get_local()
        if local:
            try:
                await local.update_user(chat_id, local_update)
            except Exception as _le:
                _async_db_logger.warning(f"LocalStorage update_user failed: {_le}")

        # 2. Background sync to MongoDB (non-blocking)
        try:
            from database.sync_manager import sync_manager
            sync_manager.background_sync(
                "sessions",
                {"chat_id": chat_id},
                mongo_update
            )
        except Exception as _se:
            # Fallback: try direct Mongo write if sync_manager unavailable
            coll = get_sessions_collection()
            if coll is not None:
                try:
                    await coll.update_one(
                        {'chat_id': chat_id},
                        {'$set': mongo_update},
                        upsert=True
                    )
                except Exception as e:
                    _async_db_logger.debug(f"MongoDB update_user fallback failed: {e}")

    
    @classmethod
    async def get_all_logged_in_users(cls):
        """Get all logged-in users (for admin session command)"""
        local = cls._get_local()
        if local:
            try:
                return await local.get_all_logged_in_users()
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage get_all_logged_in_users failed: {e}")

        coll = get_sessions_collection()
        if coll is None:
            return []
        try:
            cursor = coll.find({'logged_in': True})
            return await cursor.to_list(length=None)
        except Exception as e:
            _async_db_logger.warning(f"MongoDB get_all_logged_in_users failed: {e}")
            return []
    
    @classmethod
    async def count_logged_in_users(cls) -> int:
        """Count logged-in users"""
        local = cls._get_local()
        if local:
            try:
                return await local.count_logged_in_users()
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage count_logged_in_users failed: {e}")

        coll = get_sessions_collection()
        if coll is None:
            return 0
        try:
            return await coll.count_documents({'logged_in': True})
        except Exception as e:
            _async_db_logger.warning(f"MongoDB count_logged_in_users failed: {e}")
            return 0
    
    @classmethod
    async def insert_user(cls, chat_id: int):
        """Insert a new user if not exists. LOCAL-FIRST."""
        # 1. Try LocalStorage first
        local = cls._get_local()
        if local:
            try:
                existing = await local.find_user(chat_id)
                if existing:
                    return existing
                result = await local.insert_user(chat_id)
                # Background sync to Mongo
                try:
                    from database.sync_manager import sync_manager
                    sync_manager.background_sync(
                        "sessions",
                        {"chat_id": chat_id},
                        {"chat_id": chat_id, "logged_in": False, "session": None}
                    )
                except Exception:
                    pass
                return result
            except Exception as e:
                _async_db_logger.debug(f"LocalStorage insert_user failed: {e}")

        # 2. Fallback to MongoDB
        coll = get_sessions_collection()
        if coll is None:
            return None
        try:
            existing = await coll.find_one({'chat_id': chat_id})
            if existing is None:
                await coll.insert_one({'chat_id': chat_id, 'logged_in': False, 'session': None})
            return await coll.find_one({'chat_id': chat_id})
        except Exception as e:
            _async_db_logger.warning(f"MongoDB insert_user failed: {e}")
            return None
    
    @staticmethod
    async def update_user_by_id(doc_id, data: dict):
        """Update user by document _id"""
        coll = get_sessions_collection()
        if coll is None:
            return
        try:
            await coll.update_one(
                {'_id': doc_id},
                {'$set': data}
            )
        except Exception as e:
            _async_db_logger.warning(f"MongoDB update_user_by_id failed: {e}")
    
    @classmethod
    async def unset_user_fields(cls, chat_id: int, fields: list):
        """Unset specific fields from user document"""
        _USER_CACHE.pop(chat_id, None)

        local = cls._get_local()
        if local:
            try:
                await local.unset_user_fields(chat_id, fields)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage unset_user_fields failed: {e}")

        try:
            from database.sync_manager import sync_manager

            sync_manager.background_sync(
                "sessions",
                {"chat_id": chat_id},
                {field: "" for field in fields},
                op="unset",
            )
        except Exception as e:
            _async_db_logger.warning(f"MongoDB unset_user_fields enqueue failed: {e}")
    
    # ==================== Ban System Operations ====================
    
    @classmethod
    async def ban_user(cls, user_id: int, banned_by: int):
        """Ban a user - stores in banned_users collection"""
        local = cls._get_local()
        if local:
            try:
                await local.ban_user(user_id, banned_by)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage ban_user failed: {e}")

        try:
            from database.sync_manager import sync_manager

            sync_manager.background_sync(
                "banned_users",
                {'user_id': user_id},
                {
                    'user_id': user_id,
                    'banned_by': banned_by,
                    'banned': True,
                },
                op="set",
            )
        except Exception as e:
            _async_db_logger.warning(f"MongoDB ban_user enqueue failed: {e}")

    @classmethod
    async def unban_user(cls, user_id: int):
        """Unban a user - removes from banned_users collection"""
        local = cls._get_local()
        if local:
            try:
                await local.unban_user(user_id)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage unban_user failed: {e}")

        try:
            from database.sync_manager import sync_manager

            sync_manager.background_sync(
                "banned_users",
                {'user_id': user_id},
                {},
                op="delete_one",
            )
        except Exception as e:
            _async_db_logger.warning(f"MongoDB unban_user enqueue failed: {e}")
    
    @classmethod
    async def is_banned(cls, user_id: int) -> bool:
        """Check if a user is banned"""
        local = cls._get_local()
        if local:
            try:
                return await local.is_banned(user_id)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage is_banned failed: {e}")

        coll = get_banned_users_collection()
        if coll is None:
            return False
        try:
            result = await coll.find_one({'user_id': user_id, 'banned': True})
            return result is not None
        except Exception as e:
            _async_db_logger.warning(f"MongoDB is_banned failed: {e}")
            return False

    @classmethod
    async def get_all_banned(cls):
        """Get all banned users"""
        local = cls._get_local()
        if local:
            try:
                return await local.get_all_banned()
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage get_all_banned failed: {e}")

        coll = get_banned_users_collection()
        if coll is None:
            return []
        try:
            cursor = coll.find({'banned': True})
            return await cursor.to_list(length=None)
        except Exception as e:
            _async_db_logger.warning(f"MongoDB get_all_banned failed: {e}")
            return []
    
    # ==================== Task State Operations ====================
    
    @classmethod
    async def set_expecting_post_limit(cls, chat_id: int, data: dict):
        """Set post limit expectation data"""
        local = cls._get_local()
        if local:
            try:
                await local.set_expecting_post_limit(chat_id, data)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage set_expecting_post_limit failed: {e}")

        try:
            from database.sync_manager import sync_manager

            sync_manager.background_sync(
                "sessions",
                {'chat_id': chat_id},
                {
                    'expecting_post_limit': True,
                    'post_limit_data': data,
                },
                op="set",
            )
        except Exception as e:
            _async_db_logger.warning(f"MongoDB set_expecting_post_limit enqueue failed: {e}")

    @classmethod
    async def clear_expecting_post_limit(cls, chat_id: int):
        """Clear post limit expectation"""
        local = cls._get_local()
        if local:
            try:
                await local.clear_expecting_post_limit(chat_id)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage clear_expecting_post_limit failed: {e}")

        try:
            from database.sync_manager import sync_manager

            sync_manager.background_sync(
                "sessions",
                {'chat_id': chat_id},
                {'expecting_post_limit': "", 'post_limit_data': ""},
                op="unset",
            )
        except Exception as e:
            _async_db_logger.warning(f"MongoDB clear_expecting_post_limit enqueue failed: {e}")

    @classmethod
    async def get_expecting_post_limit(cls, chat_id: int):
        """Get user expecting post limit data"""
        local = cls._get_local()
        if local:
            try:
                return await local.get_expecting_post_limit(chat_id)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage get_expecting_post_limit failed: {e}")

        coll = get_sessions_collection()
        if coll is None:
            return None
        try:
            data = await coll.find_one({
                'chat_id': chat_id,
                'expecting_post_limit': True
            })
            if data and local:
                try:
                    await local.set_expecting_post_limit(
                        chat_id,
                        data.get('post_limit_data') or {},
                    )
                except Exception:
                    pass
            return data
        except Exception as e:
            _async_db_logger.warning(f"MongoDB get_expecting_post_limit failed: {e}")
            return None
    
    # ==================== Sent Albums Tracking (Duplicate Prevention) ====================
    
    @classmethod
    async def is_album_sent(cls, user_id: int, media_group_id: str) -> bool:
        """Check if an album was already sent to a user."""
        # Check in-memory cache first (always available)
        if is_album_in_cache(user_id, media_group_id):
            return True

        local = cls._get_local()
        if local:
            try:
                if await local.is_album_sent(user_id, media_group_id):
                    add_to_sent_albums_cache(user_id, media_group_id)
                    return True
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage is_album_sent failed: {e}")

        coll = get_sent_albums_collection()
        if coll is None:
            return False
        try:
            result = await coll.find_one({
                'user_id': user_id,
                'media_group_id': media_group_id
            })
            if result is not None:
                add_to_sent_albums_cache(user_id, media_group_id)
                if local:
                    try:
                        await local.mark_album_sent(
                            user_id,
                            media_group_id,
                            result.get('source_chat_id'),
                        )
                    except Exception:
                        pass
                return True
            return False
        except Exception as e:
            _async_db_logger.warning(f"MongoDB is_album_sent failed: {e}")
            return False
    
    @classmethod
    async def mark_album_sent(cls, user_id: int, media_group_id: str, source_chat_id: int = None):
        """Mark an album as sent to a user."""
        from datetime import datetime
        # Always update in-memory cache
        add_to_sent_albums_cache(user_id, media_group_id)

        local = cls._get_local()
        if local:
            try:
                await local.mark_album_sent(user_id, media_group_id, source_chat_id)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage mark_album_sent failed: {e}")

        try:
            from database.sync_manager import sync_manager

            sync_manager.background_sync(
                "sent_albums",
                {'user_id': user_id, 'media_group_id': media_group_id},
                {
                    'user_id': user_id,
                    'media_group_id': media_group_id,
                    'source_chat_id': source_chat_id,
                    'sent_at': datetime.utcnow(),
                },
                op="set",
            )
        except Exception as e:
            _async_db_logger.warning(f"MongoDB mark_album_sent enqueue failed: {e}")
    
    @classmethod
    async def get_user_sent_albums(cls, user_id: int) -> list:
        """Get all album IDs sent to a user"""
        cached = set(get_sent_albums_cache(user_id))
        local = cls._get_local()
        if local:
            try:
                local_albums = set(await local.get_user_sent_albums(user_id))
                cached.update(local_albums)
                for media_group_id in local_albums:
                    add_to_sent_albums_cache(user_id, media_group_id)
                if local_albums:
                    return sorted(cached)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage get_user_sent_albums failed: {e}")

        coll = get_sent_albums_collection()
        if coll is None:
            return sorted(cached)
        try:
            cursor = coll.find({'user_id': user_id})
            albums = await cursor.to_list(length=None)
            mongo_albums = []
            for album in albums:
                media_group_id = album.get('media_group_id')
                if not media_group_id:
                    continue
                mongo_albums.append(media_group_id)
                add_to_sent_albums_cache(user_id, media_group_id)
                if local:
                    try:
                        await local.mark_album_sent(
                            user_id,
                            media_group_id,
                            album.get('source_chat_id'),
                        )
                    except Exception:
                        pass
            cached.update(mongo_albums)
            return sorted(cached)
        except Exception as e:
            _async_db_logger.warning(f"MongoDB get_user_sent_albums failed: {e}")
            return sorted(cached)
    
    @classmethod
    async def clear_user_sent_albums(cls, user_id: int):
        """Clear sent album history for a user"""
        clear_sent_albums_cache(user_id)

        local = cls._get_local()
        if local:
            try:
                await local.clear_user_sent_albums(user_id)
            except Exception as e:
                _async_db_logger.warning(f"LocalStorage clear_user_sent_albums failed: {e}")

        try:
            from database.sync_manager import sync_manager

            sync_manager.background_sync(
                "sent_albums",
                {'user_id': user_id},
                {},
                op="delete_many",
            )
        except Exception as e:
            _async_db_logger.warning(f"MongoDB clear_user_sent_albums enqueue failed: {e}")


# Create a singleton instance
async_db = AsyncDatabase()


# In-memory fallback cache for sent albums
_sent_albums_cache: dict = {}


def get_sent_albums_cache(user_id: int) -> set:
    """Get in-memory cache of sent albums for a user"""
    if user_id not in _sent_albums_cache:
        _sent_albums_cache[user_id] = set()
    return _sent_albums_cache[user_id]


def add_to_sent_albums_cache(user_id: int, media_group_id: str):
    """Add album to in-memory cache"""
    if user_id not in _sent_albums_cache:
        _sent_albums_cache[user_id] = set()
    _sent_albums_cache[user_id].add(media_group_id)


def is_album_in_cache(user_id: int, media_group_id: str) -> bool:
    """Check if album is in cache"""
    return user_id in _sent_albums_cache and media_group_id in _sent_albums_cache[user_id]


def clear_sent_albums_cache(user_id: int):
    """Clear in-memory cache for a user"""
    if user_id in _sent_albums_cache:
        _sent_albums_cache[user_id].clear()
