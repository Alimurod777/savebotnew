"""
core/session_manager/session_registry.py — CRUD + JSON persistence for SessionRecord.

Storage: data/session_manager/sessions.json
All mutations are serialised through asyncio.Lock.
Read-only queries are synchronous (safe for tight selection loops).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import Dict, List, Optional, Tuple

from .models import SessionRecord, SessionType

logger = logging.getLogger(__name__)

_DATA_DIR   = os.path.join("data", "session_manager")
_SESSIONS_FILE = os.path.join(_DATA_DIR, "sessions.json")


class SessionRegistry:
    """
    In-memory store for SessionRecord objects with JSON persistence.

    All write operations are protected by an asyncio.Lock.
    Read operations (get_all, get_by_owner, …) are synchronous and lock-free —
    safe on CPython's single-threaded event loop.
    """

    def __init__(self) -> None:
        self._records: Dict[str, SessionRecord] = {}   # session_id → record
        self._lock = asyncio.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def load(self) -> None:
        """
        Load records from disk.  Non-fatal: missing file → empty registry.
        Called once at startup (inside the running event loop).
        """
        os.makedirs(_DATA_DIR, exist_ok=True)
        if not os.path.exists(_SESSIONS_FILE):
            logger.info("SessionRegistry: no sessions file found, starting empty")
            return
        try:
            with open(_SESSIONS_FILE, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if not isinstance(raw, list):
                logger.warning("SessionRegistry: sessions.json is not a list, ignoring")
                return
            loaded = 0
            for entry in raw:
                try:
                    rec = SessionRecord.from_dict(entry)
                    self._records[rec.session_id] = rec
                    loaded += 1
                except Exception as e:
                    logger.warning("SessionRegistry: skipping bad entry %s: %s", entry, e)
            logger.info("SessionRegistry: loaded %d session(s)", loaded)
        except Exception as e:
            logger.warning("SessionRegistry: failed to load sessions.json: %s", e)

    async def save(self) -> None:
        """Persist current records to disk (caller must hold _lock)."""
        os.makedirs(_DATA_DIR, exist_ok=True)
        try:
            data = [rec.to_dict() for rec in self._records.values()]
            tmp = _SESSIONS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, ensure_ascii=False)
            os.replace(tmp, _SESSIONS_FILE)
        except Exception as e:
            logger.error("SessionRegistry: failed to save sessions.json: %s", e)

    # ── Mutations (all async, all under lock) ─────────────────────────────────

    async def add(self, record: SessionRecord) -> None:
        """Add a new session record and persist."""
        async with self._lock:
            self._records[record.session_id] = record
            await self.save()
        logger.info(
            "SessionRegistry: added %s session_id=%s phone=%s",
            record.type.value, record.session_id[:8], record.phone,
        )

    async def remove(self, session_id: str) -> bool:
        """Remove by session_id. Returns True if found and removed."""
        async with self._lock:
            if session_id not in self._records:
                return False
            del self._records[session_id]
            await self.save()
        logger.info("SessionRegistry: removed session_id=%s", session_id[:8])
        return True

    async def update_field(self, session_id: str, **kwargs) -> bool:
        """
        Update one or more persisted fields on a record.
        Accepted kwargs: enabled, allow_system_use, allow_borrow, max_parallel_tasks.
        Returns True if found.
        """
        async with self._lock:
            rec = self._records.get(session_id)
            if rec is None:
                return False
            for key, val in kwargs.items():
                if hasattr(rec, key):
                    setattr(rec, key, val)
                else:
                    logger.warning("SessionRegistry.update_field: unknown field %s", key)
            await self.save()
        return True

    async def disable_session(self, session_id: str) -> Tuple[bool, str]:
        """
        Disable a session.  Refuses if type == USER_OWNED.
        Returns (success, message).
        """
        async with self._lock:
            rec = self._records.get(session_id)
            if rec is None:
                return False, "Session not found"
            if rec.type == SessionType.USER_OWNED:
                return False, "Cannot disable USER_OWNED session via owner controls"
            rec.enabled = False
            await self.save()
        return True, "Disabled"

    async def enable_session(self, session_id: str) -> bool:
        """Enable a previously disabled session."""
        return await self.update_field(session_id, enabled=True)

    async def disable_all_global(self) -> int:
        """
        Disable all GLOBAL sessions.  Returns count disabled.
        Used for emergency shutdown of system sessions.
        """
        async with self._lock:
            count = 0
            for rec in self._records.values():
                if rec.type == SessionType.GLOBAL and rec.enabled:
                    rec.enabled = False
                    count += 1
            if count:
                await self.save()
        logger.info("SessionRegistry: disabled %d GLOBAL session(s)", count)
        return count

    # ── Read-only queries (synchronous, lock-free) ────────────────────────────

    def get_all(self) -> List[SessionRecord]:
        """All records, in insertion order."""
        return list(self._records.values())

    def get(self, session_id: str) -> Optional[SessionRecord]:
        """Look up by exact session_id."""
        return self._records.get(session_id)

    def find_by_prefix(self, prefix: str) -> Optional[SessionRecord]:
        """Find first record whose session_id starts with *prefix* (8-char UUID prefix)."""
        for rec in self._records.values():
            if rec.session_id.startswith(prefix):
                return rec
        return None

    def get_by_owner(self, user_id: int) -> List[SessionRecord]:
        """
        All enabled records owned by *user_id*
        (USER_OWNED + DEDICATED + BORROWABLE with owner == user_id).
        """
        return [
            r for r in self._records.values()
            if r.owner_user_id == user_id and r.enabled
        ]

    def get_user_owned(self, user_id: int) -> Optional[SessionRecord]:
        """First enabled USER_OWNED record for *user_id*."""
        for r in self._records.values():
            if r.type == SessionType.USER_OWNED and r.owner_user_id == user_id and r.enabled:
                return r
        return None

    def get_dedicated(self, user_id: int) -> Optional[SessionRecord]:
        """First enabled DEDICATED record for *user_id*."""
        for r in self._records.values():
            if r.type == SessionType.DEDICATED and r.owner_user_id == user_id and r.enabled:
                return r
        return None

    def get_borrowable(self, exclude_owner: int) -> List[SessionRecord]:
        """
        All enabled BORROWABLE records whose owner is NOT *exclude_owner*
        and allow_borrow is True.
        """
        return [
            r for r in self._records.values()
            if r.type == SessionType.BORROWABLE
            and r.enabled
            and r.allow_borrow
            and r.owner_user_id != exclude_owner
        ]

    def get_global(self) -> List[SessionRecord]:
        """All enabled GLOBAL records with allow_system_use=True."""
        return [
            r for r in self._records.values()
            if r.type == SessionType.GLOBAL and r.enabled and r.allow_system_use
        ]

    def generate_session_id(self) -> str:
        """Generate a new UUID4 session_id that is not already in use."""
        while True:
            sid = str(uuid.uuid4())
            if sid not in self._records:
                return sid
