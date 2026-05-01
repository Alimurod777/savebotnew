"""
core/session_manager/borrow_manager.py — Concurrency-safe session slot management.

Provides atomic acquire/release of session slots so that concurrent uploads
from different users do not exceed a session's max_parallel_tasks.

Design:
  - One asyncio.Lock per session (lazy, created inside running event loop)
  - current_tasks counter lives on SessionRecord (runtime-only, not persisted)
  - Flood state is delegated to FloodController
  - Stale borrow detection: if a slot is held for > BORROW_TIMEOUT_SEC,
    it is automatically reclaimed on the next availability check
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional, Set, Tuple

from .flood_controller import flood_controller
from .models import SessionRecord

logger = logging.getLogger(__name__)

# Maximum time a borrow can be held before it's considered stale (10 min)
BORROW_TIMEOUT_SEC = 600.0


class BorrowManager:
    """
    Manages concurrent-task accounting and borrow tracking for sessions.

    All public methods are async so that the caller can await them from
    the event loop.  The per-session lock ensures atomicity of the
    check+increment/decrement pair.
    """

    def __init__(self) -> None:
        # session_id → set of user_ids currently holding a slot
        self._borrowers: Dict[str, Set[int]] = {}
        # session_id → {user_id: acquire_time_monotonic}
        self._acquire_times: Dict[str, Dict[int, float]] = {}

    # ── Synchronous fast check (for tight selection loops) ────────────────────

    def is_session_available(self, record: SessionRecord) -> bool:
        """
        Quick synchronous availability check (no lock).

        Returns True only if all of:
          - record.enabled
          - not flooded
          - current_tasks < max_parallel_tasks
        Also triggers stale borrow reclamation (best-effort).
        """
        if not record.enabled:
            return False
        if flood_controller.is_flooded(record.session_id):
            return False
        # Reclaim stale borrows before checking capacity
        self._reclaim_stale_sync(record)
        if record.current_tasks >= record.max_parallel_tasks:
            return False
        return True

    def _reclaim_stale_sync(self, record: SessionRecord) -> None:
        """
        Synchronous (no lock) scan for stale borrows.
        If a user has held a slot for > BORROW_TIMEOUT_SEC, reclaim it.
        This is best-effort — the async acquire/release methods are authoritative.
        """
        times = self._acquire_times.get(record.session_id)
        if not times:
            return
        now = time.monotonic()
        stale_users = [
            (uid, now - t) for uid, t in times.items()
            if (now - t) > BORROW_TIMEOUT_SEC
        ]
        if not stale_users:
            return
        for uid, held_for in stale_users:
            times.pop(uid, None)
            borrowers = self._borrowers.get(record.session_id)
            if borrowers:
                borrowers.discard(uid)
            if record.current_tasks > 0:
                record.current_tasks -= 1
            logger.warning(
                "BorrowManager: reclaimed stale borrow on session %s from user %d "
                "(held %.0fs > %ds)",
                record.session_id[:8], uid, held_for,
                BORROW_TIMEOUT_SEC,
            )

    # ── Async acquire / release ───────────────────────────────────────────────

    async def try_acquire(self, record: SessionRecord, user_id: int) -> bool:
        """
        Atomically attempt to acquire one slot on *record* for *user_id*.

        Returns True on success (caller must call release() when done).
        Returns False if the session is full, flooded, or disabled.
        """
        lock = record.get_lock()
        async with lock:
            if not record.enabled:
                return False
            if flood_controller.is_flooded(record.session_id):
                return False
            if record.current_tasks >= record.max_parallel_tasks:
                return False
            record.current_tasks += 1
            self._borrowers.setdefault(record.session_id, set()).add(user_id)
            self._acquire_times.setdefault(record.session_id, {})[user_id] = time.monotonic()
            return True

    async def release(self, record: SessionRecord, user_id: int) -> None:
        """
        Release one slot held by *user_id* on *record*.
        Safe to call even if acquire was not successful (no-op).
        """
        lock = record.get_lock()
        async with lock:
            if record.current_tasks > 0:
                record.current_tasks -= 1
            borrowers = self._borrowers.get(record.session_id)
            if borrowers:
                borrowers.discard(user_id)
            times = self._acquire_times.get(record.session_id)
            if times:
                times.pop(user_id, None)

    # ── Flood marking ─────────────────────────────────────────────────────────

    async def mark_flood(self, record: SessionRecord, wait_seconds: float) -> None:
        """
        Mark *record* as flooded for *wait_seconds* seconds and
        reset its current_tasks counter to 0 (all in-flight tasks
        will either fail or retry on another session).
        """
        flood_controller.mark_flood(record.session_id, wait_seconds)
        lock = record.get_lock()
        async with lock:
            record.current_tasks = 0
            self._borrowers.pop(record.session_id, None)
            self._acquire_times.pop(record.session_id, None)
        logger.info(
            "BorrowManager: session %s marked flooded for %.0fs",
            record.session_id[:8], wait_seconds,
        )

    # ── Diagnostics ───────────────────────────────────────────────────────────

    def borrowers(self, session_id: str) -> Set[int]:
        """Return the set of user IDs currently holding slots on *session_id*."""
        return set(self._borrowers.get(session_id, set()))


# Module-level singleton
borrow_manager = BorrowManager()
