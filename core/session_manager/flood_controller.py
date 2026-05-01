"""
core/session_manager/flood_controller.py — Per-session flood state tracker.

Uses monotonic timestamps so the state is immune to system clock changes.
Flood state is NOT persisted; it resets on restart (same behaviour as
the existing PremiumSessionPool).

Thread/async safety: CPython GIL makes single dict reads/writes atomic;
no lock needed for the common read path.  Writes are also single-threaded
because Pyrogram runs on one event loop, but we keep them under the same
semantics for clarity.
"""

from __future__ import annotations

import time
from typing import Dict, List, Optional

_FLOOD_BUFFER = 2.0   # extra seconds added after FloodWait


class FloodController:
    """
    Tracks per-session flood state using monotonic timestamps.

    Usage::

        fc = FloodController()
        fc.mark_flood(session_id, 30)          # session is flooded for 32s
        fc.is_flooded(session_id)              # → True
        fc.next_available_in([s1, s2, s3])    # → seconds until first free slot
    """

    def __init__(self) -> None:
        self._flood_until: Dict[str, float] = {}

    # ── Mutation ─────────────────────────────────────────────────────────────

    def mark_flood(self, session_id: str, wait_seconds: float) -> None:
        """
        Record that *session_id* is flooded for *wait_seconds* seconds
        (buffer is added automatically).  Overwrites existing value only
        if the new deadline is later.
        """
        deadline = time.monotonic() + wait_seconds + _FLOOD_BUFFER
        existing = self._flood_until.get(session_id)
        if existing is None or deadline > existing:
            self._flood_until[session_id] = deadline

    def clear_flood(self, session_id: str) -> None:
        """Manually clear flood state for *session_id* (e.g. on restart)."""
        self._flood_until.pop(session_id, None)

    # ── Queries (synchronous — safe CPython GIL dict reads) ──────────────────

    def is_flooded(self, session_id: str) -> bool:
        """Return True if the session is currently under a FloodWait."""
        deadline = self._flood_until.get(session_id)
        if deadline is None:
            return False
        if time.monotonic() >= deadline:
            # Expired — clean up lazily
            self._flood_until.pop(session_id, None)
            return False
        return True

    def seconds_until_free(self, session_id: str) -> float:
        """
        Return seconds until *session_id* is free.
        Returns 0.0 if it is already free.
        """
        deadline = self._flood_until.get(session_id)
        if deadline is None:
            return 0.0
        remaining = deadline - time.monotonic()
        return max(0.0, remaining)

    def next_available_in(self, session_ids: List[str]) -> Optional[float]:
        """
        Given a list of session IDs, return:
          - 0.0           — at least one session is available right now
          - N (float)     — all are flooded; N = seconds until the earliest one frees
          - None          — list is empty

        Used by the caller to decide whether to wait or give up.
        """
        if not session_ids:
            return None

        now = time.monotonic()
        min_wait: Optional[float] = None

        for sid in session_ids:
            deadline = self._flood_until.get(sid)
            if deadline is None or now >= deadline:
                return 0.0      # at least one is free right now
            wait = deadline - now
            if min_wait is None or wait < min_wait:
                min_wait = wait

        return min_wait   # all flooded; return soonest release


# Module-level singleton
flood_controller = FloodController()
