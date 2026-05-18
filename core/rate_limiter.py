"""
core/rate_limiter.py — Per-user sliding window rate limiter.

Window: 10 seconds (configurable at runtime by owner).
Storage: in-memory only. Resets on restart.
Synchronous check() — no await needed.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Dict, Tuple

from core.role_manager import UserRole


# Default limits: (max_requests, window_seconds)
_DEFAULT_LIMITS: Dict[UserRole, Tuple[int, int]] = {
    UserRole.NEW_USER:    (1, 10),
    UserRole.NORMAL_USER: (3, 10),
    UserRole.VIP_USER:    (10, 10),
    UserRole.BANNED:      (0, 10),
}


class RateLimiter:
    def __init__(self) -> None:
        self._windows: Dict[int, deque] = {}
        self._limits: Dict[UserRole, Tuple[int, int]] = dict(_DEFAULT_LIMITS)

    def check(self, user_id: int, role: UserRole) -> Tuple[bool, int]:
        """
        Check if user is within rate limit.
        Returns (allowed, retry_after_seconds).
        retry_after=0 when allowed.
        """
        if role == UserRole.BANNED:
            return False, 0

        max_req, window_sec = self._limits.get(role, (1, 10))
        if max_req <= 0:
            return False, max(1, window_sec)

        now = time.monotonic()
        cutoff = now - window_sec

        if user_id not in self._windows:
            self._windows[user_id] = deque()
        dq = self._windows[user_id]

        # Remove expired timestamps
        while dq and dq[0] < cutoff:
            dq.popleft()

        if len(dq) >= max_req:
            # Oldest entry + window = when slot opens
            retry_after = int(dq[0] + window_sec - now) + 1
            return False, max(1, retry_after)

        dq.append(now)
        return True, 0

    def set_limit(self, role: UserRole, max_requests: int, window_seconds: int = 10) -> None:
        """Update limit for a role at runtime (owner command)."""
        self._limits[role] = (max_requests, window_seconds)

    def get_limit(self, role: UserRole) -> Tuple[int, int]:
        return self._limits.get(role, (1, 10))


# Module-level singleton
rate_limiter = RateLimiter()
