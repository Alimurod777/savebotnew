"""
core/session_manager/models.py — Data models for the production-grade session manager.

SessionType controls how a session is selected and who can use it:
  USER_OWNED  — bound exclusively to owner (only that user can use it)
  DEDICATED   — permanently assigned to one user
  BORROWABLE  — owner's session, others may borrow when not busy
  GLOBAL      — system-owned, available to all users
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class SessionType(Enum):
    USER_OWNED  = "user_owned"   # Only owner uses it; owner commands cannot disable it
    DEDICATED   = "dedicated"    # Permanently assigned to one user
    BORROWABLE  = "borrowable"   # Owner's session; others may borrow when not busy
    GLOBAL      = "global"       # System-owned; available to all users


@dataclass
class SessionRecord:
    """
    Full session entry: persisted fields + runtime-only state.

    Persisted to JSON:
      session_id, session_string, phone, type, owner_user_id,
      enabled, allow_system_use, allow_borrow, max_parallel_tasks

    Runtime-only (NOT in JSON):
      current_tasks, flood_until, _task_lock
    """

    # ── Persisted ────────────────────────────────────────────────────────────
    session_id:        str                # UUID4
    session_string:    str
    phone:             str                # "+998..." or ""
    type:              SessionType
    owner_user_id:     Optional[int]      # None for GLOBAL
    enabled:           bool = True
    allow_system_use:  bool = True        # system may select this session
    allow_borrow:      bool = False       # non-owners may borrow (BORROWABLE only)
    max_parallel_tasks: int = 3

    # ── Runtime-only (excluded from asdict / JSON) ────────────────────────
    current_tasks:  int = field(default=0,    repr=False, compare=False)
    flood_until:    Optional[float] = field(default=None, repr=False, compare=False)
    _task_lock:     Optional[asyncio.Lock] = field(default=None, repr=False, compare=False)

    def get_lock(self) -> asyncio.Lock:
        """Return (or lazily create) the per-session asyncio.Lock."""
        if self._task_lock is None:
            self._task_lock = asyncio.Lock()
        return self._task_lock

    def to_dict(self) -> dict:
        """Serialise persisted fields only (runtime state excluded)."""
        return {
            "session_id":         self.session_id,
            "session_string":     self.session_string,
            "phone":              self.phone,
            "type":               self.type.value,
            "owner_user_id":      self.owner_user_id,
            "enabled":            self.enabled,
            "allow_system_use":   self.allow_system_use,
            "allow_borrow":       self.allow_borrow,
            "max_parallel_tasks": self.max_parallel_tasks,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SessionRecord":
        """Deserialise from JSON dict; runtime fields start at defaults."""
        return cls(
            session_id=        data["session_id"],
            session_string=    data["session_string"],
            phone=             data.get("phone", ""),
            type=              SessionType(data["type"]),
            owner_user_id=     data.get("owner_user_id"),
            enabled=           data.get("enabled", True),
            allow_system_use=  data.get("allow_system_use", True),
            allow_borrow=      data.get("allow_borrow", False),
            max_parallel_tasks=data.get("max_parallel_tasks", 3),
        )
