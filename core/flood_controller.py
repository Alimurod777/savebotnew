"""
core/flood_controller.py - Per-User Independent Flood Control.

RULES:
  ✅ Each user has their own asyncio.Lock (operations serialized per-user)
  ✅ Each user has independent FloodWait cooldown tracking
  ✅ One user hitting FloodWait does NOT affect other users
  ✅ Entity errors auto-recover (strip entities, retry)
  ✅ Fatal session errors propagate immediately (no retry)
  ✅ Idle user states are cleaned up to prevent memory leaks

ARCHITECTURE:
  - UserFloodState: per-user lock, cooldown, counters
  - FloodController.execute(): main entry point for all sends
  - flood_controller: module singleton

Usage:
    from core.flood_controller import flood_controller

    msg, err = await flood_controller.execute(
        user_id=12345,
        send_func=client.send_message,
        kwargs={'chat_id': target, 'text': 'Hello'},
    )
"""

import asyncio
import time
import logging
from typing import Dict, Optional, Callable, Any, Tuple

from pyrogram.errors import (
    FloodWait,
    RPCError,
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionRevoked,
    SessionExpired,
    UserDeactivated,
)

logger = logging.getLogger(__name__)

# Defaults
MAX_RETRIES = 3
FLOODWAIT_CAP = 120         # Max FloodWait seconds we'll honour
INTER_MESSAGE_DELAY = 0.3   # Minimum gap between sends for a single user

# Fatal errors that MUST propagate — session is permanently dead
FATAL_SESSION_ERRORS = (
    AuthKeyUnregistered, AuthKeyInvalid,
    SessionRevoked, SessionExpired, UserDeactivated,
)


class UserFloodState:
    """Per-user flood control state.  One instance per active user."""

    __slots__ = (
        'user_id', '_lock', '_bound_loop_id',
        'cooldown_until', 'last_send_time',
        'consecutive_floods', 'total_sent', 'total_errors',
    )

    def __init__(self, user_id: int) -> None:
        self.user_id = user_id
        self._lock: Optional[asyncio.Lock] = None
        self._bound_loop_id: Optional[int] = None
        self.cooldown_until: float = 0.0   # monotonic timestamp
        self.last_send_time: float = 0.0
        self.consecutive_floods: int = 0
        self.total_sent: int = 0
        self.total_errors: int = 0

    def get_lock(self) -> asyncio.Lock:
        """Get or create per-user lock bound to current event loop."""
        try:
            loop_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return asyncio.Lock()

        if self._bound_loop_id is not None and self._bound_loop_id != loop_id:
            self._lock = None
        self._bound_loop_id = loop_id

        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @property
    def is_cooling_down(self) -> bool:
        return time.monotonic() < self.cooldown_until

    @property
    def remaining_cooldown(self) -> float:
        return max(0.0, self.cooldown_until - time.monotonic())


class FloodController:
    """
    Per-user independent flood control and rate limiting.

    ISOLATION GUARANTEE:
      User A hitting FloodWait will pause User A only.
      User B, C, D continue operating without delay.

    ENTITY RECOVERY:
      If Telegram rejects entities (ENTITY_BOUNDS_INVALID, etc.),
      entities are stripped and the operation is retried.

    FATAL ERROR PROPAGATION:
      AuthKeyUnregistered, AuthKeyInvalid, SessionRevoked, SessionExpired,
      UserDeactivated are NEVER retried — they propagate immediately so the
      caller can invalidate the session.
    """

    def __init__(self) -> None:
        self._users: Dict[int, UserFloodState] = {}

    def _get_state(self, user_id: int) -> UserFloodState:
        """Get or create per-user flood state."""
        if user_id not in self._users:
            self._users[user_id] = UserFloodState(user_id)
        return self._users[user_id]

    async def execute(
        self,
        user_id: int,
        send_func: Callable,
        kwargs: dict,
        max_retries: int = MAX_RETRIES,
        inter_message_delay: float = INTER_MESSAGE_DELAY,
    ) -> Tuple[Optional[Any], Optional[str]]:
        """
        Execute a send operation with per-user flood control.

        Args:
            user_id:              User ID for isolation
            send_func:            Async callable (e.g. client.send_message)
            kwargs:               Arguments for send_func
            max_retries:          Max retry attempts
            inter_message_delay:  Minimum delay between messages for this user

        Returns:
            (result, error_message)
            result is the return value of send_func, or None on failure.
            error_message is None on success, or a human-readable string.

        Raises:
            AuthKeyUnregistered, AuthKeyInvalid, SessionRevoked,
            SessionExpired, UserDeactivated — fatal, propagated immediately.
        """
        state = self._get_state(user_id)

        async with state.get_lock():
            # Honour active per-user cooldown
            if state.is_cooling_down:
                remaining = state.remaining_cooldown
                logger.info(
                    "User %d: FloodWait cooldown, waiting %.1fs",
                    user_id, remaining,
                )
                await asyncio.sleep(remaining)

            # Inter-message delay (prevents burst-triggered FloodWait)
            elapsed = time.monotonic() - state.last_send_time
            if elapsed < inter_message_delay and state.last_send_time > 0:
                await asyncio.sleep(inter_message_delay - elapsed)

            # Make a mutable copy so entity-stripping doesn't affect caller
            kw = dict(kwargs)

            for attempt in range(max_retries):
                try:
                    result = await send_func(**kw)
                    state.last_send_time = time.monotonic()
                    state.consecutive_floods = 0
                    state.total_sent += 1
                    return result, None

                except FATAL_SESSION_ERRORS:
                    # NEVER retry — session is permanently dead
                    state.total_errors += 1
                    raise

                except FloodWait as e:
                    wait = min(
                        getattr(e, 'value', getattr(e, 'x', 30)),
                        FLOODWAIT_CAP,
                    )
                    state.consecutive_floods += 1
                    state.cooldown_until = time.monotonic() + wait

                    logger.warning(
                        "User %d: FloodWait %ds (consecutive: %d, attempt %d/%d)",
                        user_id, wait, state.consecutive_floods,
                        attempt + 1, max_retries,
                    )

                    if attempt < max_retries - 1:
                        await asyncio.sleep(wait)
                    else:
                        state.total_errors += 1
                        return None, f"FloodWait {wait}s exceeded {max_retries} retries"

                except RPCError as e:
                    err_upper = str(e).upper()

                    # Entity errors → strip entities and retry
                    if 'ENTITY' in err_upper or 'BOUNDS' in err_upper:
                        logger.warning("User %d: entity error: %s", user_id, e)
                        stripped = False
                        for key in ('entities', 'caption_entities'):
                            if key in kw:
                                kw = {k: v for k, v in kw.items() if k != key}
                                stripped = True
                                break
                        if stripped:
                            continue  # Retry without entities

                    if attempt < max_retries - 1:
                        logger.warning("User %d: RPC error: %s, retrying", user_id, e)
                        await asyncio.sleep(1)
                    else:
                        state.total_errors += 1
                        return None, str(e)

                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning("User %d: error: %s, retrying", user_id, e)
                        await asyncio.sleep(1)
                    else:
                        state.total_errors += 1
                        return None, str(e)

            return None, "Max retries exceeded"

    # ==================== STATS & CLEANUP ====================

    def get_user_stats(self, user_id: int) -> dict:
        """Per-user statistics."""
        state = self._get_state(user_id)
        return {
            'user_id': user_id,
            'total_sent': state.total_sent,
            'total_errors': state.total_errors,
            'consecutive_floods': state.consecutive_floods,
            'is_cooling_down': state.is_cooling_down,
            'remaining_cooldown': round(state.remaining_cooldown, 1),
        }

    def get_global_stats(self) -> dict:
        """Global flood controller statistics."""
        return {
            'tracked_users': len(self._users),
            'users_cooling_down': sum(
                1 for s in self._users.values() if s.is_cooling_down
            ),
            'total_sent': sum(s.total_sent for s in self._users.values()),
            'total_errors': sum(s.total_errors for s in self._users.values()),
        }

    def cleanup_idle(self, max_idle_seconds: float = 3600) -> int:
        """Remove idle user states to prevent memory leaks.

        Returns the number of states removed.
        """
        now = time.monotonic()
        to_remove = [
            uid for uid, state in self._users.items()
            if now - state.last_send_time > max_idle_seconds
            and not state.is_cooling_down
            and state.last_send_time > 0
        ]
        for uid in to_remove:
            del self._users[uid]
        return len(to_remove)


# Module singleton
flood_controller = FloodController()
