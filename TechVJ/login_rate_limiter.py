"""
Login Rate Limiter for Telegram MTProto

Prevents Telegram from blocking SMS codes due to too many login attempts
from the same IP address.

PROBLEM:
- Multiple users trying to login simultaneously from one server
- Telegram sees: same IP + many login attempts = suspicious
- Result: SMS codes stop arriving, FloodWait errors

SOLUTION:
- Queue login attempts
- Allow max N concurrent logins
- Enforce cooldown between logins
- Track per-user attempt limits

Usage:
    from TechVJ.login_rate_limiter import login_limiter
    
    # Before starting login:
    async with login_limiter.acquire(user_id):
        # Perform login here
        await do_login(...)
    
    # Or check manually:
    can_login, wait_time, reason = await login_limiter.can_login(user_id)
    if not can_login:
        await message.reply(f"Please wait {wait_time:.0f} seconds. {reason}")
"""

import asyncio
import time
import logging
from typing import Dict, Tuple, Optional
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

from config import (
    MAX_CONCURRENT_LOGINS,
    LOGIN_COOLDOWN_SECONDS,
    USER_LOGIN_COOLDOWN,
    MAX_LOGIN_ATTEMPTS_PER_HOUR,
)

logger = logging.getLogger(__name__)


@dataclass
class UserLoginState:
    """Track login state for a specific user."""
    last_attempt: float = 0
    attempts_this_hour: int = 0
    hour_start: float = field(default_factory=time.time)
    is_logging_in: bool = False
    next_allowed_at_override: float = 0
    
    def reset_hour_if_needed(self) -> None:
        """Reset hourly counter if hour has passed."""
        now = time.time()
        if now - self.hour_start >= 3600:
            self.attempts_this_hour = 0
            self.hour_start = now
    
    def record_attempt(self) -> None:
        """Record a login attempt."""
        self.reset_hour_if_needed()
        self.last_attempt = time.time()
        self.attempts_this_hour += 1
        self.next_allowed_at_override = 0
    
    def time_until_can_retry(self, user_cooldown: float) -> float:
        """Get seconds until user can try again."""
        self.reset_hour_if_needed()
        now = time.time()
        next_allowed = self.last_attempt + user_cooldown

        if self.next_allowed_at_override:
            if now >= self.next_allowed_at_override:
                self.next_allowed_at_override = 0
            else:
                next_allowed = min(next_allowed, self.next_allowed_at_override)

        return max(0.0, next_allowed - now)

    def allow_retry_after(self, wait_seconds: float) -> None:
        """Cap the next retry wait to a shorter value when safe."""
        target_time = time.time() + max(0.0, wait_seconds)
        if not self.next_allowed_at_override or target_time < self.next_allowed_at_override:
            self.next_allowed_at_override = target_time
    
    def has_exceeded_hourly_limit(self) -> bool:
        """Check if user exceeded hourly attempt limit."""
        self.reset_hour_if_needed()
        return self.attempts_this_hour >= MAX_LOGIN_ATTEMPTS_PER_HOUR


class LoginRateLimiter:
    """
    Global login rate limiter for production Telegram bots.
    
    Features:
    - Limits concurrent logins (default: 2 at a time)
    - Enforces cooldown between logins (default: 60 sec)
    - Per-user cooldown (default: 5 min between attempts)
    - Hourly attempt limit per user (default: 3/hour)
    
    Thread-safe and async-safe.
    """
    
    def __init__(
        self,
        max_concurrent: int = MAX_CONCURRENT_LOGINS,
        global_cooldown: float = LOGIN_COOLDOWN_SECONDS,
        user_cooldown: float = USER_LOGIN_COOLDOWN,
        max_per_hour: int = MAX_LOGIN_ATTEMPTS_PER_HOUR,
    ):
        self._max_concurrent = max_concurrent
        self._global_cooldown = global_cooldown
        self._user_cooldown = user_cooldown
        self._max_per_hour = max_per_hour
        
        # Semaphore for concurrent limit
        self._semaphore = asyncio.Semaphore(max_concurrent)
        
        # Global last login time
        self._last_global_login: float = 0
        self._global_lock = asyncio.Lock()
        
        # Per-user state
        self._user_states: Dict[int, UserLoginState] = {}
        self._user_lock = asyncio.Lock()
        
        # Queue for waiting users
        self._queue: asyncio.Queue = asyncio.Queue()
        self._active_logins: int = 0
        
        logger.info(
            f"LoginRateLimiter initialized: max_concurrent={max_concurrent}, "
            f"cooldown={global_cooldown}s, user_cooldown={user_cooldown}s, "
            f"max_per_hour={max_per_hour}"
        )
    
    async def _get_user_state(self, user_id: int) -> UserLoginState:
        """Get or create user state."""
        async with self._user_lock:
            if user_id not in self._user_states:
                self._user_states[user_id] = UserLoginState()
            return self._user_states[user_id]
    
    async def can_login(self, user_id: int) -> Tuple[bool, float, str]:
        """
        Check if a user can attempt login now.
        
        Args:
            user_id: Telegram user ID
        
        Returns:
            Tuple of (can_login, wait_seconds, reason)
        """
        state = await self._get_user_state(user_id)
        
        # Check if user is already logging in
        if state.is_logging_in:
            return False, 0, "You already have a login in progress"
        
        # Check hourly limit
        if state.has_exceeded_hourly_limit():
            minutes_left = (3600 - (time.time() - state.hour_start)) / 60
            return False, minutes_left * 60, f"Login limit reached. Try again in {minutes_left:.0f} minutes"
        
        # Check per-user cooldown
        user_wait = state.time_until_can_retry(self._user_cooldown)
        if user_wait > 0:
            return False, user_wait, f"Please wait {user_wait:.0f} seconds before trying again"
        
        # Check global cooldown
        async with self._global_lock:
            time_since_global = time.time() - self._last_global_login
            if time_since_global < self._global_cooldown:
                wait = self._global_cooldown - time_since_global
                return False, wait, f"Server busy. Please wait {wait:.0f} seconds"
        
        # Check concurrent limit
        if self._active_logins >= self._max_concurrent:
            return False, 30, "Login queue full. Please wait..."
        
        return True, 0, ""

    def _is_light_login_load(self) -> bool:
        """True when the server is not saturated with login traffic."""
        return self._active_logins < self._max_concurrent

    async def allow_fast_relogin(
        self,
        user_id: int,
        wait_seconds: float = 60,
        *,
        only_if_light_load: bool = True,
    ) -> bool:
        """
        Shorten the user's next retry wait after a forced session reset.

        This keeps the normal 5-minute safety net for busy periods, but when
        the login system is relatively idle we cap the next retry to 1 minute.
        """
        if only_if_light_load and not self._is_light_login_load():
            logger.info(
                "Fast re-login skipped for user %d because login load is high (%d/%d)",
                user_id,
                self._active_logins,
                self._max_concurrent,
            )
            return False

        state = await self._get_user_state(user_id)
        current_wait = state.time_until_can_retry(self._user_cooldown)
        if current_wait <= wait_seconds:
            return True

        state.allow_retry_after(wait_seconds)
        logger.info(
            "Fast re-login enabled for user %d: capped next retry to %.0fs",
            user_id,
            wait_seconds,
        )
        return True
    
    @asynccontextmanager
    async def acquire(self, user_id: int, timeout: float = 300):
        """
        Acquire a login slot with rate limiting.
        
        Usage:
            async with login_limiter.acquire(user_id) as acquired:
                if acquired:
                    await do_login()
                else:
                    # Timed out waiting
                    pass
        
        Args:
            user_id: Telegram user ID
            timeout: Max seconds to wait for slot
        
        Yields:
            True if slot acquired, False if timed out
        """
        state = await self._get_user_state(user_id)
        acquired = False
        
        try:
            # Check if user can login
            can_login, wait_time, reason = await self.can_login(user_id)
            
            if not can_login:
                if wait_time > 0 and wait_time <= timeout:
                    logger.info(f"User {user_id} waiting {wait_time:.0f}s for login slot")
                    await asyncio.sleep(wait_time)
                else:
                    logger.warning(f"User {user_id} login denied: {reason}")
                    yield False
                    return
            
            # Try to acquire semaphore
            try:
                await asyncio.wait_for(
                    self._semaphore.acquire(),
                    timeout=timeout
                )
                acquired = True
            except asyncio.TimeoutError:
                logger.warning(f"User {user_id} timed out waiting for login slot")
                yield False
                return
            
            # Mark user as logging in
            state.is_logging_in = True
            self._active_logins += 1
            
            # Update global last login time
            async with self._global_lock:
                self._last_global_login = time.time()
            
            # Record attempt
            state.record_attempt()
            
            logger.info(
                f"User {user_id} acquired login slot "
                f"(active: {self._active_logins}/{self._max_concurrent}, "
                f"attempts: {state.attempts_this_hour}/{self._max_per_hour})"
            )
            
            yield True
            
        finally:
            if acquired:
                self._semaphore.release()
                self._active_logins -= 1
                state.is_logging_in = False
                logger.debug(f"User {user_id} released login slot")
    
    async def wait_for_slot(self, user_id: int, timeout: float = 300) -> Tuple[bool, str]:
        """
        Wait for a login slot to become available.
        
        Args:
            user_id: Telegram user ID
            timeout: Max seconds to wait
        
        Returns:
            Tuple of (success, message)
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            can_login, wait_time, reason = await self.can_login(user_id)
            
            if can_login:
                return True, "Ready to login"
            
            if wait_time > timeout - (time.time() - start_time):
                return False, reason
            
            # Wait and retry
            await asyncio.sleep(min(wait_time, 10))
        
        return False, "Timeout waiting for login slot"
    
    def get_status(self) -> Dict:
        """Get current rate limiter status."""
        return {
            "active_logins": self._active_logins,
            "max_concurrent": self._max_concurrent,
            "global_cooldown": self._global_cooldown,
            "user_cooldown": self._user_cooldown,
            "max_per_hour": self._max_per_hour,
            "tracked_users": len(self._user_states),
        }
    
    async def reset_user(self, user_id: int) -> None:
        """Reset rate limit state for a user (admin use)."""
        async with self._user_lock:
            if user_id in self._user_states:
                del self._user_states[user_id]
                logger.info(f"Reset rate limit for user {user_id}")
    
    async def cleanup_old_states(self, max_age: float = 7200) -> int:
        """Remove user states older than max_age seconds."""
        now = time.time()
        removed = 0
        
        async with self._user_lock:
            to_remove = [
                uid for uid, state in self._user_states.items()
                if now - state.last_attempt > max_age and not state.is_logging_in
            ]
            
            for uid in to_remove:
                del self._user_states[uid]
                removed += 1
        
        if removed:
            logger.debug(f"Cleaned up {removed} old login states")
        
        return removed


# Global instance
_login_limiter: Optional[LoginRateLimiter] = None


def get_login_limiter() -> LoginRateLimiter:
    """Get the global LoginRateLimiter instance."""
    global _login_limiter
    if _login_limiter is None:
        _login_limiter = LoginRateLimiter()
    return _login_limiter


# Convenience alias
login_limiter = get_login_limiter()


# ============================================================================
# DECORATOR FOR HANDLERS
# ============================================================================

def rate_limited_login(func):
    """
    Decorator for login handlers that enforces rate limiting.
    
    Usage:
        @Client.on_message(filters.command("login"))
        @rate_limited_login
        async def login_handler(client, message):
            # This code only runs if rate limit allows
            await do_login(...)
    """
    async def wrapper(client, message, *args, **kwargs):
        user_id = message.from_user.id
        limiter = get_login_limiter()
        
        can_login, wait_time, reason = await limiter.can_login(user_id)
        
        if not can_login:
            if wait_time > 60:
                wait_str = f"{wait_time / 60:.0f} minutes"
            else:
                wait_str = f"{wait_time:.0f} seconds"
            
            await message.reply(
                f"⏳ **Login Rate Limited**\n\n"
                f"{reason}\n\n"
                f"Please wait **{wait_str}** before trying again."
            )
            return
        
        return await func(client, message, *args, **kwargs)
    
    return wrapper
