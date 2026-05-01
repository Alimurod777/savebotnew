"""
core/retry_utils.py - Production-grade retry utilities for Telegram operations.

DESIGN PRINCIPLES:
1. All retries have CAPS (no infinite loops)
2. FloodWait is handled with capped sleep
3. Network errors use exponential backoff with jitter
4. Transient errors are distinguished from fatal errors
5. All failures are logged with context

Usage:
    from core.retry_utils import with_retry, handle_floodwait
    
    # Decorator for automatic retry
    @with_retry(max_attempts=3, backoff_base=2.0)
    async def my_telegram_operation():
        ...
    
    # Manual FloodWait handling
    try:
        await client.send_message(...)
    except FloodWait as e:
        await handle_floodwait(e, max_wait=60)
"""

import asyncio
import random
import logging
from typing import TypeVar, Callable, Optional, Set, Type, Any
from functools import wraps

from pyrogram.errors import (
    FloodWait, 
    Timeout,
    NetworkMigrate,
    InternalServerError,
    ServiceUnavailable,
    BadRequest,
    Unauthorized,
    Forbidden,
    RPCError
)

logger = logging.getLogger(__name__)

T = TypeVar('T')

# Maximum wait times (caps to prevent infinite waits)
MAX_FLOODWAIT_SECONDS = 300  # 5 minutes max for FloodWait
MAX_BACKOFF_SECONDS = 60     # 1 minute max backoff between retries
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_BASE = 2.0

# Transient errors that warrant retry
# CRITICAL: Session must NOT terminate on any of these
TRANSIENT_ERRORS: Set[Type[Exception]] = {
    Timeout,
    NetworkMigrate,
    InternalServerError,
    ServiceUnavailable,
    ConnectionError,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
    TimeoutError,
    OSError,
}

# Fatal errors that should NOT be retried
FATAL_ERRORS: Set[Type[Exception]] = {
    Unauthorized,
    Forbidden,
}


def is_transient_error(error: Exception) -> bool:
    """Check if error is transient and retryable."""
    for transient_type in TRANSIENT_ERRORS:
        if isinstance(error, transient_type):
            return True
    
    # Check for network-related string patterns in RPCError
    if isinstance(error, RPCError):
        error_id = getattr(error, 'ID', '') or str(error)
        transient_patterns = ['TIMEOUT', 'NETWORK', 'SERVICE_UNAVAILABLE', 'INTERNAL']
        return any(p in error_id.upper() for p in transient_patterns)
    
    return False


def is_fatal_error(error: Exception) -> bool:
    """Check if error is fatal and should not be retried."""
    for fatal_type in FATAL_ERRORS:
        if isinstance(error, fatal_type):
            return True
    
    if isinstance(error, RPCError):
        error_id = getattr(error, 'ID', '') or str(error)
        fatal_patterns = ['AUTH_KEY', 'SESSION', 'BANNED', 'DEACTIVATED', 'INVALID']
        return any(p in error_id.upper() for p in fatal_patterns)
    
    return False


def get_floodwait_seconds(error: FloodWait) -> int:
    """Extract wait time from FloodWait error (handles Pyrofork variants)."""
    # Pyrofork uses .value, older versions use .x
    return getattr(error, 'value', getattr(error, 'x', 30))


async def handle_floodwait(
    error: FloodWait,
    max_wait: int = MAX_FLOODWAIT_SECONDS,
    context: str = ""
) -> bool:
    """
    Handle FloodWait error with capped sleep.
    
    Args:
        error: FloodWait exception
        max_wait: Maximum seconds to wait (default: 300)
        context: Optional context string for logging
    
    Returns:
        True if wait was successful, False if exceeded max_wait
    """
    wait_time = get_floodwait_seconds(error)
    
    if wait_time > max_wait:
        logger.warning(
            f"FloodWait {wait_time}s exceeds max {max_wait}s{f' ({context})' if context else ''}, not waiting"
        )
        return False
    
    logger.info(f"FloodWait: sleeping {wait_time}s{f' ({context})' if context else ''}")
    await asyncio.sleep(wait_time)
    return True


def calculate_backoff(
    attempt: int,
    base: float = DEFAULT_BACKOFF_BASE,
    max_backoff: float = MAX_BACKOFF_SECONDS,
    jitter: bool = True
) -> float:
    """
    Calculate exponential backoff with optional jitter.
    
    Args:
        attempt: Current attempt number (0-based)
        base: Base multiplier for exponential backoff
        max_backoff: Maximum backoff in seconds
        jitter: Add random jitter to prevent thundering herd
    
    Returns:
        Backoff time in seconds
    """
    # Exponential: base^attempt (e.g., 2^0=1, 2^1=2, 2^2=4, 2^3=8)
    backoff = min(base ** attempt, max_backoff)
    
    if jitter:
        # Add ±25% jitter
        jitter_range = backoff * 0.25
        backoff += random.uniform(-jitter_range, jitter_range)
    
    return max(0.1, backoff)  # Minimum 100ms


def with_retry(
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    max_backoff: float = MAX_BACKOFF_SECONDS,
    max_floodwait: int = MAX_FLOODWAIT_SECONDS,
    reraise_fatal: bool = True,
    on_retry: Optional[Callable[[Exception, int], None]] = None
):
    """
    Decorator for automatic retry with FloodWait and backoff handling.
    
    CRITICAL: Has CAPS on all wait times to prevent infinite loops.
    
    Args:
        max_attempts: Maximum number of attempts (default: 5)
        backoff_base: Base for exponential backoff (default: 2.0)
        max_backoff: Maximum backoff between retries (default: 60s)
        max_floodwait: Maximum FloodWait to honor (default: 300s)
        reraise_fatal: Raise fatal errors immediately without retry
        on_retry: Optional callback(error, attempt) called before each retry
    
    Usage:
        @with_retry(max_attempts=3)
        async def fetch_message(client, chat_id, msg_id):
            return await client.get_messages(chat_id, msg_id)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            last_error = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                
                except FloodWait as e:
                    wait_time = get_floodwait_seconds(e)
                    
                    if wait_time > max_floodwait:
                        logger.error(
                            f"{func.__name__} FloodWait {wait_time}s exceeds max {max_floodwait}s, giving up"
                        )
                        raise
                    
                    logger.warning(f"{func.__name__} FloodWait: {wait_time}s (attempt {attempt + 1}/{max_attempts})")
                    await asyncio.sleep(wait_time)
                    last_error = e
                    # Don't count FloodWait against attempt limit
                    continue
                
                except Exception as e:
                    last_error = e
                    
                    # Fatal errors - don't retry
                    if reraise_fatal and is_fatal_error(e):
                        logger.error(f"{func.__name__} fatal error: {e}")
                        raise
                    
                    # Transient errors - retry with backoff
                    if is_transient_error(e) and attempt < max_attempts - 1:
                        backoff = calculate_backoff(attempt, backoff_base, max_backoff)
                        logger.warning(
                            f"{func.__name__} transient error: {e}, "
                            f"retry {attempt + 1}/{max_attempts} in {backoff:.1f}s"
                        )
                        
                        if on_retry:
                            on_retry(e, attempt)
                        
                        await asyncio.sleep(backoff)
                        continue
                    
                    # Non-transient error on last attempt
                    if attempt >= max_attempts - 1:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise
                    
                    # Non-transient but still has attempts - try anyway with backoff
                    backoff = calculate_backoff(attempt, backoff_base, max_backoff)
                    logger.warning(
                        f"{func.__name__} error: {e}, retry {attempt + 1}/{max_attempts} in {backoff:.1f}s"
                    )
                    
                    if on_retry:
                        on_retry(e, attempt)
                    
                    await asyncio.sleep(backoff)
            
            # Should not reach here, but handle edge case
            if last_error:
                raise last_error
            raise RuntimeError(f"{func.__name__} failed with no error captured")
        
        return wrapper
    return decorator


class RetryContext:
    """
    Context manager for manual retry control.
    
    Usage:
        async with RetryContext(max_attempts=3) as ctx:
            for attempt in ctx:
                try:
                    result = await some_operation()
                    break  # Success
                except FloodWait as e:
                    await ctx.handle_floodwait(e)
                except Exception as e:
                    if not ctx.should_retry(e):
                        raise
                    await ctx.backoff()
    """
    
    def __init__(
        self,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        max_backoff: float = MAX_BACKOFF_SECONDS,
        max_floodwait: int = MAX_FLOODWAIT_SECONDS
    ):
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.max_backoff = max_backoff
        self.max_floodwait = max_floodwait
        self.attempt = 0
        self.last_error = None
    
    def __iter__(self):
        self.attempt = 0
        return self
    
    def __next__(self) -> int:
        if self.attempt >= self.max_attempts:
            raise StopIteration
        attempt = self.attempt
        self.attempt += 1
        return attempt
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    async def handle_floodwait(self, error: FloodWait) -> bool:
        """Handle FloodWait, returns True if handled."""
        self.last_error = error
        wait_time = get_floodwait_seconds(error)
        
        if wait_time > self.max_floodwait:
            return False
        
        await asyncio.sleep(wait_time)
        # Reset attempt counter for FloodWait (don't penalize)
        self.attempt = max(0, self.attempt - 1)
        return True
    
    def should_retry(self, error: Exception) -> bool:
        """Check if error warrants retry."""
        self.last_error = error
        
        if is_fatal_error(error):
            return False
        
        return self.attempt < self.max_attempts
    
    async def backoff(self) -> None:
        """Sleep with exponential backoff."""
        wait = calculate_backoff(self.attempt - 1, self.backoff_base, self.max_backoff)
        await asyncio.sleep(wait)


# Convenience function for one-off retries
async def retry_operation(
    operation: Callable[..., T],
    *args,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    context: str = "",
    **kwargs
) -> T:
    """
    Retry an async operation with default settings.
    
    Usage:
        result = await retry_operation(
            client.get_messages,
            chat_id, msg_id,
            context="fetch_message"
        )
    """
    @with_retry(max_attempts=max_attempts)
    async def wrapped():
        return await operation(*args, **kwargs)
    
    try:
        return await wrapped()
    except Exception as e:
        if context:
            logger.error(f"{context} failed: {e}")
        raise
