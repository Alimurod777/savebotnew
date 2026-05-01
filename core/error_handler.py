"""
Comprehensive Error Handling for Telegram Operations

Categories:
1. Recoverable (retry with backoff)
2. Session-fatal (invalidate session)
3. Rate limits (FloodWait)
4. Network errors (reconnect)
5. File errors (skip/report)
"""

import asyncio
from functools import wraps
from typing import Optional, Callable, TypeVar
from pyrogram.errors import (
    FloodWait,
    AuthKeyDuplicated,
    AuthKeyUnregistered,
    SessionRevoked,
    UserDeactivated,
    ChannelPrivate,
    ChatAdminRequired,
    UsernameInvalid,
    PeerIdInvalid,
    MessageIdInvalid,
    FileReferenceExpired,
    FileReferenceInvalid,
    RPCError,
)
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')


class SessionInvalidError(Exception):
    """Session needs re-login."""
    pass


class ItemSkipError(Exception):
    """This item should be skipped."""
    pass


class FileReferenceError(Exception):
    """File reference needs refresh."""
    pass


class ErrorCategory:
    RECOVERABLE = {
        "TIMEOUT",
        "CONNECTION_ERROR",
        "NETWORK_ERROR",
        "Peer flood",
    }
    
    SESSION_FATAL = {
        AuthKeyDuplicated,
        AuthKeyUnregistered,
        SessionRevoked,
        UserDeactivated,
    }
    
    ITEM_SKIP = {
        MessageIdInvalid,
        ChannelPrivate,
        ChatAdminRequired,
        PeerIdInvalid,
        UsernameInvalid,
    }
    
    FILE_REFERENCE = {
        FileReferenceExpired,
        FileReferenceInvalid,
    }


def with_retry(
    max_retries: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 60.0,
    on_retry: Optional[Callable] = None
):
    """Decorator for retrying async operations with exponential backoff."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                    
                except FloodWait as e:
                    wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                    logger.warning(f"FloodWait: {wait_time}s (attempt {attempt + 1})")
                    
                    if attempt < max_retries:
                        await asyncio.sleep(min(wait_time, max_delay))
                        if on_retry:
                            on_retry(attempt, e)
                        continue
                    raise
                    
                except tuple(ErrorCategory.SESSION_FATAL) as e:
                    logger.error(f"Session fatal error: {e}")
                    raise SessionInvalidError(str(e))
                    
                except tuple(ErrorCategory.ITEM_SKIP) as e:
                    logger.warning(f"Skippable error: {e}")
                    raise ItemSkipError(str(e))
                    
                except tuple(ErrorCategory.FILE_REFERENCE) as e:
                    logger.warning(f"File reference error: {e}")
                    raise FileReferenceError(str(e))
                    
                except asyncio.TimeoutError as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(f"Timeout, retrying in {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise
                    
                except (OSError, ConnectionError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(f"Network error: {e}, retrying in {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    raise
                    
                except RPCError as e:
                    error_msg = str(e).upper()
                    
                    if any(err in error_msg for err in ErrorCategory.RECOVERABLE):
                        last_exception = e
                        if attempt < max_retries:
                            delay = min(base_delay * (2 ** attempt), max_delay)
                            await asyncio.sleep(delay)
                            continue
                    raise
                    
                except Exception as e:
                    last_exception = e
                    raise
            
            if last_exception:
                raise last_exception
            
        return wrapper
    return decorator


def flood_protected(min_delay: float = 0.5):
    """Decorator to add minimum delay between calls."""
    _last_call = {}
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            key = func.__name__
            
            now = asyncio.get_running_loop().time()
            last = _last_call.get(key, 0)
            
            if now - last < min_delay:
                await asyncio.sleep(min_delay - (now - last))
            
            try:
                return await func(*args, **kwargs)
            finally:
                _last_call[key] = asyncio.get_running_loop().time()
        
        return wrapper
    return decorator
