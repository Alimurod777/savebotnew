"""
Task-Scoped Session Handler

Design Principles:
1. Sessions are EPHEMERAL - created per-task, destroyed on completion
2. No cross-event-loop sharing - each task owns its session
3. No persistent session sync - validation happens at task start
4. Fail-fast on invalid sessions - no infinite retries
5. Guaranteed cleanup via context managers

This replaces the MongoDB-based session_manager for RUNTIME operations.
Database still stores session_strings for persistence across bot restarts,
but runtime usage is strictly task-scoped.
"""

import asyncio
import uuid
import logging
from contextlib import asynccontextmanager
from typing import Optional, Tuple
from dataclasses import dataclass

from pyrogram import Client, raw
from pyrogram.errors import (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    AuthKeyDuplicated,
    SessionExpired,
    SessionRevoked,
    SessionPasswordNeeded,
    UserDeactivated,
    UserDeactivatedBan,
    FloodWait,
    RPCError,
)

from config import API_ID, API_HASH, get_client_params

logger = logging.getLogger(__name__)

# Session configuration
DEFAULT_START_TIMEOUT = 30.0
DEFAULT_STOP_TIMEOUT = 5.0
MAX_CONNECTION_RETRIES = 2
RETRY_DELAY = 1.0


# Fatal errors that indicate session is permanently invalid
# These require user to re-login - no recovery possible
FATAL_SESSION_ERRORS = (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    AuthKeyDuplicated,
    SessionExpired,
    SessionRevoked,
    UserDeactivated,
    UserDeactivatedBan,
)

# Error messages for different session failure types
SESSION_ERROR_MESSAGES = {
    'AuthKeyUnregistered': 'Session expired. Please /login again.',
    'AuthKeyInvalid': 'Session invalid. Please /login again.',
    'AuthKeyDuplicated': 'Session conflict detected. Please /logout and /login again.',
    'SessionExpired': 'Session expired. Please /login again.',
    'SessionRevoked': 'Session was revoked (possibly from another device). Please /login again.',
    'UserDeactivated': 'Account deactivated. Please contact Telegram support.',
    'UserDeactivatedBan': 'Account banned. Please contact Telegram support.',
}


class SessionError(Exception):
    """Base exception for session errors."""
    pass


class SessionInvalidError(SessionError):
    """Session is invalid/expired - fail fast, do not retry."""
    def __init__(self, message: str, error_type: str = "unknown"):
        super().__init__(message)
        self.error_type = error_type
        self.user_message = SESSION_ERROR_MESSAGES.get(error_type, message)


class SessionConnectionError(SessionError):
    """Temporary connection error - may retry."""
    pass


@dataclass
class SessionInfo:
    """Information about a started session."""
    user_telegram_id: int
    first_name: str
    username: Optional[str]
    is_premium: bool = False


@asynccontextmanager
async def create_user_session(
    session_string: str,
    user_id: int,
    timeout: float = DEFAULT_START_TIMEOUT,
    validate: bool = True,
    peers_to_resolve: list = None,
):
    """
    Create a task-scoped Pyrogram client session.
    
    GUARANTEES:
    - Client is started within this context
    - Client is stopped on exit (success, error, or cancel)
    - No client reference escapes this context
    - Fail-fast on invalid sessions
    
    Args:
        session_string: User's session string
        user_id: User ID (for naming)
        timeout: Start timeout in seconds
        validate: Whether to call get_me() to validate session
    
    Yields:
        Connected Pyrogram Client
        
    Raises:
        SessionInvalidError: Session is permanently invalid
        SessionConnectionError: Temporary connection failure
    
    Usage:
        async with create_user_session(session_str, user_id) as client:
            # Use client for operations
            messages = await client.get_messages(...)
        # Client is guaranteed stopped here
    """
    client = None
    client_name = f"user_{user_id}_{uuid.uuid4().hex[:8]}"
    
    try:
        # Get validated platform-consistent fingerprint
        fp = get_client_params(user_id)
        
        client = Client(
            client_name,
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True,
            no_updates=True,
            sleep_threshold=30,
            max_concurrent_transmissions=20,
            device_model=fp['device_model'],
            system_version=fp['system_version'],
            app_version=fp['app_version'],
            lang_code=fp['lang_code'],
        )
        
        # Attempt connection with retries for transient failures
        last_error = None
        for attempt in range(MAX_CONNECTION_RETRIES + 1):
            try:
                await asyncio.wait_for(client.start(), timeout=timeout)
                break
            except FATAL_SESSION_ERRORS as e:
                raise SessionInvalidError(
                    f"Session invalid: {type(e).__name__}",
                    error_type=type(e).__name__
                )
            except asyncio.TimeoutError:
                last_error = "Connection timeout"
                if attempt < MAX_CONNECTION_RETRIES:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                raise SessionConnectionError(f"Start timeout after {MAX_CONNECTION_RETRIES + 1} attempts")
            except FloodWait as e:
                wait = min(e.value, 30)
                if attempt < MAX_CONNECTION_RETRIES:
                    await asyncio.sleep(wait)
                    continue
                raise SessionConnectionError(f"Rate limited, wait {e.value}s")
            except Exception as e:
                last_error = str(e)
                if attempt < MAX_CONNECTION_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
                    continue
                raise SessionConnectionError(f"Connection failed: {last_error}")
        
        # Validate session if requested
        if validate:
            try:
                await asyncio.wait_for(client.get_me(), timeout=10.0)
            except FATAL_SESSION_ERRORS as e:
                raise SessionInvalidError(
                    f"Session validation failed: {type(e).__name__}",
                    error_type=type(e).__name__
                )
            except asyncio.TimeoutError:
                raise SessionConnectionError("Validation timeout")
            except Exception as e:
                raise SessionConnectionError(f"Validation failed: {e}")

        # Pre-resolve peers so their access_hash is cached in this in-memory session.
        # Without this, private channel IDs raise PEER_ID_INVALID on first use.
        # Strategy:
        #   - int peer → channels.GetChannels(access_hash=0): server resolves from membership
        #   - str peer → contacts.ResolveUsername: always works for public/accessible peers
        if peers_to_resolve:
            for peer in peers_to_resolve:
                try:
                    if isinstance(peer, int) and peer < 0:
                        # Private channel or supergroup — use raw GetChannels with access_hash=0
                        # Telegram fills access_hash from server-side membership record
                        pure_id = abs(peer)
                        if str(pure_id).startswith("100"):
                            pure_id = int(str(pure_id)[3:])
                        await client.invoke(
                            raw.functions.channels.GetChannels(
                                id=[raw.types.InputChannel(
                                    channel_id=pure_id,
                                    access_hash=0,
                                )]
                            )
                        )
                    else:
                        # Username string or positive user ID
                        await client.get_chat(peer)
                    logger.debug("Session peer resolved: %s", peer)
                except Exception as e:
                    logger.warning("Session peer resolve failed (%s): %s", peer, e)

        yield client
        
    except FATAL_SESSION_ERRORS as e:
        raise SessionInvalidError(
            f"Session error: {type(e).__name__}",
            error_type=type(e).__name__
        )
    finally:
        if client:
            try:
                if client.is_connected:
                    await asyncio.wait_for(client.stop(), timeout=DEFAULT_STOP_TIMEOUT)
            except asyncio.TimeoutError:
                logger.debug(f"Stop timeout for {client_name}")
            except Exception as e:
                logger.debug(f"Error stopping client: {e}")


async def validate_session(
    session_string: str,
    user_id: int,
    timeout: float = 15.0
) -> Tuple[bool, Optional[str], Optional[SessionInfo]]:
    """
    Validate a session string without keeping connection open.
    
    Args:
        session_string: Session to validate
        user_id: User ID for naming
        timeout: Timeout for validation
    
    Returns:
        (is_valid, error_message, session_info)
    """
    if not session_string:
        return False, "No session string provided", None
    
    try:
        async with create_user_session(session_string, user_id, timeout=timeout, validate=True) as client:
            me = await client.get_me()
            info = SessionInfo(
                user_telegram_id=me.id,
                first_name=me.first_name or "",
                username=me.username,
                is_premium=getattr(me, 'is_premium', False)
            )
            return True, None, info
            
    except SessionInvalidError as e:
        return False, str(e), None
    except SessionConnectionError as e:
        return False, f"Connection error: {e}", None
    except Exception as e:
        return False, f"Validation error: {e}", None


async def quick_session_check(session_string: str, user_id: int) -> bool:
    """
    Quick check if session is likely valid.
    Faster than full validation - just attempts connection.
    """
    if not session_string:
        return False
    
    try:
        async with create_user_session(session_string, user_id, timeout=10.0, validate=False) as client:
            return client.is_connected
    except (SessionInvalidError, SessionConnectionError):
        return False
    except Exception:
        return False


class SessionContext:
    """
    Helper class for managing session lifecycle within a task.
    
    Usage:
        ctx = SessionContext(session_string, user_id)
        async with ctx.connect() as client:
            # use client
        # client stopped, ctx.error contains any error
    """
    
    def __init__(self, session_string: str, user_id: int):
        self.session_string = session_string
        self.user_id = user_id
        self.error: Optional[str] = None
        self.is_invalid: bool = False
        self._client: Optional[Client] = None
    
    @asynccontextmanager
    async def connect(self, timeout: float = DEFAULT_START_TIMEOUT):
        """Connect and yield client, tracking any errors."""
        try:
            async with create_user_session(
                self.session_string, 
                self.user_id, 
                timeout=timeout
            ) as client:
                self._client = client
                yield client
        except SessionInvalidError as e:
            self.error = str(e)
            self.is_invalid = True
            raise
        except SessionConnectionError as e:
            self.error = str(e)
            raise
        except Exception as e:
            self.error = str(e)
            raise
        finally:
            self._client = None
    
    async def validate(self) -> Tuple[bool, Optional[str]]:
        """Validate session and return (is_valid, error)."""
        valid, error, _ = await validate_session(self.session_string, self.user_id)
        if not valid:
            self.error = error
            self.is_invalid = "invalid" in (error or "").lower()
        return valid, error
