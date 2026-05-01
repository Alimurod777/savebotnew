"""
Production-Grade Session Handler for Pyrogram

Task-scoped session management with guaranteed cleanup.

Author: WOODcraft Production-Grade Refactor
Python: 3.10+
Platform: Windows-optimized

DESIGN PRINCIPLES:
1. Sessions are EPHEMERAL - created per-task, destroyed on completion
2. SINGLE event loop - no cross-loop sharing
3. Context managers for guaranteed cleanup
4. Fail-fast on invalid sessions - no infinite retries
5. No threading, no executors, no blocking calls

USAGE:
    async with create_session(session_string, user_id) as client:
        messages = await client.get_messages(chat_id, limit=10)
    # Client is GUARANTEED to be stopped here
"""

import asyncio
import uuid
import logging
from contextlib import asynccontextmanager
from typing import Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum, auto

from pyrogram import Client
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
    Unauthorized,
    RPCError,
)

from config import API_ID, API_HASH, get_client_params

# Try to import resource calculator for optimal settings
try:
    from TechVJ.resource_calculator_v2 import calculate_optimal_config
    RESOURCE_CALC_AVAILABLE = True
except ImportError:
    RESOURCE_CALC_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# Timeouts
START_TIMEOUT = 30.0
STOP_TIMEOUT = 5.0
VALIDATION_TIMEOUT = 10.0

# Retry configuration
MAX_RETRIES = 2
RETRY_DELAY = 1.5


# ============================================================================
# FATAL ERRORS - Session is permanently invalid
# ============================================================================

FATAL_ERRORS = (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    AuthKeyDuplicated,
    SessionExpired,
    SessionRevoked,
    UserDeactivated,
    UserDeactivatedBan,
    Unauthorized,
)


# ============================================================================
# RESULT TYPES
# ============================================================================

class SessionStatus(Enum):
    """Status of a session."""
    VALID = auto()
    INVALID = auto()
    CONNECTION_ERROR = auto()
    TIMEOUT = auto()
    UNKNOWN_ERROR = auto()


@dataclass
class SessionInfo:
    """Information about a validated session."""
    user_id: int
    first_name: str
    last_name: Optional[str]
    username: Optional[str]
    is_premium: bool = False
    phone: Optional[str] = None


@dataclass
class SessionResult:
    """Result of a session operation."""
    status: SessionStatus
    info: Optional[SessionInfo] = None
    error: Optional[str] = None


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class SessionError(Exception):
    """Base exception for session errors."""
    pass


class SessionInvalidError(SessionError):
    """
    Session is permanently invalid.
    
    DO NOT retry - user needs to re-login.
    """
    def __init__(self, message: str, error_type: str = "unknown"):
        super().__init__(message)
        self.error_type = error_type


class SessionConnectionError(SessionError):
    """
    Temporary connection error.
    
    MAY retry with backoff.
    """
    pass


class SessionTimeoutError(SessionError):
    """
    Session operation timed out.
    """
    pass


# ============================================================================
# SESSION FACTORY
# ============================================================================

def create_pyrogram_client(
    session_string: str,
    user_id: int,
    for_user_session: bool = True
) -> Client:
    """
    Create a Pyrogram Client instance with optimal settings.
    
    SECURITY: Uses validated platform-consistent fingerprint from config.py
    to prevent Telegram security resets caused by platform parameter mixing.
    
    Args:
        session_string: User's session string
        user_id: User ID (for unique naming)
        for_user_session: True for user session, False for bot
        
    Returns:
        Configured Client instance (NOT started)
    """
    # Generate unique name to prevent conflicts
    client_name = f"session_{user_id}_{uuid.uuid4().hex[:8]}"
    
    # Get optimal settings if available
    if RESOURCE_CALC_AVAILABLE and for_user_session:
        try:
            config = calculate_optimal_config()
            max_concurrent = min(config.max_concurrent_transmissions, 10)
        except Exception:
            max_concurrent = 8
    else:
        max_concurrent = 8
    
    # SECURITY: Get validated platform-consistent fingerprint
    # The same user_id always returns the same fingerprint (immutability)
    fp = get_client_params(user_id)
    
    return Client(
        name=client_name,
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        in_memory=True,
        no_updates=True,  # Don't receive updates
        sleep_threshold=30,  # Auto-handle FloodWait up to 30s
        max_concurrent_transmissions=max_concurrent,
        # SECURITY: Platform-consistent fingerprint (prevents security resets)
        device_model=fp['device_model'],
        system_version=fp['system_version'],
        app_version=fp['app_version'],
        lang_code=fp['lang_code'],
    )


# ============================================================================
# MAIN SESSION CONTEXT MANAGER
# ============================================================================

@asynccontextmanager
async def create_session(
    session_string: str,
    user_id: int,
    timeout: float = START_TIMEOUT,
    validate: bool = True
):
    """
    Create a task-scoped Pyrogram session with guaranteed cleanup.
    
    This is the MAIN entry point for using user sessions.
    
    GUARANTEES:
    - Client is started within this context
    - Client is stopped on exit (success, error, or cancel)
    - No client reference escapes this context
    - Fail-fast on permanently invalid sessions
    
    Args:
        session_string: User's session string
        user_id: User ID for naming and logging
        timeout: Maximum time to wait for connection
        validate: Whether to call get_me() to validate
        
    Yields:
        Connected Pyrogram Client
        
    Raises:
        SessionInvalidError: Session is permanently invalid
        SessionConnectionError: Temporary connection failure
        SessionTimeoutError: Connection timed out
        
    Usage:
        try:
            async with create_session(session_str, user_id) as client:
                messages = await client.get_messages(...)
        except SessionInvalidError:
            # User needs to re-login
            pass
        except SessionConnectionError:
            # Temporary error, can retry
            pass
    """
    if not session_string:
        raise SessionInvalidError("No session string provided", "missing")
    
    client: Optional[Client] = None
    
    try:
        client = create_pyrogram_client(session_string, user_id)
        
        # Try to connect with retries for transient errors
        last_error: Optional[str] = None
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                # Start with timeout
                await asyncio.wait_for(client.start(), timeout=timeout)
                break  # Success!
                
            except FATAL_ERRORS as e:
                # Session is permanently invalid
                raise SessionInvalidError(
                    f"Session invalid: {type(e).__name__}",
                    error_type=type(e).__name__
                )
            
            except asyncio.TimeoutError:
                last_error = f"Connection timeout after {timeout}s"
                if attempt < MAX_RETRIES:
                    logger.debug(f"Session start timeout, attempt {attempt + 1}")
                    await asyncio.sleep(RETRY_DELAY)
                    continue
                raise SessionTimeoutError(last_error)
            
            except FloodWait as e:
                wait_time = min(e.value, 30)
                last_error = f"Rate limited for {e.value}s"
                if attempt < MAX_RETRIES:
                    logger.warning(f"FloodWait({wait_time}s) during session start")
                    await asyncio.sleep(wait_time)
                    continue
                raise SessionConnectionError(last_error)
            
            except asyncio.CancelledError:
                raise
            
            except Exception as e:
                error_str = str(e).lower()
                last_error = str(e)
                
                # Check if it's a network-related error
                if any(x in error_str for x in ["connection", "network", "timeout", "reset"]):
                    if attempt < MAX_RETRIES:
                        logger.debug(f"Connection error, retry {attempt + 1}: {e}")
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    raise SessionConnectionError(f"Connection failed: {last_error}")
                
                # Unknown error - try once more then give up
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
                    continue
                raise SessionConnectionError(f"Start failed: {last_error}")
        
        # Validate session if requested
        if validate:
            try:
                me = await asyncio.wait_for(
                    client.get_me(), 
                    timeout=VALIDATION_TIMEOUT
                )
                if not me:
                    raise SessionInvalidError("get_me() returned None", "empty_response")
                logger.debug(f"Session validated for user {me.id}")
                
            except FATAL_ERRORS as e:
                raise SessionInvalidError(
                    f"Validation failed: {type(e).__name__}",
                    error_type=type(e).__name__
                )
            except asyncio.TimeoutError:
                raise SessionTimeoutError("Session validation timed out")
            except SessionInvalidError:
                raise
            except Exception as e:
                raise SessionConnectionError(f"Validation error: {e}")
        
        yield client
        
    except FATAL_ERRORS as e:
        raise SessionInvalidError(
            f"Session error: {type(e).__name__}",
            error_type=type(e).__name__
        )
    
    finally:
        # GUARANTEED cleanup
        if client is not None:
            try:
                if client.is_connected:
                    await asyncio.wait_for(
                        client.stop(), 
                        timeout=STOP_TIMEOUT
                    )
                    logger.debug(f"Session stopped for user {user_id}")
            except asyncio.TimeoutError:
                logger.debug(f"Session stop timeout for user {user_id}")
            except Exception as e:
                logger.debug(f"Session stop error: {e}")


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

async def validate_session(
    session_string: str,
    user_id: int,
    timeout: float = START_TIMEOUT
) -> SessionResult:
    """
    Validate a session string and get user info.
    
    Opens a connection, validates, then closes.
    
    Args:
        session_string: Session to validate
        user_id: User ID for naming
        timeout: Maximum time for validation
        
    Returns:
        SessionResult with status and optional user info
    """
    if not session_string:
        return SessionResult(
            status=SessionStatus.INVALID,
            error="No session string provided"
        )
    
    try:
        async with create_session(
            session_string, 
            user_id, 
            timeout=timeout, 
            validate=True
        ) as client:
            me = await client.get_me()
            
            info = SessionInfo(
                user_id=me.id,
                first_name=me.first_name or "",
                last_name=me.last_name,
                username=me.username,
                is_premium=getattr(me, 'is_premium', False),
                phone=me.phone_number
            )
            
            return SessionResult(
                status=SessionStatus.VALID,
                info=info
            )
    
    except SessionInvalidError as e:
        return SessionResult(
            status=SessionStatus.INVALID,
            error=str(e)
        )
    
    except SessionTimeoutError as e:
        return SessionResult(
            status=SessionStatus.TIMEOUT,
            error=str(e)
        )
    
    except SessionConnectionError as e:
        return SessionResult(
            status=SessionStatus.CONNECTION_ERROR,
            error=str(e)
        )
    
    except Exception as e:
        return SessionResult(
            status=SessionStatus.UNKNOWN_ERROR,
            error=str(e)
        )


async def quick_check(
    session_string: str,
    user_id: int,
    timeout: float = 10.0
) -> bool:
    """
    Quick check if session is likely valid.
    
    Faster than full validation - just attempts connection.
    
    Returns:
        True if session appears valid
    """
    if not session_string:
        return False
    
    try:
        async with create_session(
            session_string, 
            user_id, 
            timeout=timeout, 
            validate=False
        ) as client:
            return client.is_connected
    except Exception:
        return False


# ============================================================================
# SESSION CONTEXT HELPER CLASS
# ============================================================================

class SessionContext:
    """
    Helper class for managing session lifecycle with error tracking.
    
    Useful when you need to track errors after the context exits.
    
    Usage:
        ctx = SessionContext(session_string, user_id)
        try:
            async with ctx.connect() as client:
                # use client
        except SessionError:
            if ctx.is_permanently_invalid:
                # Clear session from database
                pass
            print(f"Error: {ctx.error}")
    """
    
    def __init__(self, session_string: str, user_id: int):
        """
        Initialize session context.
        
        Args:
            session_string: User's session string
            user_id: User ID for naming
        """
        self.session_string = session_string
        self.user_id = user_id
        self.error: Optional[str] = None
        self.error_type: Optional[str] = None
        self.is_permanently_invalid = False
        self._client: Optional[Client] = None
    
    @asynccontextmanager
    async def connect(
        self, 
        timeout: float = START_TIMEOUT,
        validate: bool = True
    ):
        """
        Connect and yield client, tracking any errors.
        
        Args:
            timeout: Connection timeout
            validate: Whether to validate session
            
        Yields:
            Connected Pyrogram Client
        """
        try:
            async with create_session(
                self.session_string,
                self.user_id,
                timeout=timeout,
                validate=validate
            ) as client:
                self._client = client
                yield client
                
        except SessionInvalidError as e:
            self.error = str(e)
            self.error_type = e.error_type
            self.is_permanently_invalid = True
            raise
            
        except SessionTimeoutError as e:
            self.error = str(e)
            self.error_type = "timeout"
            raise
            
        except SessionConnectionError as e:
            self.error = str(e)
            self.error_type = "connection"
            raise
            
        except Exception as e:
            self.error = str(e)
            self.error_type = "unknown"
            raise
            
        finally:
            self._client = None
    
    async def validate(self) -> SessionResult:
        """
        Validate session and update error state.
        
        Returns:
            SessionResult with validation outcome
        """
        result = await validate_session(self.session_string, self.user_id)
        
        if result.status == SessionStatus.INVALID:
            self.is_permanently_invalid = True
            self.error = result.error
        elif result.status != SessionStatus.VALID:
            self.error = result.error
        
        return result


# ============================================================================
# BACKWARDS COMPATIBILITY
# ============================================================================

# Alias for backwards compatibility with existing code
create_user_session = create_session
