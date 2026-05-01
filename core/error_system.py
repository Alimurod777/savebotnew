"""
core/error_system.py - Production-Grade Error Handling System

DESIGN PRINCIPLES:
1. No uncaught exceptions - every failure is classified and handled
2. No infinite loops - all retries have caps
3. No silent failures - everything is logged with context
4. No raw errors to users - sanitized messages only
5. Self-healing where possible - graceful degradation elsewhere

ERROR CATEGORIES:
A. Network & Transport (retryable with backoff)
B. Telegram API (validate before send, handle specific codes)
C. Content & Entity (never trust Markdown, validate bounds)
D. Media & Download (support resume, track progress)

LOGGING REQUIREMENTS:
- error_type, chat_id, message_id, topic_id
- caption_length, entity_count, entity_bounds
- retry_count, session_state
- Rotating files + optional admin channel
"""

import asyncio
import logging
import traceback
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Callable, TypeVar, Set
from enum import Enum, auto
from functools import wraps

from pyrogram.errors import (
    FloodWait,
    AuthKeyDuplicated,
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionRevoked,
    SessionExpired,
    SessionPasswordNeeded,
    UserDeactivated,
    UserDeactivatedBan,
    ChannelPrivate,
    ChannelInvalid,
    ChatAdminRequired,
    PeerIdInvalid,
    MsgIdInvalid,
    MessageIdInvalid,
    MessageNotModified,
    MessageTooLong,
    MediaCaptionTooLong,
    FileReferenceExpired,
    FileReferenceInvalid,
    Timeout,
    NetworkMigrate,
    InternalServerError,
    ServiceUnavailable,
    BadRequest,
    Unauthorized,
    Forbidden,
    RPCError,
)

logger = logging.getLogger(__name__)

T = TypeVar('T')


# ==================== ERROR CATEGORIES ====================

class ErrorCategory(Enum):
    """Error classification categories."""
    NETWORK_TRANSPORT = auto()      # A: Network/connection issues
    TELEGRAM_API = auto()           # B: Telegram-specific API errors
    CONTENT_ENTITY = auto()         # C: Content/entity validation errors
    MEDIA_DOWNLOAD = auto()         # D: Media/file operations
    SESSION_FATAL = auto()          # Fatal session errors
    VALIDATION = auto()             # Pre-API validation failures
    UNKNOWN = auto()                # Unclassified errors


class ErrorSeverity(Enum):
    """Error severity levels for logging."""
    INFO = "INFO"           # Normal flow
    WARNING = "WARNING"     # Recoverable anomaly
    ERROR = "ERROR"         # Failed operation
    CRITICAL = "CRITICAL"   # Session/engine shutdown


class RecoveryAction(Enum):
    """Recommended recovery action."""
    RETRY = auto()          # Retry with backoff
    RETRY_IMMEDIATE = auto() # Retry immediately (FloodWait handled)
    SKIP = auto()           # Skip this item, continue pipeline
    ABORT_TASK = auto()     # Abort current task, continue others
    RELOGIN = auto()        # Session invalid, needs re-login
    SHUTDOWN = auto()       # Fatal, stop engine


# ==================== ERROR CONTEXT ====================

@dataclass
class ErrorContext:
    """Comprehensive error context for logging and recovery."""
    # Error identification
    error_type: str
    error_message: str
    category: ErrorCategory
    severity: ErrorSeverity
    recovery_action: RecoveryAction
    
    # Operation context
    operation: str = ""
    chat_id: Optional[int] = None
    message_id: Optional[int] = None
    topic_id: Optional[int] = None
    
    # Content context
    caption_length: Optional[int] = None
    text_length: Optional[int] = None
    entity_count: int = 0
    entity_bounds: List[tuple] = field(default_factory=list)
    
    # Retry context
    retry_count: int = 0
    max_retries: int = 0
    
    # Session context
    session_state: str = "unknown"
    user_id: Optional[int] = None
    
    # Timestamps
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Original exception
    original_exception: Optional[Exception] = None
    traceback_str: str = ""
    
    # User-safe message
    user_message: str = "An error occurred. Please try again."
    
    def to_log_dict(self) -> Dict[str, Any]:
        """Convert to dict for structured logging."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "error_type": self.error_type,
            "category": self.category.name,
            "severity": self.severity.value,
            "recovery": self.recovery_action.name,
            "operation": self.operation,
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "topic_id": self.topic_id,
            "caption_length": self.caption_length,
            "text_length": self.text_length,
            "entity_count": self.entity_count,
            "entity_bounds": self.entity_bounds[:5],  # First 5 only
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "session_state": self.session_state,
            "user_id": self.user_id,
            "error_message": self.error_message[:500],  # Truncate
        }
    
    def format_log_message(self) -> str:
        """Format for log file."""
        parts = [
            f"[{self.severity.value}]",
            f"[{self.category.name}]",
            f"{self.error_type}:",
            self.error_message[:200],
        ]
        
        if self.chat_id:
            parts.append(f"| chat={self.chat_id}")
        if self.message_id:
            parts.append(f"msg={self.message_id}")
        if self.topic_id:
            parts.append(f"topic={self.topic_id}")
        if self.retry_count > 0:
            parts.append(f"| retry={self.retry_count}/{self.max_retries}")
        if self.entity_count > 0:
            parts.append(f"| entities={self.entity_count}")
        
        return " ".join(parts)


# ==================== ERROR CLASSIFICATION ====================

# Session-fatal errors (require re-login)
SESSION_FATAL_ERRORS = (
    AuthKeyDuplicated,
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionRevoked,
    SessionExpired,
    UserDeactivated,
    UserDeactivatedBan,
)

# Network/transport errors (retryable)
NETWORK_ERRORS = (
    Timeout,
    NetworkMigrate,
    InternalServerError,
    ServiceUnavailable,
    ConnectionError,
    ConnectionResetError,
    TimeoutError,
    OSError,
    asyncio.TimeoutError,
)

# Item-skip errors (skip this item, continue pipeline)
SKIP_ERRORS = (
    MsgIdInvalid,
    MessageIdInvalid,
    ChannelPrivate,
    ChannelInvalid,
    ChatAdminRequired,
    PeerIdInvalid,
)

# File reference errors (refresh and retry)
FILE_REF_ERRORS = (
    FileReferenceExpired,
    FileReferenceInvalid,
)

# Content/entity error patterns (in error message)
ENTITY_ERROR_PATTERNS = {
    "ENTITY_BOUNDS_INVALID",
    "ENTITY_MENTION_USER_INVALID",
    "MESSAGE_TOO_LONG",
    "MEDIA_CAPTION_TOO_LONG",
}


def classify_error(
    error: Exception,
    operation: str = "",
    chat_id: int = None,
    message_id: int = None,
    topic_id: int = None,
    caption_length: int = None,
    entity_count: int = 0,
    entity_bounds: List[tuple] = None,
    retry_count: int = 0,
    max_retries: int = 3,
    session_state: str = "connected",
    user_id: int = None,
) -> ErrorContext:
    """
    Classify an exception into a structured ErrorContext.
    
    This is the MAIN entry point for error handling.
    All errors should go through this function.
    """
    error_type = type(error).__name__
    error_message = str(error)
    error_upper = error_message.upper()
    
    # Default values
    category = ErrorCategory.UNKNOWN
    severity = ErrorSeverity.ERROR
    recovery = RecoveryAction.ABORT_TASK
    user_message = "An error occurred. Please try again."
    
    # A. Session-fatal errors
    if isinstance(error, SESSION_FATAL_ERRORS):
        category = ErrorCategory.SESSION_FATAL
        severity = ErrorSeverity.CRITICAL
        recovery = RecoveryAction.RELOGIN
        user_message = "Session expired. Please /login again."
    
    # B. FloodWait (special handling)
    elif isinstance(error, FloodWait):
        wait_time = getattr(error, 'value', getattr(error, 'x', 30))
        category = ErrorCategory.TELEGRAM_API
        severity = ErrorSeverity.WARNING
        recovery = RecoveryAction.RETRY_IMMEDIATE
        user_message = f"Rate limited. Waiting {wait_time} seconds..."
    
    # C. Network/transport errors
    elif isinstance(error, NETWORK_ERRORS):
        category = ErrorCategory.NETWORK_TRANSPORT
        severity = ErrorSeverity.WARNING
        recovery = RecoveryAction.RETRY
        user_message = "Connection issue. Retrying automatically..."
    
    # D. Item-skip errors
    elif isinstance(error, SKIP_ERRORS):
        category = ErrorCategory.TELEGRAM_API
        severity = ErrorSeverity.WARNING
        recovery = RecoveryAction.SKIP
        
        if isinstance(error, (ChannelPrivate, ChannelInvalid)):
            user_message = "Cannot access this channel. Check membership."
        elif isinstance(error, (MsgIdInvalid, MessageIdInvalid)):
            user_message = "Message not found or deleted."
        else:
            user_message = "Cannot access this content."
    
    # E. File reference errors
    elif isinstance(error, FILE_REF_ERRORS):
        category = ErrorCategory.MEDIA_DOWNLOAD
        severity = ErrorSeverity.WARNING
        recovery = RecoveryAction.RETRY
        user_message = "Media reference expired. Refreshing..."
    
    # F. Content/entity errors (check message patterns)
    elif any(pattern in error_upper for pattern in ENTITY_ERROR_PATTERNS):
        category = ErrorCategory.CONTENT_ENTITY
        severity = ErrorSeverity.ERROR
        recovery = RecoveryAction.SKIP
        
        if "ENTITY_BOUNDS" in error_upper:
            user_message = "Text formatting error. Sending without formatting."
        elif "MESSAGE_TOO_LONG" in error_upper:
            user_message = "Message too long. Will be split."
        elif "CAPTION_TOO_LONG" in error_upper:
            user_message = "Caption too long. Will be split."
        else:
            user_message = "Content formatting issue detected."
    
    # G. Other Telegram API errors
    elif isinstance(error, RPCError):
        category = ErrorCategory.TELEGRAM_API
        
        # Check for retryable patterns
        if any(p in error_upper for p in ["TIMEOUT", "NETWORK", "SERVICE_UNAVAILABLE"]):
            severity = ErrorSeverity.WARNING
            recovery = RecoveryAction.RETRY
            user_message = "Telegram service temporarily unavailable. Retrying..."
        else:
            severity = ErrorSeverity.ERROR
            recovery = RecoveryAction.SKIP
            user_message = "Telegram API error. Skipping this item."
    
    # H. Check if retryable based on retry count
    if recovery == RecoveryAction.RETRY and retry_count >= max_retries:
        recovery = RecoveryAction.ABORT_TASK
        user_message = "Operation failed after multiple retries."
    
    # Build context
    ctx = ErrorContext(
        error_type=error_type,
        error_message=error_message,
        category=category,
        severity=severity,
        recovery_action=recovery,
        operation=operation,
        chat_id=chat_id,
        message_id=message_id,
        topic_id=topic_id,
        caption_length=caption_length,
        entity_count=entity_count,
        entity_bounds=entity_bounds or [],
        retry_count=retry_count,
        max_retries=max_retries,
        session_state=session_state,
        user_id=user_id,
        original_exception=error,
        traceback_str=traceback.format_exc(),
        user_message=user_message,
    )
    
    return ctx


# ==================== LOGGING ====================

def log_error(ctx: ErrorContext) -> None:
    """Log error with appropriate level and context."""
    log_msg = ctx.format_log_message()
    
    if ctx.severity == ErrorSeverity.CRITICAL:
        logger.critical(log_msg, extra=ctx.to_log_dict())
    elif ctx.severity == ErrorSeverity.ERROR:
        logger.error(log_msg, extra=ctx.to_log_dict())
    elif ctx.severity == ErrorSeverity.WARNING:
        logger.warning(log_msg, extra=ctx.to_log_dict())
    else:
        logger.info(log_msg, extra=ctx.to_log_dict())


# ==================== RETRY DECORATOR ====================

# Caps to prevent infinite loops
MAX_RETRIES = 5
MAX_FLOODWAIT = 300  # 5 minutes
MAX_BACKOFF = 60     # 1 minute


def with_error_handling(
    operation: str = "",
    max_retries: int = MAX_RETRIES,
    max_floodwait: int = MAX_FLOODWAIT,
    max_backoff: float = MAX_BACKOFF,
    on_skip: Optional[Callable] = None,
    on_fatal: Optional[Callable] = None,
):
    """
    Decorator for comprehensive error handling.
    
    Features:
    - Classifies all errors
    - Logs with full context
    - Retries transient errors with backoff
    - Handles FloodWait correctly
    - Escalates fatal errors
    - Never crashes the pipeline
    
    Usage:
        @with_error_handling(operation="fetch_message", max_retries=3)
        async def fetch_message(client, chat_id, msg_id):
            return await client.get_messages(chat_id, msg_id)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            retry_count = 0
            last_ctx = None
            
            # Extract context from kwargs if available
            chat_id = kwargs.get('chat_id')
            message_id = kwargs.get('message_id')
            topic_id = kwargs.get('topic_id')
            user_id = kwargs.get('user_id')
            
            while True:
                try:
                    return await func(*args, **kwargs)
                
                except Exception as e:
                    # Classify error
                    ctx = classify_error(
                        error=e,
                        operation=operation or func.__name__,
                        chat_id=chat_id,
                        message_id=message_id,
                        topic_id=topic_id,
                        retry_count=retry_count,
                        max_retries=max_retries,
                        user_id=user_id,
                    )
                    last_ctx = ctx
                    
                    # Log
                    log_error(ctx)
                    
                    # Handle based on recovery action
                    if ctx.recovery_action == RecoveryAction.RETRY_IMMEDIATE:
                        # FloodWait - sleep exact time
                        if isinstance(e, FloodWait):
                            wait = getattr(e, 'value', getattr(e, 'x', 30))
                            if wait > max_floodwait:
                                raise TelegramError(ctx)
                            await asyncio.sleep(wait)
                            continue
                    
                    elif ctx.recovery_action == RecoveryAction.RETRY:
                        if retry_count < max_retries:
                            retry_count += 1
                            backoff = min(2 ** retry_count, max_backoff)
                            await asyncio.sleep(backoff)
                            continue
                        # Max retries exceeded
                        raise TelegramError(ctx)
                    
                    elif ctx.recovery_action == RecoveryAction.SKIP:
                        if on_skip:
                            on_skip(ctx)
                        raise SkipItemError(ctx)
                    
                    elif ctx.recovery_action == RecoveryAction.RELOGIN:
                        if on_fatal:
                            on_fatal(ctx)
                        raise SessionFatalError(ctx)
                    
                    elif ctx.recovery_action == RecoveryAction.SHUTDOWN:
                        if on_fatal:
                            on_fatal(ctx)
                        raise EngineShutdownError(ctx)
                    
                    else:
                        # ABORT_TASK or unknown
                        raise TelegramError(ctx)
        
        return wrapper
    return decorator


# ==================== CUSTOM EXCEPTIONS ====================

class TelegramError(Exception):
    """Base exception with error context."""
    def __init__(self, ctx: ErrorContext):
        self.ctx = ctx
        super().__init__(ctx.user_message)


class SkipItemError(TelegramError):
    """Item should be skipped, continue pipeline."""
    pass


class SessionFatalError(TelegramError):
    """Session is invalid, needs re-login."""
    pass


class EngineShutdownError(TelegramError):
    """Fatal error, engine must shut down."""
    pass


class ValidationError(TelegramError):
    """Pre-API validation failed."""
    pass


class EntityBoundsError(TelegramError):
    """Entity bounds validation failed."""
    pass


# ==================== VALIDATION FUNCTIONS ====================

def validate_text_length(
    text: str,
    limit: int,
    operation: str = "",
) -> ErrorContext:
    """
    Validate text length before API call.
    
    Returns None if valid, ErrorContext if invalid.
    """
    if not text:
        return None
    
    # Use UTF-16 length (Telegram's native unit)
    try:
        utf16_len = len(text.encode('utf-16-le', errors='surrogatepass')) // 2
    except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
        utf16_len = sum(2 if ord(c) > 0xFFFF else 1 for c in text)
    
    if utf16_len <= limit:
        return None
    
    return ErrorContext(
        error_type="TextTooLong",
        error_message=f"Text length {utf16_len} exceeds limit {limit}",
        category=ErrorCategory.VALIDATION,
        severity=ErrorSeverity.WARNING,
        recovery_action=RecoveryAction.SKIP,
        operation=operation,
        text_length=utf16_len,
        user_message=f"Text too long ({utf16_len}/{limit}). Will be split.",
    )


def validate_entity_bounds(
    text: str,
    entities: list,
    operation: str = "",
) -> Optional[ErrorContext]:
    """
    Validate entity bounds before API call.
    
    CRITICAL: This MUST be called before any send with entities.
    
    Returns None if valid, ErrorContext if invalid.
    """
    if not entities or not text:
        return None
    
    # UTF-16 length
    try:
        text_len = len(text.encode('utf-16-le', errors='surrogatepass')) // 2
    except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
        text_len = sum(2 if ord(c) > 0xFFFF else 1 for c in text)
    invalid_bounds = []
    
    for e in entities:
        offset = getattr(e, 'offset', 0)
        length = getattr(e, 'length', 0)
        end = offset + length
        
        if offset < 0 or length <= 0 or end > text_len:
            invalid_bounds.append((offset, length, end, text_len))
    
    if not invalid_bounds:
        return None
    
    return ErrorContext(
        error_type="EntityBoundsInvalid",
        error_message=f"Invalid entity bounds: {invalid_bounds[:3]}",
        category=ErrorCategory.CONTENT_ENTITY,
        severity=ErrorSeverity.ERROR,
        recovery_action=RecoveryAction.SKIP,
        operation=operation,
        text_length=text_len,
        entity_count=len(entities),
        entity_bounds=[(e.offset, e.length) for e in entities[:10]],
        user_message="Text formatting error. Sending without formatting.",
    )


def validate_caption(
    caption: str,
    entities: list = None,
    limit: int = 1024,
    operation: str = "",
) -> Optional[ErrorContext]:
    """
    Validate caption before media send.
    
    Checks:
    1. Caption length (UTF-16)
    2. Entity bounds
    """
    # Length check
    ctx = validate_text_length(caption, limit, operation)
    if ctx:
        return ctx
    
    # Entity bounds check
    if entities:
        ctx = validate_entity_bounds(caption, entities, operation)
        if ctx:
            return ctx
    
    return None


def validate_message(
    text: str,
    entities: list = None,
    limit: int = 4096,
    operation: str = "",
) -> Optional[ErrorContext]:
    """
    Validate message before send.
    
    Checks:
    1. Text length (UTF-16)
    2. Entity bounds
    """
    # Length check
    ctx = validate_text_length(text, limit, operation)
    if ctx:
        return ctx
    
    # Entity bounds check
    if entities:
        ctx = validate_entity_bounds(text, entities, operation)
        if ctx:
            return ctx
    
    return None


# ==================== SAFE OPERATION WRAPPERS ====================

async def safe_send_message(
    client,
    chat_id: int,
    text: str,
    entities: list = None,
    **kwargs
) -> tuple:
    """
    Safely send a message with full validation and error handling.
    
    Returns:
        (success: bool, result_or_error: Message | ErrorContext)
    """
    # Pre-validation
    ctx = validate_message(text, entities, operation="send_message")
    if ctx:
        log_error(ctx)
        if entities:
            # Retry without entities
            return await safe_send_message(client, chat_id, text, entities=None, **kwargs)
        return False, ctx
    
    try:
        msg = await client.send_message(
            chat_id=chat_id,
            text=text,
            entities=entities,
            **kwargs
        )
        return True, msg
    
    except Exception as e:
        ctx = classify_error(
            error=e,
            operation="send_message",
            chat_id=chat_id,
        )
        log_error(ctx)
        
        # If entity error, retry without entities
        if ctx.category == ErrorCategory.CONTENT_ENTITY and entities:
            return await safe_send_message(client, chat_id, text, entities=None, **kwargs)
        
        return False, ctx


async def safe_send_media(
    client,
    chat_id: int,
    media_type: str,  # "photo", "video", "document", etc.
    media: str,
    caption: str = None,
    caption_entities: list = None,
    **kwargs
) -> tuple:
    """
    Safely send media with full validation and error handling.
    
    Returns:
        (success: bool, result_or_error: Message | ErrorContext)
    """
    # Pre-validation
    if caption:
        ctx = validate_caption(caption, caption_entities, operation=f"send_{media_type}")
        if ctx:
            log_error(ctx)
            if caption_entities:
                # Retry without entities
                return await safe_send_media(
                    client, chat_id, media_type, media,
                    caption=caption, caption_entities=None, **kwargs
                )
            # Truncate caption
            caption = caption[:1020] + "..." if len(caption) > 1024 else caption
    
    try:
        send_func = getattr(client, f"send_{media_type}")
        msg = await send_func(
            chat_id=chat_id,
            **{media_type: media},
            caption=caption,
            caption_entities=caption_entities,
            **kwargs
        )
        return True, msg
    
    except Exception as e:
        ctx = classify_error(
            error=e,
            operation=f"send_{media_type}",
            chat_id=chat_id,
            caption_length=len(caption) if caption else 0,
        )
        log_error(ctx)
        
        # If entity error, retry without entities
        if ctx.category == ErrorCategory.CONTENT_ENTITY and caption_entities:
            return await safe_send_media(
                client, chat_id, media_type, media,
                caption=caption, caption_entities=None, **kwargs
            )
        
        return False, ctx


# ==================== USER-FACING MESSAGES ====================

USER_MESSAGES = {
    ErrorCategory.NETWORK_TRANSPORT: "Connection issue. Retrying automatically...",
    ErrorCategory.TELEGRAM_API: "Telegram service error. Please try again.",
    ErrorCategory.CONTENT_ENTITY: "Content formatting issue. Sending simplified version.",
    ErrorCategory.MEDIA_DOWNLOAD: "Media download issue. Retrying...",
    ErrorCategory.SESSION_FATAL: "Session expired. Please /login again.",
    ErrorCategory.VALIDATION: "Content validation failed.",
    ErrorCategory.UNKNOWN: "An error occurred. Please try again.",
}


def get_user_message(ctx: ErrorContext) -> str:
    """Get user-safe error message (no technical details)."""
    return ctx.user_message or USER_MESSAGES.get(ctx.category, USER_MESSAGES[ErrorCategory.UNKNOWN])


# ==================== ADMIN NOTIFICATION ====================

async def notify_admin(
    client,
    admin_chat_id: int,
    ctx: ErrorContext,
    include_traceback: bool = False
) -> None:
    """
    Send error notification to admin channel.
    
    Only for ERROR and CRITICAL severity.
    """
    if ctx.severity not in (ErrorSeverity.ERROR, ErrorSeverity.CRITICAL):
        return
    
    lines = [
        f"**[{ctx.severity.value}] {ctx.error_type}**",
        f"",
        f"**Category:** {ctx.category.name}",
        f"**Operation:** {ctx.operation}",
        f"**Recovery:** {ctx.recovery_action.name}",
    ]
    
    if ctx.chat_id:
        lines.append(f"**Chat:** `{ctx.chat_id}`")
    if ctx.message_id:
        lines.append(f"**Message:** `{ctx.message_id}`")
    if ctx.topic_id:
        lines.append(f"**Topic:** `{ctx.topic_id}`")
    if ctx.user_id:
        lines.append(f"**User:** `{ctx.user_id}`")
    
    lines.append(f"")
    lines.append(f"**Error:** {ctx.error_message[:500]}")
    
    if ctx.retry_count > 0:
        lines.append(f"**Retries:** {ctx.retry_count}/{ctx.max_retries}")
    
    if include_traceback and ctx.traceback_str:
        tb_short = ctx.traceback_str[-1000:]  # Last 1000 chars
        lines.append(f"")
        lines.append(f"```\n{tb_short}\n```")
    
    try:
        await client.send_message(
            admin_chat_id,
            "\n".join(lines),
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Failed to notify admin: {e}")
