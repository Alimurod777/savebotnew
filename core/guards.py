"""
core/guards.py - Hardening Guards for Telegram Operations

DESIGN: Additive validation layers that EXTEND existing functionality.
Does NOT modify core flow, handlers, or data models.

Guards provided:
1. Channel/Group validation against dialog cache
2. Message range safety (skip failures, no abort)
3. Topic-aware message retrieval
4. Entity bounds pre-send validation
5. Timeout-aware operation wrappers
6. Error classification and diagnostics

Usage:
    from core.guards import (
        validate_channel_access,
        safe_get_messages_range,
        validate_entities_before_send,
        guarded_edit_message,
        classify_error
    )

CRITICAL: These guards are ADDITIVE - they wrap existing operations
without changing the underlying logic.
"""

import asyncio
import logging
import time
from typing import Optional, List, Dict, Any, Tuple, Union, Set
from dataclasses import dataclass, field
from enum import Enum

from pyrogram import Client
from pyrogram.types import Message, MessageEntity, Dialog
from pyrogram.errors import (
    ChannelInvalid, ChannelPrivate, ChatIdInvalid,
    PeerIdInvalid, UsernameInvalid, UsernameNotOccupied,
    MessageIdInvalid, MessageNotModified, MessageEmpty,
    FloodWait, Timeout, RPCError,
    FileReferenceExpired, FileReferenceInvalid,
    AuthKeyUnregistered, SessionRevoked, UserDeactivated
)

logger = logging.getLogger(__name__)

def _load_topic_extractor():
    try:
        from core.topic_extractor import TopicExtractor, TopicExtractorConfig
        return TopicExtractor, TopicExtractorConfig
    except ImportError:
        return None, None


# ============================================================================
# ERROR CLASSIFICATION
# ============================================================================

class ErrorCategory(Enum):
    """Error categories for structured diagnostics."""
    NETWORK = "network"
    ENTITY = "entity"
    PERMISSION = "permission"
    RESOLUTION = "resolution"
    SESSION = "session"
    RATE_LIMIT = "rate_limit"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


@dataclass
class ErrorDiagnostic:
    """Structured error information for logging and user feedback."""
    category: ErrorCategory
    error_type: str
    message: str
    recoverable: bool
    context: Dict[str, Any] = field(default_factory=dict)
    user_message: str = ""
    
    def __str__(self):
        return f"[{self.category.value}] {self.error_type}: {self.message}"


def classify_error(
    error: Exception,
    context: Dict[str, Any] = None
) -> ErrorDiagnostic:
    """
    Classify an error for structured handling.
    
    Args:
        error: The exception to classify
        context: Optional context (chat_id, msg_id, range, topic_id, etc.)
    
    Returns:
        ErrorDiagnostic with classification and recovery info
    """
    ctx = context or {}
    error_str = str(error)
    error_type = type(error).__name__
    
    # Session fatal errors
    if isinstance(error, (AuthKeyUnregistered, SessionRevoked, UserDeactivated)):
        return ErrorDiagnostic(
            category=ErrorCategory.SESSION,
            error_type=error_type,
            message=error_str,
            recoverable=False,
            context=ctx,
            user_message="Session is invalid. Please login again with /login"
        )
    
    # Rate limits
    if isinstance(error, FloodWait):
        wait_time = getattr(error, 'value', getattr(error, 'x', 30))
        return ErrorDiagnostic(
            category=ErrorCategory.RATE_LIMIT,
            error_type=error_type,
            message=f"Rate limited for {wait_time}s",
            recoverable=True,
            context={**ctx, 'wait_seconds': wait_time},
            user_message=f"Rate limited. Please wait {wait_time} seconds."
        )
    
    # Channel/Chat resolution errors
    if isinstance(error, (ChannelInvalid, ChannelPrivate, ChatIdInvalid, PeerIdInvalid)):
        return ErrorDiagnostic(
            category=ErrorCategory.RESOLUTION,
            error_type=error_type,
            message=error_str,
            recoverable=False,
            context=ctx,
            user_message="Cannot access this channel. Check if you're a member or if the link is correct."
        )
    
    # Username resolution errors
    if isinstance(error, (UsernameInvalid, UsernameNotOccupied)):
        return ErrorDiagnostic(
            category=ErrorCategory.RESOLUTION,
            error_type=error_type,
            message=error_str,
            recoverable=False,
            context=ctx,
            user_message="Invalid username or channel not found."
        )
    
    # Message not found
    if isinstance(error, (MessageIdInvalid, MessageEmpty)):
        return ErrorDiagnostic(
            category=ErrorCategory.NOT_FOUND,
            error_type=error_type,
            message=error_str,
            recoverable=False,
            context=ctx,
            user_message="Message not found or was deleted."
        )
    
    # Entity errors
    if isinstance(error, RPCError):
        error_id = getattr(error, 'ID', '') or ''
        if 'ENTITY' in error_id.upper():
            return ErrorDiagnostic(
                category=ErrorCategory.ENTITY,
                error_type=error_type,
                message=error_str,
                recoverable=True,  # Can retry without entities
                context=ctx,
                user_message="Message formatting error. Sending without formatting."
            )
    
    # File reference errors
    if isinstance(error, (FileReferenceExpired, FileReferenceInvalid)):
        return ErrorDiagnostic(
            category=ErrorCategory.NOT_FOUND,
            error_type=error_type,
            message=error_str,
            recoverable=True,  # Can refresh
            context=ctx,
            user_message="File reference expired. Refreshing..."
        )
    
    # Network/timeout errors
    if isinstance(error, (Timeout, TimeoutError, asyncio.TimeoutError)):
        return ErrorDiagnostic(
            category=ErrorCategory.NETWORK,
            error_type=error_type,
            message=error_str,
            recoverable=True,
            context=ctx,
            user_message="Operation timed out. Retrying..."
        )
    
    if isinstance(error, (OSError, ConnectionError, ConnectionResetError)):
        return ErrorDiagnostic(
            category=ErrorCategory.NETWORK,
            error_type=error_type,
            message=error_str,
            recoverable=True,
            context=ctx,
            user_message="Network error. Retrying..."
        )
    
    # Permission errors
    if 'ADMIN' in error_str.upper() or 'FORBIDDEN' in error_str.upper():
        return ErrorDiagnostic(
            category=ErrorCategory.PERMISSION,
            error_type=error_type,
            message=error_str,
            recoverable=False,
            context=ctx,
            user_message="Permission denied. Admin rights may be required."
        )
    
    # Unknown
    return ErrorDiagnostic(
        category=ErrorCategory.UNKNOWN,
        error_type=error_type,
        message=error_str,
        recoverable=False,
        context=ctx,
        user_message=f"An error occurred: {error_type}"
    )


# ============================================================================
# CHANNEL VALIDATION GUARD
# ============================================================================

@dataclass
class ChannelValidationResult:
    """Result of channel validation."""
    valid: bool
    chat_id: Optional[int] = None
    access_hash: Optional[int] = None
    title: Optional[str] = None
    error: Optional[str] = None
    diagnostic: Optional[ErrorDiagnostic] = None


# Cache for validated channels (per session)
_channel_cache: Dict[int, Dict[Union[int, str], ChannelValidationResult]] = {}
_cache_ttl = 300  # 5 minutes
_cache_timestamps: Dict[int, Dict[Union[int, str], float]] = {}


async def validate_channel_access(
    client: Client,
    chat_identifier: Union[int, str],
    use_cache: bool = True,
    force_refresh: bool = False
) -> ChannelValidationResult:
    """
    Validate channel access BEFORE any API calls.
    
    This guard verifies the channel against the user's dialog cache
    to prevent CHANNEL_INVALID errors.
    
    Args:
        client: Pyrogram Client (user session)
        chat_identifier: Channel ID (int) or username (str)
        use_cache: Use cached validation results
        force_refresh: Force fresh validation even if cached
    
    Returns:
        ChannelValidationResult with validity and access info
    """
    # Get session identifier for caching
    session_id = id(client)
    
    # Check cache
    if use_cache and not force_refresh:
        if session_id in _channel_cache:
            cached = _channel_cache[session_id].get(chat_identifier)
            if cached:
                timestamp = _cache_timestamps.get(session_id, {}).get(chat_identifier, 0)
                if time.time() - timestamp < _cache_ttl:
                    return cached
    
    # Normalize chat_id for private channels
    normalized_id = chat_identifier
    if isinstance(chat_identifier, int) and chat_identifier > 0:
        # Might be a raw channel ID without -100 prefix
        if chat_identifier > 1000000000:  # Likely already has prefix
            pass
        else:
            normalized_id = int(f"-100{chat_identifier}")
    elif isinstance(chat_identifier, int) and chat_identifier < 0:
        if not str(chat_identifier).startswith("-100"):
            normalized_id = int(f"-100{abs(chat_identifier)}")
    
    try:
        # Method 1: Try get_chat directly (fastest)
        chat = await client.get_chat(normalized_id)
        
        result = ChannelValidationResult(
            valid=True,
            chat_id=chat.id,
            access_hash=getattr(chat, 'access_hash', None),
            title=getattr(chat, 'title', None) or getattr(chat, 'first_name', 'Unknown')
        )
        
        # Cache result
        _update_cache(session_id, chat_identifier, result)
        return result
        
    except (ChannelInvalid, ChannelPrivate, ChatIdInvalid, PeerIdInvalid) as e:
        # Try to find in dialogs as fallback
        # FIX Issue 4.1: Add limit to dialog scan for performance
        try:
            dialog_count = 0
            dialog_limit = 500  # Reasonable limit to prevent long scans
            
            async for dialog in client.get_dialogs(limit=dialog_limit):
                dialog_count += 1
                dialog_id = dialog.chat.id
                dialog_username = getattr(dialog.chat, 'username', None)
                
                if dialog_id == normalized_id or dialog_id == chat_identifier:
                    result = ChannelValidationResult(
                        valid=True,
                        chat_id=dialog_id,
                        access_hash=getattr(dialog.chat, 'access_hash', None),
                        title=getattr(dialog.chat, 'title', None)
                    )
                    _update_cache(session_id, chat_identifier, result)
                    return result
                
                if isinstance(chat_identifier, str) and dialog_username:
                    if dialog_username.lower() == chat_identifier.lower().lstrip('@'):
                        result = ChannelValidationResult(
                            valid=True,
                            chat_id=dialog_id,
                            access_hash=getattr(dialog.chat, 'access_hash', None),
                            title=getattr(dialog.chat, 'title', None)
                        )
                        _update_cache(session_id, chat_identifier, result)
                        return result
        except Exception:
            pass
        
        # Validation failed
        diagnostic = classify_error(e, {'chat_identifier': chat_identifier})
        result = ChannelValidationResult(
            valid=False,
            error=str(e),
            diagnostic=diagnostic
        )
        _update_cache(session_id, chat_identifier, result)
        return result
        
    except Exception as e:
        diagnostic = classify_error(e, {'chat_identifier': chat_identifier})
        return ChannelValidationResult(
            valid=False,
            error=str(e),
            diagnostic=diagnostic
        )


def _update_cache(session_id: int, identifier: Union[int, str], result: ChannelValidationResult):
    """Update the validation cache."""
    if session_id not in _channel_cache:
        _channel_cache[session_id] = {}
        _cache_timestamps[session_id] = {}
    
    _channel_cache[session_id][identifier] = result
    _cache_timestamps[session_id][identifier] = time.time()


def clear_channel_cache(session_id: int = None):
    """Clear channel validation cache."""
    if session_id:
        _channel_cache.pop(session_id, None)
        _cache_timestamps.pop(session_id, None)
    else:
        _channel_cache.clear()
        _cache_timestamps.clear()


# ============================================================================
# MESSAGE RANGE SAFETY GUARD
# ============================================================================

@dataclass
class MessageRangeResult:
    """Result of safe message range retrieval."""
    messages: List[Message]
    skipped_ids: List[int]
    errors: Dict[int, ErrorDiagnostic]
    total_requested: int
    
    @property
    def success_count(self) -> int:
        return len(self.messages)
    
    @property
    def skip_count(self) -> int:
        return len(self.skipped_ids)


async def safe_get_messages_range(
    client: Client,
    chat_id: Union[int, str],
    message_ids: List[int],
    validate_channel: bool = True,
    batch_size: int = 100,
    inter_batch_delay: float = 0.3
) -> MessageRangeResult:
    """
    Safely retrieve a range of messages with skip-on-failure logic.
    
    CRITICAL: A single missing/deleted message does NOT abort the range.
    Each message ID is treated as independent.
    
    Args:
        client: Pyrogram Client
        chat_id: Channel/chat ID or username
        message_ids: List of message IDs to retrieve
        validate_channel: Validate channel access first
        batch_size: Messages per batch (max 100)
        inter_batch_delay: Delay between batches to avoid FloodWait
    
    Returns:
        MessageRangeResult with successful messages and skip info
    """
    result = MessageRangeResult(
        messages=[],
        skipped_ids=[],
        errors={},
        total_requested=len(message_ids)
    )
    
    if not message_ids:
        return result
    
    # Validate channel access first
    if validate_channel:
        validation = await validate_channel_access(client, chat_id)
        if not validation.valid:
            logger.warning(f"Channel validation failed: {validation.error}")
            # Mark all as skipped with the same error
            result.skipped_ids = message_ids.copy()
            for msg_id in message_ids:
                result.errors[msg_id] = validation.diagnostic
            return result
        
        # Use validated chat_id
        chat_id = validation.chat_id or chat_id
    
    # Process in batches
    batch_size = min(batch_size, 100)  # Telegram limit
    
    i = 0
    while i < len(message_ids):
        batch_ids = message_ids[i:i + batch_size]

        try:
            # Fetch batch
            messages = await client.get_messages(chat_id, batch_ids)

            # Handle single message or list
            if not isinstance(messages, list):
                messages = [messages]

            # Process each message
            for idx, msg in enumerate(messages):
                if msg and not getattr(msg, 'empty', False):
                    result.messages.append(msg)
                else:
                    if idx < len(batch_ids):
                        msg_id = batch_ids[idx]
                        result.skipped_ids.append(msg_id)
                        result.errors[msg_id] = ErrorDiagnostic(
                            category=ErrorCategory.NOT_FOUND,
                            error_type="MessageEmpty",
                            message="Message deleted or not found",
                            recoverable=False,
                            context={'chat_id': chat_id, 'msg_id': msg_id}
                        )

            # Inter-batch delay
            if i + batch_size < len(message_ids):
                await asyncio.sleep(inter_batch_delay)

            i += batch_size

        except FloodWait as e:
            wait_time = getattr(e, 'value', getattr(e, 'x', 30))
            logger.warning(f"FloodWait {wait_time}s during batch fetch")
            await asyncio.sleep(min(wait_time, 60))
            continue

        except Exception as e:
            # Log but don't abort - mark batch as skipped
            diagnostic = classify_error(e, {'chat_id': chat_id, 'batch_ids': batch_ids})
            logger.warning(f"Batch fetch error: {diagnostic}")

            for msg_id in batch_ids:
                result.skipped_ids.append(msg_id)
                result.errors[msg_id] = diagnostic
            i += batch_size
    
    logger.info(
        f"Message range result: {result.success_count} fetched, "
        f"{result.skip_count} skipped out of {result.total_requested}"
    )
    
    return result


# ============================================================================
# TOPIC-AWARE MESSAGE GUARD
# ============================================================================

def is_message_in_topic(msg: Message, topic_id: int) -> bool:
    """
    Check if a message belongs to a specific topic.
    
    Topic membership is determined by:
    1. Message IS the topic starter (msg.id == topic_id)
    2. Message has message_thread_id == topic_id
    3. Message has reply_to_top_message_id == topic_id
    4. Message has reply_to_message_id == topic_id (direct reply to topic)
    """
    if not msg or getattr(msg, 'empty', False):
        return False
    
    # Method 1: This IS the topic starter
    if msg.id == topic_id:
        return True

    thread_id = getattr(msg, 'message_thread_id', None)
    if thread_id == topic_id:
        return True
    
    # Method 2: reply_to_top_message_id points to topic
    reply_top = getattr(msg, 'reply_to_top_message_id', None)
    reply_to_obj = getattr(msg, 'reply_to', None)
    if reply_top is None and reply_to_obj is not None:
        reply_top = getattr(reply_to_obj, 'reply_to_top_message_id', None)
        if reply_top is None:
            reply_top = getattr(reply_to_obj, 'reply_to_top_id', None)
    if reply_top == topic_id:
        return True
    
    # Method 3: Direct reply to topic starter
    reply_to = getattr(msg, 'reply_to_message_id', None)
    if reply_to is None and reply_to_obj is not None:
        reply_to = getattr(reply_to_obj, 'reply_to_msg_id', None)
    if reply_to == topic_id and not reply_top:
        return True
    
    return False


@dataclass
class TopicRangeResult:
    """Result of smart topic range retrieval."""
    messages: List[Message]
    found_ids: Set[int]
    missing_ids: Set[int]
    out_of_topic_ids: Set[int]
    total_requested: int
    
    @property
    def success_count(self) -> int:
        return len(self.messages)


async def smart_get_topic_range(
    client: Client,
    chat_id: Union[int, str],
    topic_id: int,
    start_id: int,
    end_id: int,
    history_limit: int = 300,
    batch_delay: float = 0.1
) -> TopicRangeResult:
    """
    Smart topic message retrieval - finds ALL topic messages in ID range.
    
    Algorithm:
    1. First pass: Check all IDs in range directly
    2. Second pass: Scan topic history to find scattered IDs
    3. Merge results (deduplicated)
    
    This solves the problem where topic messages have non-sequential IDs:
    - User requests: 101-110
    - Actual topic messages: 101, 103, 107, 120, 125
    - Result: Gets 101, 103, 107 from direct check + 120 from history scan
    
    Args:
        client: Pyrogram Client
        chat_id: Channel/supergroup ID
        topic_id: Topic/thread ID
        start_id: Range start (can be > end_id for reverse)
        end_id: Range end
        history_limit: Max messages to scan from topic history
        batch_delay: Delay between batch requests
    
    Returns:
        TopicRangeResult with all found messages
    """
    TopicExtractor, TopicExtractorConfig = _load_topic_extractor()
    try:
        if not (TopicExtractor and TopicExtractorConfig):
            raise RuntimeError("TopicExtractor unavailable")
        extractor = TopicExtractor(
            client,
            TopicExtractorConfig(
                chat_id=chat_id,
                topic_id=topic_id,
                fetch_batch_size=100,
                inter_page_delay=batch_delay,
                stop_after_ids={start_id, end_id},
            ),
        )
        messages = await extractor.extract_between(start_id, end_id)
        found_ids = {msg.id for msg in messages}
        return TopicRangeResult(
            messages=messages,
            found_ids=found_ids,
            missing_ids={start_id, end_id} - found_ids,
            out_of_topic_ids=set(),
            total_requested=len(messages),
        )
    except Exception as e:
        logger.warning(f"TopicExtractor smart range failed without history scan: {e}")

    return TopicRangeResult(
        messages=[],
        found_ids=set(),
        missing_ids={start_id, end_id},
        out_of_topic_ids=set(),
        total_requested=2,
    )


async def get_all_topic_messages(
    client: Client,
    chat_id: Union[int, str],
    topic_id: int,
    limit: int = 100,
    min_id: int = None,
    max_id: int = None
) -> List[Message]:
    """
    Get ONLY messages belonging to a specific topic.
    
    Uses get_chat_history with reply_to_message_id filter.
    
    Args:
        client: Pyrogram Client
        chat_id: Channel/supergroup ID
        topic_id: Topic/thread ID
        limit: Maximum messages to retrieve
        min_id: Optional minimum message ID filter
        max_id: Optional maximum message ID filter
    
    Returns:
        List of Messages in the topic
    
    Usage:
        # Get last 100 messages from topic
        messages = await get_all_topic_messages(client, chat_id, topic_id=5)
        
        # Get topic messages with ID >= 100
        messages = await get_all_topic_messages(client, chat_id, topic_id=5, min_id=100)
    """
    TopicExtractor, TopicExtractorConfig = _load_topic_extractor()
    try:
        if not (TopicExtractor and TopicExtractorConfig):
            raise RuntimeError("TopicExtractor unavailable")
        extractor = TopicExtractor(
            client,
            TopicExtractorConfig(
                chat_id=chat_id,
                topic_id=topic_id,
                fetch_batch_size=min(limit, 100),
                incremental_from_cache=min_id is not None,
            ),
        )
        messages = await extractor.extract_all()
        if min_id is not None:
            messages = [msg for msg in messages if msg.id >= min_id]
        if max_id is not None:
            messages = [msg for msg in messages if msg.id <= max_id]
        return messages[:limit]
    except Exception as e:
        logger.warning(f"TopicExtractor get_all failed without history scan: {e}")

    return []


async def get_topic_message_boundaries(
    client: Client,
    chat_id: Union[int, str],
    topic_id: int,
    limit: int = 1
) -> Tuple[Optional[int], Optional[int]]:
    """
    Get actual first and last message IDs for a topic.
    
    CRITICAL: Topic message IDs are NOT guaranteed to be sequential.
    Must use actual retrieval, not math.
    
    Args:
        client: Pyrogram Client
        chat_id: Forum/supergroup ID
        topic_id: Topic/thread ID
        limit: How many boundary messages to check
    
    Returns:
        (first_msg_id, last_msg_id) or (None, None) if unavailable
    """
    TopicExtractor, TopicExtractorConfig = _load_topic_extractor()
    try:
        if not (TopicExtractor and TopicExtractorConfig):
            raise RuntimeError("TopicExtractor unavailable")
        extractor = TopicExtractor(
            client,
            TopicExtractorConfig(
                chat_id=chat_id,
                topic_id=topic_id,
                fetch_batch_size=max(1, min(limit, 100)),
            ),
        )
        messages = await extractor.extract_all()
        if not messages:
            return None, None
        return messages[0].id, messages[-1].id
    except Exception as e:
        logger.warning(f"Failed to get topic boundaries: {e}")
        return None, None


async def safe_get_topic_messages(
    client: Client,
    chat_id: Union[int, str],
    topic_id: int,
    message_ids: List[int] = None,
    limit: int = 100
) -> MessageRangeResult:
    """
    Safely retrieve messages from a topic.
    
    Args:
        client: Pyrogram Client
        chat_id: Forum/supergroup ID
        topic_id: Topic/thread ID
        message_ids: Specific IDs to fetch (or None for recent)
        limit: Max messages if no specific IDs
    
    Returns:
        MessageRangeResult
    """
    result = MessageRangeResult(
        messages=[],
        skipped_ids=[],
        errors={},
        total_requested=len(message_ids) if message_ids else limit
    )
    
    if message_ids:
        # Fetch specific messages and filter by topic
        range_result = await safe_get_messages_range(
            client, chat_id, message_ids, validate_channel=True
        )
        
        for msg in range_result.messages:
            if is_message_in_topic(msg, topic_id):
                result.messages.append(msg)
            else:
                result.skipped_ids.append(msg.id)
                result.errors[msg.id] = ErrorDiagnostic(
                    category=ErrorCategory.NOT_FOUND,
                    error_type="TopicMismatch",
                    message=f"Message {msg.id} not in topic {topic_id}",
                    recoverable=False,
                    context={'chat_id': chat_id, 'topic_id': topic_id, 'msg_id': msg.id}
                )
        
        # Include skips from range fetch
        result.skipped_ids.extend(range_result.skipped_ids)
        result.errors.update(range_result.errors)
        
    else:
        TopicExtractor, TopicExtractorConfig = _load_topic_extractor()
        try:
            if not (TopicExtractor and TopicExtractorConfig):
                raise RuntimeError("TopicExtractor unavailable")
            extractor = TopicExtractor(
                client,
                TopicExtractorConfig(
                    chat_id=chat_id,
                    topic_id=topic_id,
                    fetch_batch_size=min(limit, 100),
                ),
            )
            result.messages = await extractor.extract_all()
            result.total_requested = len(result.messages)
            return result
        except Exception as e:
            diagnostic = classify_error(e, {'chat_id': chat_id, 'topic_id': topic_id})
            logger.warning(f"Topic fetch error: {diagnostic}")
    
    return result


# ============================================================================
# ENTITY VALIDATION GUARD
# ============================================================================

def validate_entities_before_send(
    text: str,
    entities: List[MessageEntity],
    drop_invalid: bool = True
) -> Tuple[str, Optional[List[MessageEntity]], List[str]]:
    """
    Validate entity bounds BEFORE send/edit operations.
    
    CRITICAL: Call this before ANY send_message or edit_message with entities.
    Prevents ENTITY_BOUNDS_INVALID errors.
    
    Args:
        text: Message text
        entities: MessageEntity list
        drop_invalid: If True, drop invalid entities; if False, return errors
    
    Returns:
        (text, valid_entities or None, list_of_warnings)
    """
    warnings = []
    
    if not text:
        if entities:
            warnings.append("Dropping all entities: empty text")
        return text, None, warnings
    
    if not entities:
        return text, None, warnings
    
    # Calculate UTF-16 length (Telegram's unit)
    try:
        text_utf16 = len(text.encode("utf-16-le", errors="surrogatepass")) // 2
    except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
        text_utf16 = sum(2 if ord(c) > 0xFFFF else 1 for c in text)
    
    valid_entities = []
    
    for i, entity in enumerate(entities):
        offset = getattr(entity, 'offset', -1)
        length = getattr(entity, 'length', 0)
        
        # Validation checks
        if offset < 0:
            warnings.append(f"Entity[{i}]: negative offset {offset}")
            if not drop_invalid:
                return text, None, warnings
            continue
        
        if length <= 0:
            warnings.append(f"Entity[{i}]: invalid length {length}")
            if not drop_invalid:
                return text, None, warnings
            continue
        
        end = offset + length
        if end > text_utf16:
            warnings.append(
                f"Entity[{i}]: bounds exceed text (offset={offset}, "
                f"length={length}, text_len={text_utf16})"
            )
            if not drop_invalid:
                return text, None, warnings
            continue
        
        # Entity is valid
        valid_entities.append(entity)
    
    if len(valid_entities) < len(entities):
        dropped = len(entities) - len(valid_entities)
        logger.warning(f"Dropped {dropped} invalid entities out of {len(entities)}")
    
    return text, valid_entities if valid_entities else None, warnings


# ============================================================================
# TIMEOUT-AWARE OPERATION GUARDS
# ============================================================================

async def guarded_edit_message(
    client: Client,
    chat_id: Union[int, str],
    message_id: int,
    text: str = None,
    entities: List[MessageEntity] = None,
    timeout: float = 30.0,
    max_retries: int = 2,
    **kwargs
) -> Tuple[Optional[Message], Optional[ErrorDiagnostic]]:
    """
    Edit message with timeout protection and entity validation.
    
    Args:
        client: Pyrogram Client
        chat_id: Target chat
        message_id: Message to edit
        text: New text (optional)
        entities: Text entities (optional)
        timeout: Operation timeout in seconds
        max_retries: Max retry attempts
        **kwargs: Additional edit_message arguments
    
    Returns:
        (edited_message or None, error_diagnostic or None)
    """
    # Validate entities if provided
    if text and entities:
        text, entities, warnings = validate_entities_before_send(text, entities)
        for w in warnings:
            logger.debug(f"Entity validation: {w}")
    
    context = {'chat_id': chat_id, 'message_id': message_id}
    
    for attempt in range(max_retries + 1):
        try:
            edit_kwargs = {'chat_id': chat_id, 'message_id': message_id, **kwargs}
            if text:
                edit_kwargs['text'] = text
            if entities:
                edit_kwargs['entities'] = entities
            
            result = await asyncio.wait_for(
                client.edit_message_text(**edit_kwargs),
                timeout=timeout
            )
            return result, None
            
        except MessageNotModified:
            # Not an error - message already has this content
            return None, None
            
        except asyncio.TimeoutError:
            if attempt < max_retries:
                backoff = 2 ** attempt
                logger.warning(f"Edit timeout, retry in {backoff}s")
                await asyncio.sleep(backoff)
                continue
            
            diagnostic = ErrorDiagnostic(
                category=ErrorCategory.NETWORK,
                error_type="Timeout",
                message=f"Edit timed out after {timeout}s",
                recoverable=False,
                context=context
            )
            return None, diagnostic
            
        except FloodWait as e:
            wait_time = getattr(e, 'value', getattr(e, 'x', 30))
            if wait_time <= 60:
                await asyncio.sleep(wait_time)
                continue
            
            diagnostic = classify_error(e, context)
            return None, diagnostic
            
        except Exception as e:
            # Try without entities on entity error
            if entities and 'ENTITY' in str(e).upper():
                logger.warning("Entity error, retrying without entities")
                entities = None
                continue
            
            diagnostic = classify_error(e, context)
            return None, diagnostic
    
    return None, ErrorDiagnostic(
        category=ErrorCategory.UNKNOWN,
        error_type="MaxRetries",
        message="Max retries exceeded",
        recoverable=False,
        context=context
    )


async def guarded_delete_messages(
    client: Client,
    chat_id: Union[int, str],
    message_ids: List[int],
    timeout: float = 30.0,
    max_retries: int = 2
) -> Tuple[int, List[ErrorDiagnostic]]:
    """
    Delete messages with timeout protection.
    
    Args:
        client: Pyrogram Client
        chat_id: Target chat
        message_ids: Messages to delete
        timeout: Operation timeout
        max_retries: Max retry attempts
    
    Returns:
        (deleted_count, list_of_errors)
    """
    errors = []
    deleted = 0
    
    # Process in batches of 100 (Telegram limit)
    for i in range(0, len(message_ids), 100):
        batch = message_ids[i:i + 100]
        
        for attempt in range(max_retries + 1):
            try:
                await asyncio.wait_for(
                    client.delete_messages(chat_id, batch),
                    timeout=timeout
                )
                deleted += len(batch)
                break
                
            except asyncio.TimeoutError:
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                
                errors.append(ErrorDiagnostic(
                    category=ErrorCategory.NETWORK,
                    error_type="Timeout",
                    message=f"Delete timed out for batch starting at index {i}",
                    recoverable=False,
                    context={'chat_id': chat_id, 'batch_start': i}
                ))
                
            except FloodWait as e:
                wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                if wait_time <= 60:
                    await asyncio.sleep(wait_time)
                    continue
                
                errors.append(classify_error(e, {'chat_id': chat_id, 'batch_start': i}))
                break
                
            except Exception as e:
                errors.append(classify_error(e, {'chat_id': chat_id, 'batch_start': i}))
                break
    
    return deleted, errors


# ============================================================================
# CONVENIENCE WRAPPER FOR EXISTING HANDLERS
# ============================================================================

async def guarded_copy_operation(
    client: Client,
    source_chat: Union[int, str],
    message_ids: List[int],
    operation_name: str = "copy"
) -> Tuple[bool, MessageRangeResult, Optional[str]]:
    """
    High-level guard wrapper for copy operations.
    
    Combines channel validation + safe range fetch + error classification.
    
    Args:
        client: Pyrogram Client (user session)
        source_chat: Source channel/chat
        message_ids: Messages to copy
        operation_name: Name for logging
    
    Returns:
        (success, message_range_result, user_error_message or None)
    """
    # Step 1: Validate channel
    validation = await validate_channel_access(client, source_chat)
    if not validation.valid:
        user_msg = validation.diagnostic.user_message if validation.diagnostic else "Cannot access channel"
        return False, MessageRangeResult([], message_ids, {}, len(message_ids)), user_msg
    
    # Step 2: Fetch messages safely
    result = await safe_get_messages_range(
        client,
        validation.chat_id or source_chat,
        message_ids,
        validate_channel=False  # Already validated
    )
    
    # Step 3: Determine success
    if result.success_count == 0:
        # All failed
        if result.errors:
            first_error = list(result.errors.values())[0]
            return False, result, first_error.user_message
        return False, result, "No messages could be retrieved"
    
    # Partial or full success
    if result.skip_count > 0:
        logger.info(
            f"{operation_name}: {result.success_count} messages retrieved, "
            f"{result.skip_count} skipped"
        )
    
    return True, result, None
