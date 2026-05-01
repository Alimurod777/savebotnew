"""
core/api_validator.py - Pre-API Validation Layer

RULE: NEVER call Telegram API without validation first.

This module provides:
1. Text length validation (UTF-16)
2. Entity bounds validation
3. Caption/media compatibility validation
4. Session state validation

All validators return (is_valid, error_context_or_none).
"""

import logging
from typing import Optional, List, Tuple, Any
from dataclasses import dataclass

from pyrogram import Client
from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType

logger = logging.getLogger(__name__)

# Telegram limits (UTF-16 code units)
MESSAGE_LIMIT = 4096
CAPTION_LIMIT = 1024
ALBUM_CAPTION_LIMIT = 1024

# Entity types that require URL
URL_ENTITY_TYPES = {
    MessageEntityType.TEXT_LINK,
}


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    details: dict = None
    
    @staticmethod
    def ok() -> 'ValidationResult':
        return ValidationResult(is_valid=True)
    
    @staticmethod
    def fail(error_type: str, message: str, **details) -> 'ValidationResult':
        return ValidationResult(
            is_valid=False,
            error_type=error_type,
            error_message=message,
            details=details or {}
        )


def utf16_length(text: str) -> int:
    """Calculate UTF-16 code unit length (Telegram's native unit)."""
    if not text:
        return 0
    try:
        return len(text.encode('utf-16-le', errors='surrogatepass')) // 2
    except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
        count = 0
        for ch in text:
            count += 2 if ord(ch) > 0xFFFF else 1
        return count


# ==================== TEXT VALIDATION ====================

def validate_text_length(
    text: str,
    limit: int = MESSAGE_LIMIT,
    context: str = "text"
) -> ValidationResult:
    """
    Validate text length against Telegram limit.
    
    Args:
        text: Text to validate
        limit: Maximum UTF-16 units (default: 4096)
        context: Description for error message
    
    Returns:
        ValidationResult
    """
    if not text:
        return ValidationResult.ok()
    
    length = utf16_length(text)
    
    if length <= limit:
        return ValidationResult.ok()
    
    return ValidationResult.fail(
        error_type="TEXT_TOO_LONG",
        message=f"{context} length {length} exceeds limit {limit}",
        length=length,
        limit=limit,
        overflow=length - limit
    )


def validate_caption_length(
    caption: str,
    limit: int = CAPTION_LIMIT
) -> ValidationResult:
    """Validate caption length for media."""
    return validate_text_length(caption, limit, "caption")


def validate_message_length(
    text: str,
    limit: int = MESSAGE_LIMIT
) -> ValidationResult:
    """Validate message text length."""
    return validate_text_length(text, limit, "message")


# ==================== ENTITY VALIDATION ====================

def validate_single_entity(
    text: str,
    entity: MessageEntity,
    text_utf16_len: int = None
) -> ValidationResult:
    """
    Validate a single entity's bounds.
    
    Rules:
    1. offset >= 0
    2. length > 0
    3. offset + length <= text_length
    4. TEXT_LINK must have url
    """
    if text_utf16_len is None:
        text_utf16_len = utf16_length(text)
    
    offset = getattr(entity, 'offset', 0)
    length = getattr(entity, 'length', 0)
    end = offset + length
    entity_type = getattr(entity, 'type', None)
    
    # Rule 1: offset >= 0
    if offset < 0:
        return ValidationResult.fail(
            error_type="ENTITY_OFFSET_NEGATIVE",
            message=f"Entity offset {offset} is negative",
            offset=offset,
            length=length
        )
    
    # Rule 2: length > 0
    if length <= 0:
        return ValidationResult.fail(
            error_type="ENTITY_LENGTH_INVALID",
            message=f"Entity length {length} is invalid",
            offset=offset,
            length=length
        )
    
    # Rule 3: bounds within text
    if end > text_utf16_len:
        return ValidationResult.fail(
            error_type="ENTITY_BOUNDS_INVALID",
            message=f"Entity end {end} exceeds text length {text_utf16_len}",
            offset=offset,
            length=length,
            end=end,
            text_length=text_utf16_len
        )
    
    # Rule 4: URL entities must have url
    if entity_type in URL_ENTITY_TYPES:
        url = getattr(entity, 'url', None)
        if not url:
            return ValidationResult.fail(
                error_type="ENTITY_URL_MISSING",
                message=f"TEXT_LINK entity missing url",
                offset=offset,
                length=length
            )
    
    return ValidationResult.ok()


def validate_entities(
    text: str,
    entities: List[MessageEntity]
) -> ValidationResult:
    """
    Validate all entities against text.
    
    Returns first invalid entity's error, or ok if all valid.
    """
    if not entities:
        return ValidationResult.ok()
    
    if not text:
        return ValidationResult.fail(
            error_type="ENTITIES_WITHOUT_TEXT",
            message="Entities provided but text is empty",
            entity_count=len(entities)
        )
    
    text_utf16_len = utf16_length(text)
    invalid_entities = []
    
    for i, entity in enumerate(entities):
        result = validate_single_entity(text, entity, text_utf16_len)
        if not result.is_valid:
            invalid_entities.append({
                'index': i,
                'offset': getattr(entity, 'offset', 0),
                'length': getattr(entity, 'length', 0),
                'error': result.error_type
            })
    
    if not invalid_entities:
        return ValidationResult.ok()
    
    return ValidationResult.fail(
        error_type="ENTITIES_INVALID",
        message=f"{len(invalid_entities)} invalid entities found",
        invalid_count=len(invalid_entities),
        total_count=len(entities),
        invalid_entities=invalid_entities[:5],  # First 5
        text_length=text_utf16_len
    )


def sanitize_entities(
    text: str,
    entities: List[MessageEntity]
) -> List[MessageEntity]:
    """
    Remove invalid entities, keeping only valid ones.
    
    Use this before sending to prevent ENTITY_BOUNDS_INVALID.
    """
    if not entities or not text:
        return []
    
    text_utf16_len = utf16_length(text)
    valid = []
    
    for entity in entities:
        result = validate_single_entity(text, entity, text_utf16_len)
        if result.is_valid:
            valid.append(entity)
        else:
            logger.debug(f"Dropping invalid entity: {result.error_message}")
    
    return valid


# ==================== CAPTION VALIDATION ====================

def validate_caption_with_entities(
    caption: str,
    entities: List[MessageEntity],
    limit: int = CAPTION_LIMIT
) -> ValidationResult:
    """
    Full caption validation including length and entities.
    
    Call this BEFORE send_photo, send_video, etc.
    """
    # Length check
    result = validate_caption_length(caption, limit)
    if not result.is_valid:
        return result
    
    # Entity bounds check
    if entities:
        result = validate_entities(caption, entities)
        if not result.is_valid:
            return result
    
    return ValidationResult.ok()


def validate_message_with_entities(
    text: str,
    entities: List[MessageEntity],
    limit: int = MESSAGE_LIMIT
) -> ValidationResult:
    """
    Full message validation including length and entities.
    
    Call this BEFORE send_message.
    """
    # Length check
    result = validate_message_length(text, limit)
    if not result.is_valid:
        return result
    
    # Entity bounds check
    if entities:
        result = validate_entities(text, entities)
        if not result.is_valid:
            return result
    
    return ValidationResult.ok()


# ==================== SESSION VALIDATION ====================

async def validate_session(
    client: Client,
    timeout: float = 10.0
) -> ValidationResult:
    """
    Validate that session is connected and functional.
    
    Call this before any operation if session state is uncertain.
    """
    import asyncio
    
    if not client:
        return ValidationResult.fail(
            error_type="SESSION_NULL",
            message="Client is None"
        )
    
    if not client.is_connected:
        return ValidationResult.fail(
            error_type="SESSION_DISCONNECTED",
            message="Client is not connected"
        )
    
    # Try a simple API call
    try:
        await asyncio.wait_for(client.get_me(), timeout=timeout)
        return ValidationResult.ok()
    except asyncio.TimeoutError:
        return ValidationResult.fail(
            error_type="SESSION_TIMEOUT",
            message="Session validation timed out"
        )
    except Exception as e:
        return ValidationResult.fail(
            error_type="SESSION_INVALID",
            message=f"Session validation failed: {e}"
        )


# ==================== MEDIA VALIDATION ====================

def validate_media_caption(
    caption: str,
    caption_entities: List[MessageEntity] = None
) -> Tuple[str, List[MessageEntity], ValidationResult]:
    """
    Validate and fix media caption.
    
    Returns:
        (fixed_caption, fixed_entities, validation_result)
        
    If validation fails, returns truncated/sanitized version.
    """
    if not caption:
        return None, None, ValidationResult.ok()
    
    # Sanitize entities first
    safe_entities = sanitize_entities(caption, caption_entities) if caption_entities else None
    
    # Check length
    result = validate_caption_length(caption)
    
    if result.is_valid:
        return caption, safe_entities, result
    
    # Caption too long - truncate
    # Find safe truncation point (don't break in middle of entity)
    limit = CAPTION_LIMIT - 3  # Leave room for "..."
    
    # Find last valid position
    if safe_entities:
        # Don't truncate inside an entity
        for entity in sorted(safe_entities, key=lambda e: e.offset + e.length, reverse=True):
            end = entity.offset + entity.length
            if end <= limit:
                break
            limit = entity.offset - 1
    
    # Truncate
    truncated = caption[:limit].rstrip() + "..." if limit > 0 else caption[:CAPTION_LIMIT-3] + "..."
    
    # Re-sanitize entities for truncated text
    if safe_entities:
        safe_entities = sanitize_entities(truncated, safe_entities)
    
    return truncated, safe_entities, ValidationResult.fail(
        error_type="CAPTION_TRUNCATED",
        message=f"Caption truncated from {len(caption)} to {len(truncated)}",
        original_length=len(caption),
        truncated_length=len(truncated)
    )


# ==================== COMBINED VALIDATORS ====================

def prepare_message_for_send(
    text: str,
    entities: List[MessageEntity] = None
) -> Tuple[str, List[MessageEntity], ValidationResult]:
    """
    Prepare message for sending with full validation.
    
    Returns:
        (text, sanitized_entities, validation_result)
    """
    if not text:
        return "", None, ValidationResult.ok()
    
    # Sanitize entities
    safe_entities = sanitize_entities(text, entities) if entities else None
    
    # Validate
    result = validate_message_with_entities(text, safe_entities)
    
    return text, safe_entities, result


def prepare_caption_for_send(
    caption: str,
    entities: List[MessageEntity] = None
) -> Tuple[str, List[MessageEntity], ValidationResult]:
    """
    Prepare caption for sending with full validation.
    
    Automatically truncates and sanitizes if needed.
    
    Returns:
        (caption, sanitized_entities, validation_result)
    """
    return validate_media_caption(caption, entities)


# ==================== LOGGING HELPERS ====================

def log_validation_failure(result: ValidationResult, context: str = "") -> None:
    """Log a validation failure with details."""
    if result.is_valid:
        return
    
    logger.warning(
        f"Validation failed{f' ({context})' if context else ''}: "
        f"{result.error_type} - {result.error_message}"
    )
    
    if result.details:
        logger.debug(f"Details: {result.details}")
