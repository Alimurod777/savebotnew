"""
core/entity_validator.py - Entity validation to prevent ENTITY_BOUNDS_INVALID

CRITICAL: Every MessageEntity MUST be validated before sending to Telegram.
Invalid entities cause ENTITY_BOUNDS_INVALID error and message send failure.

Validation Rules (MTProto):
1. offset >= 0
2. length > 0  
3. offset + length <= utf16_length(text)
4. No overlapping entities with same type at same position
5. URL entities must have valid url field

ENTITY-AWARE SPLITTING:
When text exceeds Telegram limits, splitting MUST:
1. Calculate UTF-16 offsets correctly (Telegram uses UTF-16 code units)
2. Recalculate entity offsets for each chunk
3. Handle entities that span split boundaries (move or clone)
4. Never produce invalid entities

Usage:
    from core.entity_validator import validate_entities, sanitize_entities
    
    # Validate and get safe entities
    safe_entities = sanitize_entities(text, entities)
    
    # Or validate with error details
    is_valid, errors = validate_entities(text, entities)
    
    # Entity-aware splitting for long messages
    chunks = split_with_entities(text, entities, max_utf16=4096)
"""

import logging
from typing import List, Tuple, Optional, Set
from dataclasses import dataclass
from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType

logger = logging.getLogger(__name__)


def utf16_length(text: str) -> int:
    """Calculate UTF-16 code unit length (Telegram's offset/length unit)."""
    if not text:
        return 0
    try:
        # surrogatepass handles lone surrogates from corrupted captions
        return len(text.encode("utf-16-le", errors="surrogatepass")) // 2
    except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
        count = 0
        for ch in text:
            count += 2 if ord(ch) > 0xFFFF else 1
        return count


def validate_single_entity(
    text: str,
    entity: MessageEntity,
    text_utf16_len: int = None
) -> Tuple[bool, Optional[str]]:
    """
    Validate a single MessageEntity against text.
    
    Args:
        text: The text the entity applies to
        entity: MessageEntity to validate
        text_utf16_len: Pre-calculated UTF-16 length (optimization)
    
    Returns:
        (is_valid, error_message or None)
    """
    if text_utf16_len is None:
        text_utf16_len = utf16_length(text)
    
    # Rule 1: offset must be non-negative
    offset = getattr(entity, 'offset', None)
    if offset is None or offset < 0:
        return False, f"Invalid offset: {offset}"
    
    # Rule 2: length must be positive
    length = getattr(entity, 'length', None)
    if length is None or length <= 0:
        return False, f"Invalid length: {length}"
    
    # Rule 3: offset + length must not exceed text length
    end = offset + length
    if end > text_utf16_len:
        return False, f"Entity exceeds text bounds: offset={offset}, length={length}, text_len={text_utf16_len}"
    
    # Rule 4: URL entities must have url field
    entity_type = getattr(entity, 'type', None)
    if entity_type == MessageEntityType.TEXT_LINK:
        url = getattr(entity, 'url', None)
        if not url:
            return False, "TEXT_LINK entity missing url field"
    
    return True, None


def validate_entities(
    text: str,
    entities: List[MessageEntity]
) -> Tuple[bool, List[str]]:
    """
    Validate all entities against text.
    
    Args:
        text: The text entities apply to
        entities: List of MessageEntity objects
    
    Returns:
        (all_valid, list_of_error_messages)
    """
    if not entities:
        return True, []
    
    if not text:
        return False, ["Cannot have entities with empty text"]
    
    text_utf16_len = utf16_length(text)
    errors = []
    
    for i, entity in enumerate(entities):
        is_valid, error = validate_single_entity(text, entity, text_utf16_len)
        if not is_valid:
            errors.append(f"Entity[{i}]: {error}")
    
    return len(errors) == 0, errors


def sanitize_entities(
    text: str,
    entities: List[MessageEntity],
    log_dropped: bool = True
) -> List[MessageEntity]:
    """
    Remove invalid entities, keeping only valid ones.
    
    CRITICAL: Use this before ANY send operation with entities.
    
    Args:
        text: The text entities apply to
        entities: List of MessageEntity objects
        log_dropped: Whether to log dropped entities
    
    Returns:
        List of valid entities (may be empty)
    """
    if not entities:
        return []
    
    if not text:
        if log_dropped and entities:
            logger.warning(f"Dropping {len(entities)} entities: empty text")
        return []
    
    text_utf16_len = utf16_length(text)
    valid_entities = []
    dropped_count = 0
    
    for entity in entities:
        # Strip CUSTOM_EMOJI — non-premium accounts cannot send these
        if getattr(entity, 'type', None) == MessageEntityType.CUSTOM_EMOJI:
            dropped_count += 1
            if log_dropped:
                logger.debug("Dropped CUSTOM_EMOJI entity (non-premium safe)")
            continue

        is_valid, error = validate_single_entity(text, entity, text_utf16_len)
        if is_valid:
            valid_entities.append(entity)
        else:
            dropped_count += 1
            if log_dropped:
                logger.debug(f"Dropped invalid entity: {error}")
    
    if dropped_count > 0 and log_dropped:
        logger.warning(f"Sanitized entities: dropped {dropped_count}/{len(entities)} invalid")
    
    return valid_entities


def clamp_entities(
    text: str,
    entities: List[MessageEntity]
) -> List[MessageEntity]:
    """
    Clamp entity bounds to fit within text, instead of dropping.
    
    Use when you want to preserve as much formatting as possible.
    Entities that can't be salvaged are dropped.
    
    Args:
        text: The text entities apply to
        entities: List of MessageEntity objects
    
    Returns:
        List of clamped/valid entities
    """
    if not entities or not text:
        return []
    
    text_utf16_len = utf16_length(text)
    result = []
    
    for entity in entities:
        offset = getattr(entity, 'offset', 0)
        length = getattr(entity, 'length', 0)
        
        # Skip if completely outside text
        if offset >= text_utf16_len or offset < 0:
            continue
        
        # Clamp offset to valid range
        if offset < 0:
            length += offset  # Reduce length by negative offset
            offset = 0
        
        # Skip if length is invalid after clamping
        if length <= 0:
            continue
        
        # Clamp end to text boundary
        end = offset + length
        if end > text_utf16_len:
            length = text_utf16_len - offset
        
        # Skip if nothing left
        if length <= 0:
            continue
        
        # Create new entity with clamped bounds
        try:
            new_entity = MessageEntity(
                type=entity.type,
                offset=offset,
                length=length,
                url=getattr(entity, 'url', None),
                user=getattr(entity, 'user', None),
                language=getattr(entity, 'language', None),
                custom_emoji_id=getattr(entity, 'custom_emoji_id', None),
            )
            result.append(new_entity)
        except Exception as e:
            logger.debug(f"Failed to clamp entity: {e}")
    
    return result


def deduplicate_entities(entities: List[MessageEntity]) -> List[MessageEntity]:
    """
    Remove duplicate entities at same position with same type.
    
    Args:
        entities: List of MessageEntity objects
    
    Returns:
        Deduplicated list
    """
    if not entities:
        return []
    
    seen: Set[Tuple[int, int, str]] = set()
    result = []
    
    for entity in entities:
        key = (
            getattr(entity, 'offset', 0),
            getattr(entity, 'length', 0),
            str(getattr(entity, 'type', ''))
        )
        if key not in seen:
            seen.add(key)
            result.append(entity)
    
    return result


def prepare_entities_for_send(
    text: str,
    entities: List[MessageEntity],
    strict: bool = False
) -> Optional[List[MessageEntity]]:
    """
    Prepare entities for sending - the main entry point for validation.
    
    CRITICAL: Call this before EVERY send operation with entities.
    
    Args:
        text: Message text
        entities: Raw entities list
        strict: If True, return None on any invalid entity
                If False, sanitize and return valid subset
    
    Returns:
        Valid entities list, or None if strict mode and validation failed
    """
    if not entities:
        return None  # Explicitly return None for empty, not empty list
    
    if not text:
        logger.warning("prepare_entities_for_send: empty text, dropping all entities")
        return None
    
    # Log debug info
    text_utf16 = utf16_length(text)
    logger.debug(f"prepare_entities_for_send: text_len={len(text)}, utf16_len={text_utf16}, entities={len(entities)}")
    
    # Deduplicate first
    entities = deduplicate_entities(entities)
    
    if strict:
        is_valid, errors = validate_entities(text, entities)
        if not is_valid:
            for error in errors:
                logger.error(f"Entity validation failed (strict): {error}")
            return None
        return entities
    else:
        # Sanitize mode - keep valid entities
        return sanitize_entities(text, entities) or None


def safe_build_entity(
    entity_type: MessageEntityType,
    offset: int,
    length: int,
    text_length: int,
    url: str = None,
    user=None,
    language: str = None
) -> Optional[MessageEntity]:
    """
    Safely build a MessageEntity with bounds checking.
    
    Returns None if entity would be invalid.
    """
    # Validate bounds
    if offset < 0 or length <= 0:
        return None
    if offset + length > text_length:
        return None
    
    # Validate URL for TEXT_LINK
    if entity_type == MessageEntityType.TEXT_LINK and not url:
        return None
    
    try:
        return MessageEntity(
            type=entity_type,
            offset=offset,
            length=length,
            url=url,
            user=user,
            language=language,
        )
    except Exception as e:
        logger.debug(f"Failed to build entity: {e}")
        return None


# ==================== CAPTION-SPECIFIC VALIDATION ====================

CAPTION_LIMIT = 1024
MESSAGE_LIMIT = 4096


def validate_caption_entities(
    caption: str,
    entities: List[MessageEntity]
) -> Tuple[bool, List[str]]:
    """
    Validate entities specifically for captions (1024 char limit).
    """
    if not caption:
        return True, []
    
    # First check caption length
    if utf16_length(caption) > CAPTION_LIMIT:
        return False, [f"Caption exceeds {CAPTION_LIMIT} UTF-16 units"]
    
    return validate_entities(caption, entities)


def prepare_caption_for_send(
    caption: str,
    entities: List[MessageEntity]
) -> Tuple[Optional[str], Optional[List[MessageEntity]]]:
    """
    Prepare caption and entities for sending.
    
    Truncates if needed and adjusts entities accordingly.
    
    Returns:
        (caption, entities) - entities may be None
    """
    if not caption:
        return None, None
    
    text_len = utf16_length(caption)
    
    # If within limit, just sanitize entities
    if text_len <= CAPTION_LIMIT:
        safe_entities = prepare_entities_for_send(caption, entities)
        return caption, safe_entities
    
    # Need to truncate - this is a fallback, proper splitting should be done elsewhere
    logger.warning(f"Caption truncated from {text_len} to {CAPTION_LIMIT} UTF-16 units")
    
    # Truncate by UTF-16 units
    truncated = ""
    count = 0
    for ch in caption:
        units = 2 if ord(ch) > 0xFFFF else 1
        if count + units > CAPTION_LIMIT - 3:  # Leave room for "..."
            break
        truncated += ch
        count += units
    
    truncated += "..."
    
    # Clamp entities to truncated text
    clamped = clamp_entities(truncated, entities) if entities else None
    safe_entities = prepare_entities_for_send(truncated, clamped) if clamped else None
    
    return truncated, safe_entities


# ==================== ENTITY-AWARE SPLITTING ====================

def utf16_index_to_char_index(text: str, utf16_offset: int) -> int:
    """Convert UTF-16 offset to Python character index."""
    if utf16_offset <= 0:
        return 0
    count = 0
    for idx, ch in enumerate(text):
        units = 2 if ord(ch) > 0xFFFF else 1
        if count + units > utf16_offset:
            return idx
        count += units
        if count == utf16_offset:
            return idx + 1
    return len(text)


def char_index_to_utf16(text: str, char_index: int) -> int:
    """Convert Python character index to UTF-16 offset."""
    if char_index <= 0:
        return 0
    if char_index >= len(text):
        return utf16_length(text)
    count = 0
    for idx, ch in enumerate(text[:char_index]):
        count += 2 if ord(ch) > 0xFFFF else 1
    return count


@dataclass
class SplitChunk:
    """A chunk of text with its entities."""
    text: str
    entities: List[MessageEntity]
    
    @property
    def utf16_len(self) -> int:
        return utf16_length(self.text)


def find_safe_split_point(
    text: str,
    entities: List[MessageEntity],
    max_utf16: int,
    min_split: int = None
) -> int:
    """
    Find a safe UTF-16 offset to split text without breaking entities.
    
    Priority:
    1. Split at paragraph break (double newline) not inside entity
    2. Split at line break not inside entity  
    3. Split at word boundary not inside entity
    4. Split just before an entity if needed
    5. Hard split at max_utf16 (last resort)
    
    Returns UTF-16 offset for split point.
    """
    if min_split is None:
        min_split = max_utf16 // 3
    
    text_utf16 = utf16_length(text)
    if text_utf16 <= max_utf16:
        return text_utf16
    
    # Build set of "forbidden" ranges where entities exist
    entity_ranges = []
    for e in entities or []:
        start = getattr(e, 'offset', 0)
        end = start + getattr(e, 'length', 0)
        if start >= 0 and end > start:
            entity_ranges.append((start, end))
    
    def is_inside_entity(utf16_pos: int) -> bool:
        """Check if position falls inside any entity."""
        for start, end in entity_ranges:
            if start < utf16_pos < end:
                return True
        return False
    
    def is_just_before_entity(utf16_pos: int) -> bool:
        """Check if position is right before an entity start."""
        for start, _ in entity_ranges:
            if utf16_pos == start:
                return True
        return False
    
    # Search backwards from max_utf16 for a good split point
    search_limit_char = utf16_index_to_char_index(text, max_utf16)
    search_text = text[:search_limit_char]
    
    # Try paragraph break
    para_pos = search_text.rfind('\n\n')
    if para_pos > 0:
        utf16_pos = char_index_to_utf16(text, para_pos + 2)
        if utf16_pos >= min_split and not is_inside_entity(utf16_pos):
            return utf16_pos
    
    # Try line break
    line_pos = search_text.rfind('\n')
    if line_pos > 0:
        utf16_pos = char_index_to_utf16(text, line_pos + 1)
        if utf16_pos >= min_split and not is_inside_entity(utf16_pos):
            return utf16_pos
    
    # Try word boundary (space)
    space_pos = search_text.rfind(' ')
    if space_pos > 0:
        utf16_pos = char_index_to_utf16(text, space_pos + 1)
        if utf16_pos >= min_split and not is_inside_entity(utf16_pos):
            return utf16_pos
    
    # Try splitting just before an entity
    for start, _ in sorted(entity_ranges, reverse=True):
        if min_split <= start <= max_utf16:
            return start
    
    # Hard split - find largest position not inside entity
    for test_pos in range(max_utf16, min_split, -1):
        if not is_inside_entity(test_pos):
            return test_pos
    
    # Absolute fallback
    return max_utf16


def split_entities_at(
    text: str,
    entities: List[MessageEntity],
    utf16_split: int
) -> Tuple[Tuple[str, List[MessageEntity]], Tuple[str, List[MessageEntity]]]:
    """
    Split text and entities at a UTF-16 offset.
    
    Entities are distributed:
    - Entities entirely before split -> first chunk
    - Entities entirely after split -> second chunk (offset adjusted)
    - Entities spanning split -> dropped (should be prevented by find_safe_split_point)
    
    Returns:
        ((text1, entities1), (text2, entities2))
    """
    # Convert UTF-16 split to character index
    char_split = utf16_index_to_char_index(text, utf16_split)
    
    text1 = text[:char_split]
    text2 = text[char_split:]
    
    entities1 = []
    entities2 = []
    
    for entity in (entities or []):
        offset = getattr(entity, 'offset', 0)
        length = getattr(entity, 'length', 0)
        end = offset + length
        
        if end <= utf16_split:
            # Entity entirely in first chunk
            entities1.append(entity)
        elif offset >= utf16_split:
            # Entity entirely in second chunk - adjust offset
            new_offset = offset - utf16_split
            try:
                new_entity = MessageEntity(
                    type=entity.type,
                    offset=new_offset,
                    length=length,
                    url=getattr(entity, 'url', None),
                    user=getattr(entity, 'user', None),
                    language=getattr(entity, 'language', None),
                    custom_emoji_id=getattr(entity, 'custom_emoji_id', None),
                )
                entities2.append(new_entity)
            except Exception as e:
                logger.debug(f"Failed to create adjusted entity: {e}")
        else:
            # Entity spans split - this should be rare if find_safe_split_point works
            logger.warning(f"Entity spans split point, dropping: offset={offset}, length={length}")
    
    return (text1, entities1), (text2, entities2)


def split_with_entities(
    text: str,
    entities: List[MessageEntity],
    max_utf16: int = MESSAGE_LIMIT
) -> List[SplitChunk]:
    """
    Split text and entities into chunks respecting Telegram limits.
    
    CRITICAL: This is the main entry point for entity-aware splitting.
    
    Features:
    - Respects UTF-16 length limits
    - Recalculates entity offsets for each chunk
    - Avoids splitting inside entities
    - Validates all output entities
    
    Args:
        text: Text to split
        entities: Associated entities
        max_utf16: Maximum UTF-16 units per chunk (4096 for messages, 1024 for captions)
    
    Returns:
        List of SplitChunk objects ready for sending
    """
    if not text:
        return []
    
    # Sanitize entities first
    safe_entities = sanitize_entities(text, entities) if entities else []
    
    text_utf16 = utf16_length(text)
    
    # No split needed
    if text_utf16 <= max_utf16:
        return [SplitChunk(text=text, entities=safe_entities)]
    
    chunks = []
    remaining_text = text
    remaining_entities = safe_entities
    
    while remaining_text:
        remaining_utf16 = utf16_length(remaining_text)
        
        if remaining_utf16 <= max_utf16:
            # Last chunk - validate entities before adding
            final_entities = prepare_entities_for_send(remaining_text, remaining_entities)
            chunks.append(SplitChunk(text=remaining_text, entities=final_entities or []))
            break
        
        # Find safe split point
        split_point = find_safe_split_point(remaining_text, remaining_entities, max_utf16)
        
        # Split
        (chunk_text, chunk_entities), (next_text, next_entities) = split_entities_at(
            remaining_text, remaining_entities, split_point
        )
        
        # Validate chunk entities
        validated = prepare_entities_for_send(chunk_text, chunk_entities)
        chunks.append(SplitChunk(text=chunk_text.rstrip(), entities=validated or []))
        
        remaining_text = next_text.lstrip()
        remaining_entities = next_entities
        
        # Recalculate entity offsets after lstrip if any whitespace was removed
        if next_text != remaining_text:
            stripped_chars = len(next_text) - len(remaining_text)
            stripped_utf16 = char_index_to_utf16(next_text, stripped_chars)
            remaining_entities = []
            for e in next_entities:
                new_offset = getattr(e, 'offset', 0) - stripped_utf16
                if new_offset >= 0:
                    try:
                        new_entity = MessageEntity(
                            type=e.type,
                            offset=new_offset,
                            length=e.length,
                            url=getattr(e, 'url', None),
                            user=getattr(e, 'user', None),
                            language=getattr(e, 'language', None),
                            custom_emoji_id=getattr(e, 'custom_emoji_id', None),
                        )
                        remaining_entities.append(new_entity)
                    except Exception:
                        pass
    
    logger.debug(f"Split text into {len(chunks)} chunks")
    return chunks


def split_caption_with_entities(
    caption: str,
    entities: List[MessageEntity]
) -> Tuple[SplitChunk, Optional[List[SplitChunk]]]:
    """
    Split caption for media with overflow handling.
    
    Returns:
        (caption_chunk, overflow_chunks or None)
        - caption_chunk: First 1024 UTF-16 units for media caption
        - overflow_chunks: Remaining text as message chunks (or None if no overflow)
    """
    if not caption:
        return SplitChunk(text="", entities=[]), None
    
    text_utf16 = utf16_length(caption)
    
    if text_utf16 <= CAPTION_LIMIT:
        safe_entities = prepare_entities_for_send(caption, entities)
        return SplitChunk(text=caption, entities=safe_entities or []), None
    
    # Need to split - find safe point for caption
    split_point = find_safe_split_point(caption, entities, CAPTION_LIMIT)
    
    (cap_text, cap_entities), (overflow_text, overflow_entities) = split_entities_at(
        caption, entities, split_point
    )
    
    # Validate caption entities
    safe_cap_entities = prepare_entities_for_send(cap_text, cap_entities)
    caption_chunk = SplitChunk(text=cap_text.rstrip(), entities=safe_cap_entities or [])
    
    # Split overflow into message chunks
    if overflow_text:
        # Calculate UTF-16 units stripped by lstrip()
        stripped_text = overflow_text.lstrip()
        if stripped_text and len(stripped_text) < len(overflow_text):
            # Need to adjust entity offsets for the stripped prefix
            prefix = overflow_text[:len(overflow_text) - len(stripped_text)]
            prefix_utf16 = utf16_length(prefix)
            
            # Shift all entity offsets back by the stripped prefix length
            adjusted_entities = []
            for e in overflow_entities:
                new_offset = getattr(e, 'offset', 0) - prefix_utf16
                if new_offset < 0:
                    continue  # Entity was in stripped whitespace
                try:
                    adjusted = MessageEntity(
                        type=e.type,
                        offset=new_offset,
                        length=e.length,
                        url=getattr(e, 'url', None),
                        user=getattr(e, 'user', None),
                        language=getattr(e, 'language', None),
                        custom_emoji_id=getattr(e, 'custom_emoji_id', None),
                    )
                    adjusted_entities.append(adjusted)
                except Exception:
                    pass
            overflow_entities = adjusted_entities
            overflow_text = stripped_text
        elif stripped_text:
            overflow_text = stripped_text
        
        overflow_chunks = split_with_entities(overflow_text, overflow_entities, MESSAGE_LIMIT)
    else:
        overflow_chunks = None
    
    return caption_chunk, overflow_chunks



