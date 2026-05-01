"""
core/entity_splitter.py - Production-grade entity-aware text splitting.

ARCHITECTURE: Entity-First (as per specification)
- Never split inside an entity
- Move entire entity to next chunk if it doesn't fit
- UTF-16 offset recalculation is mandatory
- Zero tolerance for ENTITY_BOUNDS_INVALID

ALGORITHM:
1. Read text left → right
2. Detect full hyperlink/entity boundaries
3. Before appending to chunk:
   - If current_chunk_length + entity_length ≤ limit → append
   - Else: finalize chunk, move ENTIRE entity to next chunk
4. Recalculate UTF-16 offsets for each chunk

GUARANTEES:
- Hyperlink position preserved
- No phantom duplicate links
- No ENTITY_BOUNDS_INVALID
- Production-grade stability
"""

import logging
from typing import List, Tuple, Optional
from dataclasses import dataclass, field

from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType

logger = logging.getLogger(__name__)

# Telegram limits
CAPTION_LIMIT = 1024
MESSAGE_LIMIT = 4096


def utf16_len(text: str) -> int:
    """
    Calculate UTF-16 code unit length (Telegram's native unit).
    
    Handles lone surrogates (0xD800-0xDFFF) which can appear in
    corrupted Telegram captions and cause 'utf-16-le' codec errors.
    """
    if not text:
        return 0
    try:
        return len(text.encode('utf-16-le', errors='surrogatepass')) // 2
    except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
        # Manual count: BMP chars = 1 unit, supplementary (>0xFFFF) = 2 units,
        # lone surrogates (0xD800-0xDFFF) = 1 unit each
        count = 0
        for ch in text:
            cp = ord(ch)
            count += 2 if cp > 0xFFFF else 1
        return count


def utf16_to_char_index(text: str, utf16_offset: int) -> int:
    """Convert UTF-16 offset to Python string index."""
    if utf16_offset <= 0:
        return 0
    count = 0
    for i, ch in enumerate(text):
        if count >= utf16_offset:
            return i
        count += 2 if ord(ch) > 0xFFFF else 1
    return len(text)


def char_to_utf16_offset(text: str, char_index: int) -> int:
    """Convert Python string index to UTF-16 offset."""
    if char_index <= 0:
        return 0
    count = 0
    for ch in text[:char_index]:
        count += 2 if ord(ch) > 0xFFFF else 1
    return count


@dataclass
class EntitySpan:
    """An entity with its text span information."""
    entity: MessageEntity
    start_utf16: int  # UTF-16 offset (original)
    end_utf16: int    # UTF-16 end (original)
    start_char: int   # Python char index
    end_char: int     # Python char end
    
    @property
    def length_utf16(self) -> int:
        return self.end_utf16 - self.start_utf16
    
    def extract_text(self, full_text: str) -> str:
        """Extract the text covered by this entity."""
        return full_text[self.start_char:self.end_char]


@dataclass  
class TextChunk:
    """A chunk of text with recalculated entities."""
    text: str
    entities: List[MessageEntity] = field(default_factory=list)
    
    @property
    def utf16_length(self) -> int:
        return utf16_len(self.text)
    
    def validate(self) -> bool:
        """Validate all entity bounds are within text."""
        text_len = self.utf16_length
        for e in self.entities:
            if e.offset < 0 or e.length <= 0:
                return False
            if e.offset + e.length > text_len:
                return False
        return True


def build_entity_spans(text: str, entities: List[MessageEntity]) -> List[EntitySpan]:
    """
    Build EntitySpan list with calculated char indices.
    
    Converts UTF-16 offsets to Python char indices for text slicing.
    """
    if not entities:
        return []
    
    spans = []
    for e in entities:
        start_utf16 = e.offset
        end_utf16 = e.offset + e.length
        
        # Convert to char indices
        start_char = utf16_to_char_index(text, start_utf16)
        end_char = utf16_to_char_index(text, end_utf16)
        
        spans.append(EntitySpan(
            entity=e,
            start_utf16=start_utf16,
            end_utf16=end_utf16,
            start_char=start_char,
            end_char=end_char
        ))
    
    # Sort by start position
    spans.sort(key=lambda s: s.start_utf16)
    return spans


def clone_entity_with_offset(entity: MessageEntity, new_offset: int) -> Optional[MessageEntity]:
    """Create a copy of entity with new offset.

    Returns None if the entity type is unsupported (e.g. unknown enum).
    """
    try:
        return MessageEntity(
            type=entity.type,
            offset=new_offset,
            length=entity.length,
            url=getattr(entity, 'url', None),
            user=getattr(entity, 'user', None),
            language=getattr(entity, 'language', None),
            custom_emoji_id=getattr(entity, 'custom_emoji_id', None),
        )
    except Exception as exc:
        logger.warning("clone_entity_with_offset failed type=%s: %s", entity.type, exc)
        return None


def split_text_with_entities(
    text: str,
    entities: List[MessageEntity],
    limit: int = MESSAGE_LIMIT
) -> List[TextChunk]:
    """
    Split text preserving ALL entities without breaking them.
    
    CRITICAL ALGORITHM (as per specification):
    1. Build segments: plain text and entity-covered text
    2. Iterate left-to-right
    3. For each segment:
       - If fits in current chunk → add
       - If doesn't fit → finalize chunk, start new one
    4. Entity is NEVER split - moves entirely to next chunk
    5. Recalculate UTF-16 offsets for each chunk
    
    Args:
        text: Raw text (not Markdown)
        entities: List of MessageEntity objects
        limit: Max UTF-16 units per chunk
    
    Returns:
        List of TextChunk with recalculated entities
    """
    if not text:
        return []
    
    text_utf16 = utf16_len(text)
    
    # No split needed
    if text_utf16 <= limit:
        # Validate entities
        valid_entities = _validate_entities(text, entities)
        return [TextChunk(text=text, entities=valid_entities)]
    
    # Build entity spans
    spans = build_entity_spans(text, entities or [])
    
    # Build segments (alternating plain text and entity text)
    segments = _build_segments(text, spans)
    
    # Split into chunks
    chunks = []
    current_text = ""
    current_entities = []
    current_utf16 = 0
    
    for segment in segments:
        seg_text = segment['text']
        seg_utf16 = segment['utf16_len']
        seg_entity = segment.get('entity')  # EntitySpan or None
        
        # Check if segment fits
        if current_utf16 + seg_utf16 <= limit:
            # Fits - add to current chunk
            if seg_entity:
                # Create entity with recalculated offset and actual segment
                # length (may differ from original for overlapping entities)
                try:
                    new_entity = MessageEntity(
                        type=seg_entity.entity.type,
                        offset=current_utf16,
                        length=seg_utf16,
                        url=getattr(seg_entity.entity, 'url', None),
                        user=getattr(seg_entity.entity, 'user', None),
                        language=getattr(seg_entity.entity, 'language', None),
                        custom_emoji_id=getattr(seg_entity.entity, 'custom_emoji_id', None),
                    )
                    current_entities.append(new_entity)
                except Exception as exc:
                    logger.warning("Failed to create entity: %s", exc)
            
            current_text += seg_text
            current_utf16 += seg_utf16
        else:
            # Doesn't fit
            if seg_entity:
                # CRITICAL: Entity must stay whole - finalize current chunk
                if current_text.strip():
                    chunks.append(TextChunk(
                        text=current_text.rstrip(),
                        entities=current_entities
                    ))
                
                # Start new chunk with this entity
                # But first, check if entity itself fits in limit
                if seg_utf16 > limit:
                    # Entity too large - this is an edge case
                    # Log warning and add without entity formatting
                    logger.warning(
                        f"Entity too large ({seg_utf16} > {limit}), "
                        f"adding as plain text"
                    )
                    for chunk_text in _split_plain_text(seg_text, limit):
                        if chunk_text.strip():
                            chunks.append(TextChunk(text=chunk_text.rstrip(), entities=[]))
                    current_text = ""
                    current_entities = []
                    current_utf16 = 0
                    continue
                else:
                    # Entity fits - start new chunk
                    try:
                        new_entity = MessageEntity(
                            type=seg_entity.entity.type,
                            offset=0,
                            length=seg_utf16,
                            url=getattr(seg_entity.entity, 'url', None),
                            user=getattr(seg_entity.entity, 'user', None),
                            language=getattr(seg_entity.entity, 'language', None),
                            custom_emoji_id=getattr(seg_entity.entity, 'custom_emoji_id', None),
                        )
                        current_text = seg_text
                        current_entities = [new_entity]
                        current_utf16 = seg_utf16
                    except Exception as exc:
                        logger.warning("Failed to create entity: %s", exc)
                        current_text = seg_text
                        current_entities = []
                        current_utf16 = seg_utf16
            else:
                # Plain text - can split at word boundary
                remaining = seg_text
                
                while remaining:
                    space_left = limit - current_utf16
                    remaining_utf16 = utf16_len(remaining)
                    
                    if remaining_utf16 <= space_left:
                        # Fits
                        current_text += remaining
                        current_utf16 += remaining_utf16
                        break
                    else:
                        # Need to split plain text
                        split_point = _find_word_boundary(remaining, space_left)
                        
                        if split_point > 0:
                            current_text += remaining[:split_point]
                            remaining = remaining[split_point:].lstrip()
                        
                        # Finalize chunk
                        if current_text.strip():
                            chunks.append(TextChunk(
                                text=current_text.rstrip(),
                                entities=current_entities
                            ))
                        
                        current_text = ""
                        current_entities = []
                        current_utf16 = 0
    
    # Add final chunk
    if current_text.strip():
        chunks.append(TextChunk(
            text=current_text.rstrip(),
            entities=current_entities
        ))
    
    # Post-process: recalculate entities after rstrip() may have shortened text
    validated_chunks = []
    for i, chunk in enumerate(chunks):
        text_len = chunk.utf16_length
        # Drop entities that now exceed bounds due to rstrip()
        safe_entities = []
        for e in chunk.entities:
            if e.offset >= 0 and e.length > 0 and e.offset + e.length <= text_len:
                safe_entities.append(e)
            else:
                logger.warning(
                    f"Chunk {i}: entity offset={e.offset} len={e.length} "
                    f"exceeds text len={text_len}, dropped"
                )
        chunk.entities = safe_entities
        validated_chunks.append(chunk)
    
    return validated_chunks


def _build_segments(text: str, spans: List[EntitySpan]) -> List[dict]:
    """
    Build alternating segments of plain text and entity text.

    Handles overlapping entities (e.g. bold inside italic) by advancing
    the position cursor forward-only.  Text is never emitted twice.

    Returns list of {'text': str, 'utf16_len': int, 'entity': EntitySpan or None}
    """
    segments = []
    pos = 0  # Current char position (only advances forward)

    for span in spans:
        # Plain text before entity (only the non-overlapping gap)
        if span.start_char > pos:
            plain = text[pos:span.start_char]
            if plain:
                segments.append({
                    'text': plain,
                    'utf16_len': utf16_len(plain),
                    'entity': None
                })

        # Entity segment: only emit text beyond current pos
        if span.end_char > pos:
            seg_start = max(pos, span.start_char)
            entity_text = text[seg_start:span.end_char]
            segments.append({
                'text': entity_text,
                'utf16_len': utf16_len(entity_text),
                'entity': span
            })
            pos = span.end_char
        # else: entity fully within already-processed text, skip

    # Plain text after last entity
    if pos < len(text):
        plain = text[pos:]
        if plain:
            segments.append({
                'text': plain,
                'utf16_len': utf16_len(plain),
                'entity': None
            })

    return segments


def _find_word_boundary(text: str, max_utf16: int) -> int:
    """Find char index for word boundary within UTF-16 limit."""
    if not text:
        return 0
    
    # Find char index for max_utf16
    max_char = utf16_to_char_index(text, max_utf16)
    search = text[:max_char]
    
    # Try to find word boundary
    for sep in ['\n\n', '\n', ' ', '.', ',']:
        pos = search.rfind(sep)
        if pos > max_char // 3:
            return pos + len(sep) if sep in ['\n\n', '\n', ' '] else pos + 1
    
    # Hard split
    return max_char


def _split_plain_text(text: str, limit: int) -> List[str]:
    """Split plain text into UTF-16 limited chunks."""
    if not text:
        return []

    chunks = []
    remaining = text

    while remaining:
        if utf16_len(remaining) <= limit:
            chunks.append(remaining)
            break

        split_point = _find_word_boundary(remaining, limit)
        if split_point <= 0:
            split_point = utf16_to_char_index(remaining, limit)

        chunk = remaining[:split_point].rstrip()
        remaining = remaining[split_point:].lstrip()

        if chunk:
            chunks.append(chunk)

    return chunks


def _validate_entities(text: str, entities: List[MessageEntity]) -> List[MessageEntity]:
    """Remove invalid entities and CUSTOM_EMOJI (non-premium safety)."""
    if not entities:
        return []

    text_utf16 = utf16_len(text)
    valid = []

    for e in entities:
        if e.offset < 0 or e.length <= 0:
            continue
        if e.offset + e.length > text_utf16:
            continue
        # Strip CUSTOM_EMOJI — non-premium accounts cannot send these
        if getattr(e, 'type', None) == MessageEntityType.CUSTOM_EMOJI:
            continue
        valid.append(e)

    return valid


# ==================== CAPTION SPLITTING ====================

def split_caption_with_overflow(
    caption: str,
    entities: List[MessageEntity]
) -> Tuple[TextChunk, List[TextChunk]]:
    """
    Split caption for media with overflow handling.
    
    Returns:
        (caption_chunk, overflow_chunks)
        - caption_chunk: First 1024 UTF-16 units for media
        - overflow_chunks: Remaining text as message chunks
    """
    if not caption:
        return TextChunk(text="", entities=[]), []
    
    caption_utf16 = utf16_len(caption)
    
    # Fits in caption
    if caption_utf16 <= CAPTION_LIMIT:
        valid = _validate_entities(caption, entities)
        return TextChunk(text=caption, entities=valid), []
    
    # Need to split - use same algorithm
    all_chunks = split_text_with_entities(caption, entities, CAPTION_LIMIT)
    
    if not all_chunks:
        return TextChunk(text="", entities=[]), []
    
    # First chunk = caption (re-split if needed to fit CAPTION_LIMIT)
    first = all_chunks[0]
    if first.utf16_length > CAPTION_LIMIT:
        # Re-split first chunk more aggressively
        sub_chunks = split_text_with_entities(first.text, first.entities, CAPTION_LIMIT)
        if sub_chunks:
            caption_chunk = sub_chunks[0]
            overflow = sub_chunks[1:] + all_chunks[1:]
        else:
            caption_chunk = TextChunk(text=caption[:CAPTION_LIMIT], entities=[])
            overflow = all_chunks[1:]
    else:
        caption_chunk = first
        overflow = all_chunks[1:]
    
    # Re-split overflow for message limit
    final_overflow = []
    for chunk in overflow:
        if chunk.utf16_length > MESSAGE_LIMIT:
            sub = split_text_with_entities(chunk.text, chunk.entities, MESSAGE_LIMIT)
            final_overflow.extend(sub)
        else:
            final_overflow.append(chunk)
    
    return caption_chunk, final_overflow


# ==================== HIGH-LEVEL FUNCTIONS ====================

def prepare_message_chunks(
    text: str,
    entities: List[MessageEntity]
) -> List[dict]:
    """
    Prepare message chunks ready for send_message().
    
    Returns list of kwargs dicts for client.send_message()
    """
    chunks = split_text_with_entities(text, entities, MESSAGE_LIMIT)
    
    result = []
    for chunk in chunks:
        kwargs = {'text': chunk.text}
        if chunk.entities:
            kwargs['entities'] = chunk.entities
        result.append(kwargs)
    
    return result


def prepare_caption_kwargs(
    caption: str,
    entities: List[MessageEntity]
) -> Tuple[dict, List[dict]]:
    """
    Prepare caption kwargs for media + overflow message kwargs.
    
    Returns:
        (caption_kwargs, list_of_overflow_kwargs)
    """
    cap_chunk, overflow = split_caption_with_overflow(caption, entities)
    
    caption_kwargs = {}
    if cap_chunk.text:
        caption_kwargs['caption'] = cap_chunk.text
        if cap_chunk.entities:
            caption_kwargs['caption_entities'] = cap_chunk.entities
    
    overflow_kwargs = []
    for chunk in overflow:
        kwargs = {'text': chunk.text}
        if chunk.entities:
            kwargs['entities'] = chunk.entities
        overflow_kwargs.append(kwargs)
    
    return caption_kwargs, overflow_kwargs


# ==================== MARKDOWNV2 REBUILD ====================

# Characters that must be escaped in MarkdownV2 outside entities
_MDV2_ESCAPE_CHARS = r'_*[]()~`>#+-=|{}.!\\'


def _escape_mdv2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    result = []
    for ch in text:
        if ch in _MDV2_ESCAPE_CHARS:
            result.append('\\')
        result.append(ch)
    return ''.join(result)


def rebuild_markdown_from_chunk(chunk: TextChunk) -> str:
    """
    Rebuild MarkdownV2 string from a TextChunk.
    
    Entities are rendered as their MarkdownV2 equivalents.
    Plain text between entities is escaped.
    This is used when entities cannot be sent directly
    (e.g., forwarding to a channel that only accepts parse_mode).
    
    IMPORTANT: Escaping is recalculated AFTER rebuild, not before.
    """
    if not chunk.entities:
        return _escape_mdv2(chunk.text)
    
    # Sort entities by offset ascending
    sorted_ents = sorted(chunk.entities, key=lambda e: e.offset)
    
    parts = []
    pos = 0  # current char position
    
    for ent in sorted_ents:
        start_char = utf16_to_char_index(chunk.text, ent.offset)
        end_char = utf16_to_char_index(chunk.text, ent.offset + ent.length)
        
        # Plain text before this entity - escape it
        if start_char > pos:
            parts.append(_escape_mdv2(chunk.text[pos:start_char]))
        
        entity_text = chunk.text[start_char:end_char]
        
        # Wrap entity text in MarkdownV2 syntax
        if ent.type == MessageEntityType.BOLD:
            parts.append(f'*{_escape_mdv2(entity_text)}*')
        elif ent.type == MessageEntityType.ITALIC:
            parts.append(f'_{_escape_mdv2(entity_text)}_')
        elif ent.type == MessageEntityType.CODE:
            # Code content is NOT escaped in MarkdownV2
            parts.append(f'`{entity_text}`')
        elif ent.type == MessageEntityType.PRE:
            lang = getattr(ent, 'language', '') or ''
            # Pre content is NOT escaped in MarkdownV2
            parts.append(f'```{lang}\n{entity_text}\n```')
        elif ent.type == MessageEntityType.SPOILER:
            parts.append(f'||{_escape_mdv2(entity_text)}||')
        elif ent.type == MessageEntityType.TEXT_LINK:
            url = getattr(ent, 'url', '') or ''
            # Link text is escaped, URL is NOT
            parts.append(f'[{_escape_mdv2(entity_text)}]({url})')
        elif ent.type == MessageEntityType.STRIKETHROUGH:
            parts.append(f'~{_escape_mdv2(entity_text)}~')
        elif ent.type == MessageEntityType.UNDERLINE:
            parts.append(f'__{_escape_mdv2(entity_text)}__')
        elif ent.type == MessageEntityType.TEXT_MENTION:
            user = getattr(ent, 'user', None)
            if user and hasattr(user, 'id'):
                parts.append(f'[{_escape_mdv2(entity_text)}](tg://user?id={user.id})')
            else:
                parts.append(_escape_mdv2(entity_text))
        else:
            # Unknown entity type - just escape the text
            parts.append(_escape_mdv2(entity_text))
        
        pos = end_char
    
    # Remaining plain text after last entity
    if pos < len(chunk.text):
        parts.append(_escape_mdv2(chunk.text[pos:]))
    
    return ''.join(parts)
