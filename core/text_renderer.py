"""
core/text_renderer.py - Segment-based text rendering with native MessageEntity.

ARCHITECTURE:
- Text is represented as logical segments (plain text, hyperlinks, formatting)
- Offsets are calculated ONLY during final rendering
- NO Markdown/HTML parsing - uses MessageEntity directly

CRITICAL: This replaces ALL Markdown-based link generation.

Usage:
    renderer = TextRenderer()
    renderer.text("Click ")
    renderer.link("here", "https://example.com")
    renderer.text(" to continue")
    
    # Render for sending
    text, entities = renderer.render()
    await client.send_message(chat_id, text, entities=entities)
    
    # Or with auto-splitting
    chunks = renderer.render_chunks(max_length=4096)
    for text, entities in chunks:
        await client.send_message(chat_id, text, entities=entities)
"""

import logging
from typing import List, Tuple, Optional, Union, Any
from dataclasses import dataclass, field
from enum import Enum
from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType

logger = logging.getLogger(__name__)

# Telegram limits
MAX_MESSAGE_LENGTH = 4096
MAX_CAPTION_LENGTH = 1024


class SegmentType(Enum):
    """Types of text segments."""
    PLAIN = "plain"
    LINK = "link"
    BOLD = "bold"
    ITALIC = "italic"
    CODE = "code"
    PRE = "pre"
    UNDERLINE = "underline"
    STRIKETHROUGH = "strikethrough"
    SPOILER = "spoiler"
    MENTION = "mention"  # tg://user?id=xxx
    NATIVE = "native"   # passthrough: keeps original MessageEntity as-is


@dataclass
class Segment:
    """A single text segment with optional formatting."""
    text: str
    type: SegmentType = SegmentType.PLAIN
    url: Optional[str] = None  # For LINK type
    user_id: Optional[int] = None  # For MENTION type
    language: Optional[str] = None  # For PRE type
    native_entity: Optional[Any] = None  # For NATIVE type: original MessageEntity

    @property
    def length(self) -> int:
        """UTF-16 code unit length — what Telegram counts for entity offset/length."""
        try:
            return len(self.text.encode("utf-16-le", errors="surrogatepass")) // 2
        except Exception:
            count = 0
            for ch in self.text:
                count += 2 if ord(ch) > 0xFFFF else 1
            return count

    @property
    def char_length(self) -> int:
        """Python character count — for string slicing."""
        return len(self.text)
    
    def to_entity(self, offset: int) -> Optional[MessageEntity]:
        """Convert segment to MessageEntity at given offset."""
        if self.type == SegmentType.PLAIN:
            return None

        # NATIVE: rebuild original entity at new offset with correct length
        if self.type == SegmentType.NATIVE and self.native_entity is not None:
            orig = self.native_entity
            kwargs = {
                'type': orig.type,
                'offset': offset,
                'length': self.length,
            }
            # Copy optional fields from original entity
            for field in ('url', 'user', 'language', 'custom_emoji_id'):
                val = getattr(orig, field, None)
                if val is not None:
                    kwargs[field] = val
            try:
                return MessageEntity(**kwargs)
            except Exception:
                return None

        entity_map = {
            SegmentType.LINK: MessageEntityType.TEXT_LINK,
            SegmentType.BOLD: MessageEntityType.BOLD,
            SegmentType.ITALIC: MessageEntityType.ITALIC,
            SegmentType.CODE: MessageEntityType.CODE,
            SegmentType.PRE: MessageEntityType.PRE,
            SegmentType.UNDERLINE: MessageEntityType.UNDERLINE,
            SegmentType.STRIKETHROUGH: MessageEntityType.STRIKETHROUGH,
            SegmentType.SPOILER: MessageEntityType.SPOILER,
            SegmentType.MENTION: MessageEntityType.TEXT_LINK,
        }

        entity_type = entity_map.get(self.type)
        if not entity_type:
            return None
        
        # Build entity kwargs
        kwargs = {
            'type': entity_type,
            'offset': offset,
            'length': self.length,
        }
        
        if self.type == SegmentType.LINK and self.url:
            kwargs['url'] = self.url
        elif self.type == SegmentType.MENTION and self.user_id:
            kwargs['url'] = f"tg://user?id={self.user_id}"
        elif self.type == SegmentType.PRE and self.language:
            kwargs['language'] = self.language
        
        return MessageEntity(**kwargs)


class TextRenderer:
    """
    Segment-based text renderer using native MessageEntity.
    
    NO MARKDOWN. NO HTML. Only MessageEntity objects.
    
    Example:
        renderer = TextRenderer()
        renderer.text("Welcome! ")
        renderer.bold("Important: ")
        renderer.link("Click here", "https://example.com")
        renderer.newline()
        renderer.text("Thank you!")
        
        text, entities = renderer.render()
        # text = "Welcome! Important: Click here\nThank you!"
        # entities = [MessageEntity(BOLD, 9, 11), MessageEntity(TEXT_LINK, 20, 10, url="...")]
    """
    
    def __init__(self):
        self._segments: List[Segment] = []
    
    def text(self, content: str) -> "TextRenderer":
        """Add plain text."""
        if content:
            self._segments.append(Segment(text=content, type=SegmentType.PLAIN))
        return self
    
    def link(self, text: str, url: str) -> "TextRenderer":
        """Add hyperlink. THIS IS THE ONLY WAY TO ADD LINKS."""
        if text and url:
            self._segments.append(Segment(text=text, type=SegmentType.LINK, url=url))
        return self
    
    def bold(self, text: str) -> "TextRenderer":
        """Add bold text."""
        if text:
            self._segments.append(Segment(text=text, type=SegmentType.BOLD))
        return self
    
    def italic(self, text: str) -> "TextRenderer":
        """Add italic text."""
        if text:
            self._segments.append(Segment(text=text, type=SegmentType.ITALIC))
        return self
    
    def code(self, text: str) -> "TextRenderer":
        """Add inline code."""
        if text:
            self._segments.append(Segment(text=text, type=SegmentType.CODE))
        return self
    
    def pre(self, text: str, language: str = None) -> "TextRenderer":
        """Add code block."""
        if text:
            self._segments.append(Segment(text=text, type=SegmentType.PRE, language=language))
        return self
    
    def underline(self, text: str) -> "TextRenderer":
        """Add underlined text."""
        if text:
            self._segments.append(Segment(text=text, type=SegmentType.UNDERLINE))
        return self
    
    def strikethrough(self, text: str) -> "TextRenderer":
        """Add strikethrough text."""
        if text:
            self._segments.append(Segment(text=text, type=SegmentType.STRIKETHROUGH))
        return self
    
    def spoiler(self, text: str) -> "TextRenderer":
        """Add spoiler text."""
        if text:
            self._segments.append(Segment(text=text, type=SegmentType.SPOILER))
        return self
    
    def mention(self, text: str, user_id: int) -> "TextRenderer":
        """Add user mention."""
        if text and user_id:
            self._segments.append(Segment(text=text, type=SegmentType.MENTION, user_id=user_id))
        return self
    
    def newline(self, count: int = 1) -> "TextRenderer":
        """Add newline(s)."""
        self._segments.append(Segment(text="\n" * count, type=SegmentType.PLAIN))
        return self
    
    def clear(self) -> "TextRenderer":
        """Clear all segments."""
        self._segments.clear()
        return self
    
    @property
    def total_length(self) -> int:
        """Get total text length."""
        return sum(s.length for s in self._segments)
    
    def render(self) -> Tuple[str, List[MessageEntity]]:
        """
        Render segments to text and entities.
        
        CRITICAL: Offsets are calculated HERE, not stored.
        
        Returns:
            (text, entities) tuple ready for send_message()
        """
        text_parts = []
        entities = []
        current_offset = 0
        
        for segment in self._segments:
            text_parts.append(segment.text)
            
            entity = segment.to_entity(current_offset)
            if entity:
                entities.append(entity)
            
            current_offset += segment.length
        
        return ''.join(text_parts), entities
    
    def render_chunks(self, max_length: int = MAX_MESSAGE_LENGTH) -> List[Tuple[str, List[MessageEntity]]]:
        """
        Render with auto-splitting for long messages.
        
        Handles:
        - Moving whole segments to next chunk when possible
        - Splitting large PLAIN text segments at word boundaries
        - Never splitting formatted segments (links, bold, etc.)
        
        Returns:
            List of (text, entities) tuples
        """
        if self.total_length <= max_length:
            return [self.render()]
        
        chunks = []
        current_segments = []
        current_length = 0
        
        for segment in self._segments:
            seg_len = segment.length
            
            # Case 1: Segment fits in current chunk
            if current_length + seg_len <= max_length:
                current_segments.append(segment)
                current_length += seg_len
                continue
            
            # Case 2: Segment doesn't fit - flush current chunk first
            if current_segments:
                chunk_renderer = TextRenderer()
                chunk_renderer._segments = current_segments
                chunks.append(chunk_renderer.render())
                current_segments = []
                current_length = 0
            
            # Case 3: Segment itself is larger than max_length
            if seg_len > max_length:
                # Only split PLAIN text segments
                if segment.type == SegmentType.PLAIN:
                    # Split at word boundaries using UTF-16 lengths
                    from core.utf16_utils import utf16_len as _u16len
                    text = segment.text
                    while _u16len(text) > max_length:
                        # Find split point at word boundary (char index, then check UTF-16)
                        # Binary search for char index where UTF-16 len <= max_length
                        lo, hi = 0, len(text)
                        while lo < hi:
                            mid = (lo + hi + 1) // 2
                            if _u16len(text[:mid]) <= max_length:
                                lo = mid
                            else:
                                hi = mid - 1
                        split_at = lo
                        # Try to split at word boundary
                        wb = text.rfind(' ', 0, split_at)
                        if wb > split_at // 2:
                            split_at = wb
                        else:
                            nb = text.rfind('\n', 0, split_at)
                            if nb > split_at // 2:
                                split_at = nb

                        # Create chunk with first part
                        part = text[:split_at].rstrip()
                        if part:
                            chunk_renderer = TextRenderer()
                            chunk_renderer._segments = [Segment(text=part, type=SegmentType.PLAIN)]
                            chunks.append(chunk_renderer.render())

                        text = text[split_at:].lstrip()

                    # Remaining text goes to current segment list
                    if text:
                        current_segments.append(Segment(text=text, type=SegmentType.PLAIN))
                        current_length = _u16len(text)
                else:
                    # Formatted segment too large - add as-is (rare edge case)
                    current_segments.append(segment)
                    current_length = seg_len
            else:
                # Segment fits on its own
                current_segments.append(segment)
                current_length = seg_len
        
        # Render remaining segments
        if current_segments:
            chunk_renderer = TextRenderer()
            chunk_renderer._segments = current_segments
            chunks.append(chunk_renderer.render())
        
        return chunks


def extract_to_renderer(text: str, entities: List[MessageEntity]) -> TextRenderer:
    """
    Convert existing text + entities to TextRenderer segments.

    Use this to migrate from old Pyrogram messages to new renderer.

    CRITICAL FIX: Handles overlapping entities correctly by skipping
    entities that overlap with already processed ranges.

    IMPORTANT: entity offset/length are UTF-16 code units (Telegram protocol).
    Text slicing must use utf16_slice / utf16_to_char_index, NOT plain indexing.

    Args:
        text: Original message text
        entities: Original message entities

    Returns:
        TextRenderer with equivalent segments
    """
    from core.utf16_utils import utf16_len, utf16_to_char_index, utf16_slice

    renderer = TextRenderer()

    if not text:
        return renderer

    # Convert Pyrogram Str subclass to plain str to avoid
    # Str.__getitem__ → remove_surrogates → utf-16-le decode errors.
    text = str(text)

    if not entities:
        renderer.text(text)
        return renderer

    text_utf16_len = utf16_len(text)

    # Sort entities by offset, then by length (longer first for same offset)
    sorted_entities = sorted(entities, key=lambda e: (e.offset, -e.length))

    current_utf16_pos = 0
    processed_ranges = []  # Track (start, end) UTF-16 of processed entities

    for entity in sorted_entities:
        offset = entity.offset  # UTF-16 offset
        length = entity.length  # UTF-16 length
        end = offset + length   # UTF-16 end

        # Bounds check using UTF-16 lengths
        if offset < 0 or end > text_utf16_len or length <= 0:
            continue

        # Skip if this entity overlaps with already processed entity
        is_overlapping = False
        for (p_start, p_end) in processed_ranges:
            if not (end <= p_start or offset >= p_end):
                is_overlapping = True
                break

        if is_overlapping:
            continue

        # Add plain text before this entity (using UTF-16 offsets for slicing)
        if offset > current_utf16_pos:
            renderer.text(utf16_slice(text, current_utf16_pos, offset))

        # Extract entity text using UTF-16 offsets
        entity_text = utf16_slice(text, offset, end)

        # Add segment based on entity type
        if entity.type == MessageEntityType.TEXT_LINK:
            url = getattr(entity, 'url', None)
            if url:
                renderer.link(entity_text, url)
            else:
                renderer.text(entity_text)
        elif entity.type == MessageEntityType.BOLD:
            renderer.bold(entity_text)
        elif entity.type == MessageEntityType.ITALIC:
            renderer.italic(entity_text)
        elif entity.type == MessageEntityType.CODE:
            renderer.code(entity_text)
        elif entity.type == MessageEntityType.PRE:
            lang = getattr(entity, 'language', None)
            renderer.pre(entity_text, lang)
        elif entity.type == MessageEntityType.UNDERLINE:
            renderer.underline(entity_text)
        elif entity.type == MessageEntityType.STRIKETHROUGH:
            renderer.strikethrough(entity_text)
        elif entity.type == MessageEntityType.SPOILER:
            renderer.spoiler(entity_text)
        elif entity.type == MessageEntityType.TEXT_MENTION:
            user = getattr(entity, 'user', None)
            if user and user.id:
                renderer.mention(entity_text, user.id)
            else:
                renderer.text(entity_text)
        else:
            # Passthrough: keep original entity (CUSTOM_EMOJI, BLOCKQUOTE, MENTION, URL, etc.)
            renderer._segments.append(
                Segment(text=entity_text, type=SegmentType.NATIVE, native_entity=entity)
            )

        processed_ranges.append((offset, end))
        current_utf16_pos = end

    # Add remaining text after last entity
    if current_utf16_pos < text_utf16_len:
        renderer.text(utf16_slice(text, current_utf16_pos, text_utf16_len))

    return renderer


async def send_rendered(
    client,
    chat_id: int,
    renderer: TextRenderer,
    reply_to_message_id: int = None,
    disable_web_page_preview: bool = True
) -> List:
    """
    Send rendered text with entities.
    
    Auto-splits if message exceeds 4096 characters.
    
    Args:
        client: Pyrogram Client
        chat_id: Target chat
        renderer: TextRenderer instance
        reply_to_message_id: Optional reply
        disable_web_page_preview: Disable link previews
    
    Returns:
        List of sent messages
    """
    from pyrogram.types import LinkPreviewOptions
    
    chunks = renderer.render_chunks(MAX_MESSAGE_LENGTH)
    sent = []
    
    for i, (text, entities) in enumerate(chunks):
        if not text:
            continue
        
        try:
            msg = await client.send_message(
                chat_id=chat_id,
                text=text,
                entities=entities if entities else None,
                reply_to_message_id=reply_to_message_id if i == 0 else None,
                link_preview_options=LinkPreviewOptions(is_disabled=disable_web_page_preview)
            )
            sent.append(msg)
        except Exception as e:
            logger.warning(f"Error sending chunk {i}: {e}")
    
    return sent


# Convenience function for simple text with links
def render_text_with_links(text: str, links: List[Tuple[str, str, str]]) -> Tuple[str, List[MessageEntity]]:
    """
    Quick helper to render text with multiple links.
    
    Args:
        text: Template text with {0}, {1}, etc. placeholders
        links: List of (placeholder_text, url) tuples
    
    Example:
        text, entities = render_text_with_links(
            "Visit {0} and {1} for more info",
            [("our site", "https://example.com"), ("docs", "https://docs.example.com")]
        )
    """
    renderer = TextRenderer()
    parts = text.split('{')
    
    if parts:
        renderer.text(parts[0])
    
    for i, part in enumerate(parts[1:]):
        if '}' in part:
            idx_str, rest = part.split('}', 1)
            try:
                idx = int(idx_str)
                if idx < len(links):
                    link_text, url = links[idx]
                    renderer.link(link_text, url)
            except ValueError:
                renderer.text('{' + part)
            renderer.text(rest)
        else:
            renderer.text('{' + part)
    
    return renderer.render()
