"""
core/smart_renderer.py - Dual-mode message rendering (Markdown vs Entity).

DESIGN PHILOSOPHY:
- Markdown for simple, short messages (readable, fast)
- Entity-based for complex cases (100% reliable)
- Automatic mode selection based on content analysis

RULES:
┌─────────────────────────────────────────────────────────────────┐
│  MODE A - MARKDOWN (Default, Safe Zone)                         │
│  Allowed when ALL conditions are true:                          │
│  ✓ Length ≤ 3500 characters                                     │
│  ✓ No splitting required                                        │
│  ✓ No entity offset recalculation                               │
│  ✓ Simple links only                                            │
│  ✓ No forward/edit/re-send with transformation                  │
├─────────────────────────────────────────────────────────────────┤
│  MODE B - ENTITY-SAFE (Mandatory Fallback)                      │
│  Required when ANY condition is true:                           │
│  ✓ Length > 3500 characters                                     │
│  ✓ Message must be split                                        │
│  ✓ Caption for media (video, document, album)                   │
│  ✓ Reconstructed from msg.text + msg.entities                   │
│  ✓ Forwarded/cloned/edited content                              │
│  ✓ Known edge cases                                             │
└─────────────────────────────────────────────────────────────────┘

CRITICAL: Markdown must NEVER be used after text splitting.
"""

import logging
import re
from typing import List, Tuple, Optional, Union
from dataclasses import dataclass, field
from enum import Enum, auto
from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType, ParseMode

logger = logging.getLogger(__name__)


def utf16_length(text: str) -> int:
    if not text:
        return 0
    try:
        return len(text.encode("utf-16-le", errors="surrogatepass")) // 2
    except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
        count = 0
        for ch in text:
            count += 2 if ord(ch) > 0xFFFF else 1
        return count


def utf16_offset_to_index(text: str, offset: int) -> int:
    if offset <= 0:
        return 0
    count = 0
    for idx, ch in enumerate(text):
        units = 2 if ord(ch) > 0xFFFF else 1
        if count + units > offset:
            return idx
        count += units
        if count == offset:
            return idx + 1
    return len(text)


def split_text_by_utf16(text: str, limit: int) -> List[str]:
    if not text:
        return []
    if limit <= 0:
        return [text]
    if utf16_length(text) <= limit:
        return [text]
    parts = []
    start = 0
    count = 0
    for idx, ch in enumerate(text):
        units = 2 if ord(ch) > 0xFFFF else 1
        if count + units > limit:
            if start < idx:
                parts.append(text[start:idx])
            start = idx
            count = 0
        count += units
    if start < len(text):
        parts.append(text[start:])
    return [part for part in parts if part]

# Thresholds
MARKDOWN_SAFE_LIMIT = 3500  # Below this, Markdown is safe
MESSAGE_LIMIT = 4096
CAPTION_LIMIT = 1024
CAPTION_MARKDOWN_LIMIT = 900  # Safe zone for captions with Markdown


class RenderMode(Enum):
    """Rendering mode selection."""
    MARKDOWN = auto()  # Use parse_mode=MARKDOWN
    ENTITY = auto()    # Use entities=[] parameter


@dataclass
class Segment:
    """A text segment with optional formatting."""
    text: str
    is_link: bool = False
    url: Optional[str] = None
    is_bold: bool = False
    is_italic: bool = False
    is_code: bool = False
    is_pre: bool = False
    is_underline: bool = False
    is_strikethrough: bool = False
    is_spoiler: bool = False
    user_id: Optional[int] = None  # For mentions
    language: Optional[str] = None  # For code blocks
    
    @property
    def length(self) -> int:
        return utf16_length(self.text)

    def is_plain(self) -> bool:
        return not (
            self.is_link
            or self.url
            or self.is_bold
            or self.is_italic
            or self.is_code
            or self.is_pre
            or self.is_underline
            or self.is_strikethrough
            or self.is_spoiler
            or self.user_id
            or self.language
        )

    def clone_with_text(self, text: str) -> "Segment":
        return Segment(
            text=text,
            is_link=self.is_link,
            url=self.url,
            is_bold=self.is_bold,
            is_italic=self.is_italic,
            is_code=self.is_code,
            is_pre=self.is_pre,
            is_underline=self.is_underline,
            is_strikethrough=self.is_strikethrough,
            is_spoiler=self.is_spoiler,
            user_id=self.user_id,
            language=self.language,
        )
    
    def to_markdown(self) -> str:
        """Convert segment to Markdown string."""
        result = self.text
        
        if self.is_link and self.url:
            # Escape special chars in link text
            escaped = result.replace('[', '\\[').replace(']', '\\]')
            result = f"[{escaped}]({self.url})"
        elif self.user_id:
            result = f"[{result}](tg://user?id={self.user_id})"
        elif self.is_bold:
            result = f"**{result}**"
        elif self.is_italic:
            result = f"__{result}__"
        elif self.is_code:
            result = f"`{result}`"
        elif self.is_pre:
            if self.language:
                result = f"```{self.language}\n{result}\n```"
            else:
                result = f"```\n{result}\n```"
        elif self.is_strikethrough:
            result = f"~~{result}~~"
        elif self.is_spoiler:
            result = f"||{result}||"
        # Underline not supported in Markdown
        
        return result
    
    def to_entity(self, offset: int) -> Optional[MessageEntity]:
        """Convert segment to MessageEntity at given offset."""
        if self.is_link and self.url:
            return MessageEntity(
                type=MessageEntityType.TEXT_LINK,
                offset=offset,
                length=self.length,
                url=self.url
            )
        elif self.user_id:
            return MessageEntity(
                type=MessageEntityType.TEXT_LINK,
                offset=offset,
                length=self.length,
                url=f"tg://user?id={self.user_id}"
            )
        elif self.is_bold:
            return MessageEntity(
                type=MessageEntityType.BOLD,
                offset=offset,
                length=self.length
            )
        elif self.is_italic:
            return MessageEntity(
                type=MessageEntityType.ITALIC,
                offset=offset,
                length=self.length
            )
        elif self.is_code:
            return MessageEntity(
                type=MessageEntityType.CODE,
                offset=offset,
                length=self.length
            )
        elif self.is_pre:
            return MessageEntity(
                type=MessageEntityType.PRE,
                offset=offset,
                length=self.length,
                language=self.language
            )
        elif self.is_underline:
            return MessageEntity(
                type=MessageEntityType.UNDERLINE,
                offset=offset,
                length=self.length
            )
        elif self.is_strikethrough:
            return MessageEntity(
                type=MessageEntityType.STRIKETHROUGH,
                offset=offset,
                length=self.length
            )
        elif self.is_spoiler:
            return MessageEntity(
                type=MessageEntityType.SPOILER,
                offset=offset,
                length=self.length
            )
        return None


class SmartRenderer:
    """
    Intelligent dual-mode message renderer.
    
    Automatically chooses between Markdown and Entity-based rendering
    based on content analysis.
    
    Usage:
        renderer = SmartRenderer()
        renderer.text("Hello ")
        renderer.link("World", "https://example.com")
        
        # Auto-detect best mode
        result = renderer.render()
        await client.send_message(chat_id, **result)
        
        # Or force entity mode
        result = renderer.render(force_entity=True)
    """
    
    def __init__(self):
        self._segments: List[Segment] = []
        self._force_entity: bool = False
        self._is_caption: bool = False
        self._is_album: bool = False
        self._is_reconstructed: bool = False
    
    # ==================== SEGMENT BUILDERS ====================
    
    def text(self, content: str) -> "SmartRenderer":
        """Add plain text."""
        if content:
            self._segments.append(Segment(text=content))
        return self
    
    def link(self, text: str, url: str) -> "SmartRenderer":
        """Add hyperlink."""
        if text and url:
            self._segments.append(Segment(text=text, is_link=True, url=url))
        return self
    
    def bold(self, text: str) -> "SmartRenderer":
        """Add bold text."""
        if text:
            self._segments.append(Segment(text=text, is_bold=True))
        return self
    
    def italic(self, text: str) -> "SmartRenderer":
        """Add italic text."""
        if text:
            self._segments.append(Segment(text=text, is_italic=True))
        return self
    
    def code(self, text: str) -> "SmartRenderer":
        """Add inline code."""
        if text:
            self._segments.append(Segment(text=text, is_code=True))
        return self
    
    def pre(self, text: str, language: str = None) -> "SmartRenderer":
        """Add code block."""
        if text:
            self._segments.append(Segment(text=text, is_pre=True, language=language))
        return self
    
    def mention(self, text: str, user_id: int) -> "SmartRenderer":
        """Add user mention."""
        if text and user_id:
            self._segments.append(Segment(text=text, user_id=user_id))
        return self
    
    def underline(self, text: str) -> "SmartRenderer":
        """Add underlined text."""
        if text:
            self._segments.append(Segment(text=text, is_underline=True))
        return self
    
    def strikethrough(self, text: str) -> "SmartRenderer":
        """Add strikethrough text."""
        if text:
            self._segments.append(Segment(text=text, is_strikethrough=True))
        return self
    
    def spoiler(self, text: str) -> "SmartRenderer":
        """Add spoiler text."""
        if text:
            self._segments.append(Segment(text=text, is_spoiler=True))
        return self
    
    def newline(self, count: int = 1) -> "SmartRenderer":
        """Add newline(s)."""
        if count > 0:
            self._segments.append(Segment(text="\n" * count))
        return self
    
    # ==================== MODE HINTS ====================
    
    def as_caption(self) -> "SmartRenderer":
        """Mark as media caption (stricter rules)."""
        self._is_caption = True
        return self
    
    def as_album(self) -> "SmartRenderer":
        """Mark as album caption (always entity mode)."""
        self._is_album = True
        self._force_entity = True
        return self
    
    def from_message(self) -> "SmartRenderer":
        """Mark as reconstructed from message (entity mode recommended)."""
        self._is_reconstructed = True
        return self
    
    def force_entity_mode(self) -> "SmartRenderer":
        """Force entity-based rendering."""
        self._force_entity = True
        return self
    
    # ==================== ANALYSIS ====================
    
    @property
    def total_length(self) -> int:
        """Get total plain text length."""
        return sum(s.length for s in self._segments)
    
    @property
    def markdown_length(self) -> int:
        """Get total length when rendered as Markdown."""
        return sum(len(s.to_markdown()) for s in self._segments)
    
    def _detect_mode(self) -> RenderMode:
        """
        Automatically detect the best rendering mode.
        
        Returns ENTITY mode if ANY of these conditions are true:
        - Force entity flag is set
        - Album caption
        - Reconstructed from message with entities
        - Text length > MARKDOWN_SAFE_LIMIT
        - Would require splitting
        - Caption > CAPTION_MARKDOWN_LIMIT
        """
        # Forced entity mode
        if self._force_entity:
            return RenderMode.ENTITY
        
        # Album always uses entity
        if self._is_album:
            return RenderMode.ENTITY
        
        # Reconstructed content - prefer entity for safety
        if self._is_reconstructed and any(
            s.is_link or s.user_id for s in self._segments
        ):
            return RenderMode.ENTITY
        
        total = self.total_length
        md_len = self.markdown_length
        
        # Caption rules
        if self._is_caption:
            if total > CAPTION_MARKDOWN_LIMIT:
                return RenderMode.ENTITY
            if md_len > CAPTION_LIMIT:
                return RenderMode.ENTITY
        else:
            # Message rules
            if total > MARKDOWN_SAFE_LIMIT:
                return RenderMode.ENTITY
            if md_len > MESSAGE_LIMIT:
                return RenderMode.ENTITY
        
        # Check for complex entities that might cause issues
        has_complex = any(
            s.is_pre or s.is_spoiler or s.is_underline
            for s in self._segments
        )
        if has_complex and total > 2000:
            return RenderMode.ENTITY
        
        return RenderMode.MARKDOWN
    
    def _needs_splitting(self, limit: int) -> bool:
        """Check if content needs to be split."""
        return self.total_length > limit

    def _expand_segments(self, limit: int) -> List[Segment]:
        """Split oversized segments into smaller ones while preserving formatting."""
        if limit <= 0:
            return list(self._segments)

        expanded: List[Segment] = []
        for segment in self._segments:
            if segment.length <= limit:
                expanded.append(segment)
                continue

            for part in split_text_by_utf16(segment.text, limit):
                if part:
                    expanded.append(segment.clone_with_text(part))

        return expanded
    
    # ==================== RENDERING ====================
    
    def render(self, force_entity: bool = False) -> dict:
        """
        Render to send_message kwargs.
        
        Returns dict with either:
        - {'text': str, 'parse_mode': ParseMode.MARKDOWN}
        - {'text': str, 'entities': List[MessageEntity]}
        
        NEVER both parse_mode and entities together.
        """
        if force_entity:
            self._force_entity = True
        
        mode = self._detect_mode()
        
        if mode == RenderMode.MARKDOWN:
            return self._render_markdown()
        else:
            return self._render_entity()
    
    def _render_markdown(self) -> dict:
        """Render as Markdown."""
        text = ''.join(s.to_markdown() for s in self._segments)
        return {
            'text': text,
            'parse_mode': ParseMode.MARKDOWN
        }
    
    def _render_entity(self) -> dict:
        """
        Render with MessageEntity objects.
        
        CRITICAL: Validates all entities before returning to prevent
        ENTITY_BOUNDS_INVALID errors.
        """
        from core.entity_validator import prepare_entities_for_send
        
        text_parts = []
        entities = []
        offset = 0
        
        for segment in self._segments:
            text_parts.append(segment.text)
            
            entity = segment.to_entity(offset)
            if entity:
                entities.append(entity)
            
            offset += segment.length
        
        text = ''.join(text_parts)
        result = {'text': text}
        
        if entities:
            # CRITICAL: Validate entities before returning
            safe_entities = prepare_entities_for_send(text, entities)
            if safe_entities:
                result['entities'] = safe_entities
        
        return result
    
    def render_caption(self, force_entity: bool = False) -> dict:
        """
        Render as media caption.
        
        Returns dict with either:
        - {'caption': str, 'parse_mode': ParseMode.MARKDOWN}
        - {'caption': str, 'caption_entities': List[MessageEntity]}
        """
        self._is_caption = True
        if force_entity:
            self._force_entity = True
        
        mode = self._detect_mode()
        
        if mode == RenderMode.MARKDOWN:
            text = ''.join(s.to_markdown() for s in self._segments)
            return {
                'caption': text,
                'parse_mode': ParseMode.MARKDOWN
            }
        else:
            from core.entity_validator import prepare_entities_for_send
            
            text_parts = []
            entities = []
            offset = 0
            
            for segment in self._segments:
                text_parts.append(segment.text)
                entity = segment.to_entity(offset)
                if entity:
                    entities.append(entity)
                offset += segment.length
            
            caption = ''.join(text_parts)
            result = {'caption': caption}
            
            if entities:
                # CRITICAL: Validate caption entities before returning
                safe_entities = prepare_entities_for_send(caption, entities)
                if safe_entities:
                    result['caption_entities'] = safe_entities
            
            return result
    
    def render_chunks(self, limit: int = MESSAGE_LIMIT) -> List[dict]:
        """
        Render with auto-splitting.
        
        CRITICAL: When splitting is required, ALWAYS uses entity mode.
        Markdown is NEVER used for split messages.
        
        Returns list of send_message kwargs dicts.
        """
        if not self._needs_splitting(limit):
            return [self.render()]
        
        # Splitting required - MUST use entity mode
        chunks = []
        current_segments = []
        current_length = 0
        
        segments = self._expand_segments(limit)
        
        for segment in segments:
            if current_length + segment.length > limit:
                # Flush current chunk
                if current_segments:
                    chunk_renderer = SmartRenderer()
                    chunk_renderer._segments = current_segments
                    chunk_renderer._force_entity = True  # Force entity for splits
                    chunks.append(chunk_renderer._render_entity())
                
                current_segments = [segment]
                current_length = segment.length
            else:
                current_segments.append(segment)
                current_length += segment.length
        
        # Final chunk
        if current_segments:
            chunk_renderer = SmartRenderer()
            chunk_renderer._segments = current_segments
            chunk_renderer._force_entity = True
            chunks.append(chunk_renderer._render_entity())
        
        return chunks
    
    def render_caption_chunks(self, limit: int = CAPTION_LIMIT) -> Tuple[dict, Optional[List[dict]]]:
        """
        Render caption with overflow handling.
        
        Returns (caption_kwargs, overflow_message_kwargs_list or None)
        
        If caption exceeds limit:
        - First part goes as caption (entity mode)
        - Overflow goes as one or more separate messages (entity mode)
        """
        if not self._needs_splitting(limit):
            return self.render_caption(), None
        
        # Split required
        caption_segments = []
        overflow_segments = []
        current_length = 0
        in_overflow = False
        
        segments = self._expand_segments(limit)
        
        for segment in segments:
            if not in_overflow and current_length + segment.length <= limit:
                caption_segments.append(segment)
                current_length += segment.length
            else:
                in_overflow = True
                overflow_segments.append(segment)
        
        # Render caption (entity mode)
        cap_renderer = SmartRenderer()
        cap_renderer._segments = caption_segments
        cap_renderer._force_entity = True
        cap_renderer._is_caption = True
        caption_result = cap_renderer.render_caption(force_entity=True)
        
        # Render overflow if exists
        overflow_results: Optional[List[dict]] = None
        if overflow_segments:
            overflow_renderer = SmartRenderer()
            overflow_renderer._segments = [Segment(text="[Caption continued]\n\n", is_bold=True)]
            overflow_renderer._segments.extend(overflow_segments)
            overflow_renderer._force_entity = True
            overflow_results = overflow_renderer.render_chunks(limit=MESSAGE_LIMIT)
        
        return caption_result, overflow_results
    
    def clear(self) -> "SmartRenderer":
        """Clear all segments and reset flags."""
        self._segments.clear()
        self._force_entity = False
        self._is_caption = False
        self._is_album = False
        self._is_reconstructed = False
        return self


# ==================== CONVERSION UTILITIES ====================

def from_text_and_entities(
    text: str,
    entities: List[MessageEntity],
    is_caption: bool = False
) -> SmartRenderer:
    renderer = SmartRenderer()
    renderer._is_reconstructed = True
    renderer._is_caption = is_caption

    # Pyrogram returns Str objects (not plain str) for msg.text/caption.
    # Str overrides encode() for UTF-16 entity offsets — calling
    # .encode("utf-16-le") on it raises a codec error.  Always convert first.
    if text is not None:
        text = str(text)

    if not text:
        return renderer

    if not entities:
        renderer.text(text)
        return renderer

    sorted_entities = sorted(entities, key=lambda e: (e.offset, -e.length))
    total_utf16 = utf16_length(text)
    current_pos = 0

    for entity in sorted_entities:
        offset = entity.offset or 0
        length = entity.length or 0
        end_offset = offset + length

        if offset < 0 or end_offset > total_utf16:
            continue

        start = utf16_offset_to_index(text, offset)
        end = utf16_offset_to_index(text, end_offset)

        if start < current_pos:
            continue

        if start > current_pos:
            renderer.text(text[current_pos:start])

        entity_text = text[start:end]

        if entity.type == MessageEntityType.TEXT_LINK:
            url = getattr(entity, 'url', None)
            if url:
                renderer.link(entity_text, url)
            else:
                renderer.text(entity_text)
        elif entity.type == MessageEntityType.TEXT_MENTION:
            user = getattr(entity, 'user', None)
            if user and user.id:
                renderer.mention(entity_text, user.id)
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
        elif entity.type == MessageEntityType.URL:
            renderer.text(entity_text)
        elif entity.type == MessageEntityType.MENTION:
            renderer.text(entity_text)
        else:
            renderer.text(entity_text)

        current_pos = end

    if current_pos < len(text):
        renderer.text(text[current_pos:])

    return renderer


def from_message(msg) -> SmartRenderer:
    """
    Create SmartRenderer from Pyrogram Message.
    """
    if msg.text:
        return from_text_and_entities(str(msg.text), msg.entities or [])
    if msg.caption:
        return from_text_and_entities(str(msg.caption), msg.caption_entities or [], is_caption=True)
    return SmartRenderer()


async def smart_send_message(
    client,
    chat_id: int,
    renderer: SmartRenderer,
    reply_to_message_id: int = None,
    reply_markup = None,
    disable_web_page_preview: bool = True
) -> list:
    """
    Send message using SmartRenderer with auto mode detection.
    
    Handles:
    - Automatic Markdown/Entity selection
    - Auto-splitting for long messages
    - Link preview control
    
    Returns list of sent messages.
    """
    from pyrogram.types import LinkPreviewOptions
    
    chunks = renderer.render_chunks()
    sent = []
    
    for i, kwargs in enumerate(chunks):
        try:
            # Add common params
            kwargs['chat_id'] = chat_id
            kwargs['link_preview_options'] = LinkPreviewOptions(is_disabled=disable_web_page_preview)
            
            if i == 0 and reply_to_message_id:
                kwargs['reply_to_message_id'] = reply_to_message_id
            
            if i == len(chunks) - 1 and reply_markup:
                kwargs['reply_markup'] = reply_markup
            
            msg = await client.send_message(**kwargs)
            sent.append(msg)
            
        except Exception as e:
            logger.warning(f"smart_send_message error on chunk {i}: {e}")
    
    return sent


async def smart_send_media(
    client,
    chat_id: int,
    media_type: str,
    file_path: str,
    renderer: SmartRenderer = None,
    reply_to_message_id: int = None,
    reply_markup = None,
    **extra_kwargs
) -> list:
    """
    Send media with smart caption handling.
    
    Args:
        client: Pyrogram client
        chat_id: Target chat
        media_type: "photo", "video", "document", "audio", "voice", "animation"
        file_path: Path to media file
        renderer: SmartRenderer with caption (optional)
        reply_to_message_id: Reply to message
        reply_markup: Keyboard markup
        **extra_kwargs: Additional media-specific params (thumb, duration, etc.)
    
    Returns list of sent messages (media + overflow if any).
    """
    sent = []
    overflow_chunks = None
    
    # Prepare caption
    if renderer:
        renderer.as_caption()
        caption_kwargs, overflow_chunks = renderer.render_caption_chunks()
    else:
        caption_kwargs = {}
    
    # Build send kwargs
    send_kwargs = {
        'chat_id': chat_id,
        **caption_kwargs,
        **extra_kwargs
    }
    
    if reply_to_message_id:
        send_kwargs['reply_to_message_id'] = reply_to_message_id
    if reply_markup:
        send_kwargs['reply_markup'] = reply_markup
    
    # Send media
    try:
        send_func = getattr(client, f"send_{media_type}")
        msg = await send_func(file_path, **send_kwargs)
        sent.append(msg)
    except Exception as e:
        logger.error(f"smart_send_media error: {e}")
        raise
    
    # Send overflow caption if exists
    if overflow_chunks:
        for idx, chunk_kwargs in enumerate(overflow_chunks):
            try:
                payload = dict(chunk_kwargs)
                payload['chat_id'] = chat_id
                msg = await client.send_message(**payload)
                sent.append(msg)
            except Exception as e:
                logger.warning(f"Failed to send caption overflow chunk {idx}: {e}")
    
    return sent
