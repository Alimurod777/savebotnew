"""
core/entity_builder.py - MessageEntity-based text formatting (NOT Markdown/HTML).

CRITICAL: This module uses Telegram's MessageEntity objects directly,
avoiding all Markdown/HTML parsing issues.

Features:
- Entity-aware text splitting (respects 4096 UTF-16 unit limit)
- Automatic entity offset recalculation per chunk (UTF-16)
- Hyperlinks via MessageEntity.TEXT_LINK
- All formatting preserved across message splits
- No broken links or formatting

IMPORTANT: All offsets and lengths MUST use UTF-16 code units,
not Python len() which counts Unicode code points.
"""

import logging
from typing import List, Tuple, Optional
from dataclasses import dataclass
from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType

from core.entity_splitter import utf16_len

logger = logging.getLogger(__name__)

# Telegram limits
MAX_MESSAGE_LENGTH = 4096
MAX_CAPTION_LENGTH = 1024


@dataclass
class TextWithEntities:
    """Text with associated MessageEntity objects.

    Supports tuple unpacking for backward compatibility:
        text, entities = builder.build()
    """
    text: str
    entities: List[MessageEntity]

    def __bool__(self):
        return bool(self.text)

    def __len__(self):
        return len(self.text)

    def __iter__(self):
        """Allow tuple unpacking: text, entities = result."""
        return iter((self.text, self.entities))

    def __getitem__(self, index):
        """Allow indexing: result[0] = text, result[1] = entities."""
        return (self.text, self.entities)[index]


class EntityBuilder:
    """
    Builder for creating text with MessageEntity objects.
    
    Usage:
        builder = EntityBuilder()
        builder.add_text("Click ")
        builder.add_link("here", "https://example.com")
        builder.add_text(" to continue")
        
        result = builder.build()
        # result.text = "Click here to continue"
        # result.entities = [MessageEntity(TEXT_LINK, offset=6, length=4, url="...")]
    """
    
    def __init__(self):
        self._text = ""
        self._entities: List[MessageEntity] = []
    
    def add_text(self, text: str) -> "EntityBuilder":
        """Add plain text."""
        self._text += text
        return self
    
    def add_link(self, text: str, url: str) -> "EntityBuilder":
        """Add hyperlink text."""
        if not text or not url:
            return self
        
        offset = utf16_len(self._text)
        self._text += text
        
        self._entities.append(MessageEntity(
            type=MessageEntityType.TEXT_LINK,
            offset=offset,
            length=utf16_len(text),
            url=url
        ))
        return self
    
    def add_bold(self, text: str) -> "EntityBuilder":
        """Add bold text."""
        if not text:
            return self
        
        offset = utf16_len(self._text)
        self._text += text
        
        self._entities.append(MessageEntity(
            type=MessageEntityType.BOLD,
            offset=offset,
            length=utf16_len(text)
        ))
        return self
    
    def add_italic(self, text: str) -> "EntityBuilder":
        """Add italic text."""
        if not text:
            return self
        
        offset = utf16_len(self._text)
        self._text += text
        
        self._entities.append(MessageEntity(
            type=MessageEntityType.ITALIC,
            offset=offset,
            length=utf16_len(text)
        ))
        return self
    
    def add_code(self, text: str) -> "EntityBuilder":
        """Add inline code."""
        if not text:
            return self
        
        offset = utf16_len(self._text)
        self._text += text
        
        self._entities.append(MessageEntity(
            type=MessageEntityType.CODE,
            offset=offset,
            length=utf16_len(text)
        ))
        return self
    
    def add_newline(self, count: int = 1) -> "EntityBuilder":
        """Add newline(s)."""
        self._text += "\n" * count
        return self
    
    def build(self) -> TextWithEntities:
        """Build final text with entities."""
        return TextWithEntities(
            text=self._text,
            entities=self._entities.copy()
        )
    
    def clear(self) -> "EntityBuilder":
        """Clear builder for reuse."""
        self._text = ""
        self._entities.clear()
        return self


def split_text_with_entities(
    text: str,
    entities: List[MessageEntity],
    max_length: int = MAX_MESSAGE_LENGTH
) -> List[TextWithEntities]:
    """
    Split long text into chunks while preserving entities.
    
    CRITICAL: Uses UTF-16 code unit measurement for all limits.
    Entity offsets are recalculated for each chunk.
    Entities are NEVER split - if an entity doesn't fit, the entire
    entity moves to the next chunk.
    
    Args:
        text: Original text
        entities: List of MessageEntity objects
        max_length: Maximum UTF-16 code unit length per chunk (default 4096)
    
    Returns:
        List of TextWithEntities chunks
    """
    from core.entity_splitter import (
        split_text_with_entities as _entity_split,
        utf16_len,
    )
    
    if not text:
        return []
    
    if utf16_len(text) <= max_length:
        return [TextWithEntities(text=text, entities=entities or [])]
    
    # Delegate to the production-grade entity-aware splitter which
    # uses proper UTF-16 measurement and never breaks entities/hyperlinks
    from core.entity_splitter import TextChunk
    chunks = _entity_split(text, entities or [], max_length)
    
    # Convert TextChunk -> TextWithEntities
    return [
        TextWithEntities(text=c.text, entities=c.entities)
        for c in chunks
        if c.text
    ]


def extract_entities_from_message(msg) -> Tuple[str, List[MessageEntity]]:
    """
    Extract text and entities from a Pyrogram Message.
    
    Handles both text and caption, preserving all entity types.
    
    Args:
        msg: Pyrogram Message object
    
    Returns:
        (text, entities) tuple
    """
    # Try text first, then caption
    if msg.text:
        text = msg.text
        entities = msg.entities or []
    elif msg.caption:
        text = msg.caption
        entities = msg.caption_entities or []
    else:
        return "", []
    
    # Convert entities to list (might be a generator)
    entities = list(entities) if entities else []
    
    return text, entities


def copy_entities(entities: List[MessageEntity]) -> List[MessageEntity]:
    """Create deep copies of entities."""
    if not entities:
        return []
    
    copies = []
    for e in entities:
        copies.append(MessageEntity(
            type=e.type,
            offset=e.offset,
            length=e.length,
            url=getattr(e, 'url', None),
            user=getattr(e, 'user', None),
            language=getattr(e, 'language', None),
            custom_emoji_id=getattr(e, 'custom_emoji_id', None)
        ))
    return copies


async def send_text_with_entities(
    client,
    chat_id: int,
    text: str,
    entities: List[MessageEntity] = None,
    reply_to_message_id: int = None,
    disable_web_page_preview: bool = True
) -> List:
    """
    Send text message with entities, auto-splitting if needed.
    
    Args:
        client: Pyrogram Client
        chat_id: Target chat ID
        text: Message text
        entities: List of MessageEntity
        reply_to_message_id: Optional reply
        disable_web_page_preview: Disable link previews
    
    Returns:
        List of sent messages
    """
    from pyrogram.types import LinkPreviewOptions
    
    if not text:
        return []
    
    # Split if needed
    chunks = split_text_with_entities(text, entities or [])
    
    sent_messages = []
    for i, chunk in enumerate(chunks):
        try:
            # Only reply to first message
            reply_id = reply_to_message_id if i == 0 else None
            
            msg = await client.send_message(
                chat_id=chat_id,
                text=chunk.text,
                entities=chunk.entities if chunk.entities else None,
                reply_to_message_id=reply_id,
                link_preview_options=LinkPreviewOptions(is_disabled=disable_web_page_preview)
            )
            sent_messages.append(msg)
            
        except Exception as e:
            logger.warning(f"Error sending chunk {i}: {e}")
    
    return sent_messages
