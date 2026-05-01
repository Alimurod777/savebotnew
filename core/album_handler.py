"""
core/album_handler.py - Atomic album (media group) handling.

CRITICAL REQUIREMENTS:
1. Detect albums via media_group_id
2. Process albums atomically (all or nothing)
3. Preserve original order
4. Caption + hyperlinks go on FIRST media only
5. Respect Telegram limits (max 10 media per batch, 1024 char caption)
6. Long captions: first media gets truncated caption, overflow as follow-up message

PYROFORK COMPATIBILITY:
- Handle 'Messages.__init__() missing keyword-only argument: topics'
- Treat this as non-fatal, assume message was sent unless Telegram rejects

Usage:
    handler = AlbumHandler(client)
    
    # Collect album messages
    await handler.collect(message)
    
    # Send collected album to target
    sent = await handler.send_album(target_chat_id)
"""

import asyncio
import logging
from typing import List, Optional, Dict, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict

from pyrogram import Client
from pyrogram.types import Message, InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
from pyrogram.errors import FloodWait, ChatAdminRequired, MediaEmpty

from core.entity_validator import (
    utf16_length, 
    split_caption_with_entities,
    sanitize_entities,
    prepare_entities_for_send,
    CAPTION_LIMIT,
    SplitChunk
)

logger = logging.getLogger(__name__)

# Telegram limits
MAX_ALBUM_SIZE = 10
ALBUM_COLLECT_TIMEOUT = 2.0  # Seconds to wait for more album messages


@dataclass
class AlbumMessage:
    """A single message in an album."""
    message: Message
    position: int  # Original position in album
    media_type: str  # photo, video, document, audio
    file_id: str
    file_unique_id: str
    caption: Optional[str] = None
    caption_entities: Optional[List] = None
    
    @classmethod
    def from_message(cls, msg: Message, position: int = 0) -> Optional["AlbumMessage"]:
        """Create AlbumMessage from Pyrogram Message."""
        media_type = None
        file_id = None
        file_unique_id = None
        
        if msg.photo:
            media_type = "photo"
            file_id = msg.photo.file_id
            file_unique_id = msg.photo.file_unique_id
        elif msg.video:
            media_type = "video"
            file_id = msg.video.file_id
            file_unique_id = msg.video.file_unique_id
        elif msg.document:
            media_type = "document"
            file_id = msg.document.file_id
            file_unique_id = msg.document.file_unique_id
        elif msg.audio:
            media_type = "audio"
            file_id = msg.audio.file_id
            file_unique_id = msg.audio.file_unique_id
        else:
            return None
        
        return cls(
            message=msg,
            position=position,
            media_type=media_type,
            file_id=file_id,
            file_unique_id=file_unique_id,
            caption=msg.caption,
            caption_entities=msg.caption_entities
        )


@dataclass
class Album:
    """Collection of messages forming a media group."""
    media_group_id: str
    chat_id: int
    messages: List[AlbumMessage] = field(default_factory=list)
    collected_at: float = 0.0
    
    def add(self, msg: AlbumMessage) -> None:
        """Add message to album, maintaining order."""
        self.messages.append(msg)
        self.messages.sort(key=lambda m: m.position)
    
    @property
    def size(self) -> int:
        return len(self.messages)
    
    @property
    def is_complete(self) -> bool:
        """Albums are complete after timeout or at max size."""
        return self.size >= MAX_ALBUM_SIZE
    
    def get_combined_caption(self) -> Tuple[Optional[str], Optional[List]]:
        """Get combined caption and entities from first message with caption."""
        for msg in self.messages:
            if msg.caption:
                return msg.caption, msg.caption_entities
        return None, None


class AlbumCollector:
    """
    Collects album messages across a time window.
    
    Telegram sends album messages individually but with same media_group_id.
    We buffer them and process when complete or timeout.
    """
    
    def __init__(self, timeout: float = ALBUM_COLLECT_TIMEOUT):
        self._timeout = timeout
        self._albums: Dict[str, Album] = {}
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._timers: Dict[str, asyncio.Task] = {}
    
    async def add_message(
        self, 
        message: Message,
        on_complete: Optional[callable] = None
    ) -> Optional[Album]:
        """
        Add a message to its album.
        
        Args:
            message: Message with media_group_id
            on_complete: Optional callback when album is ready
        
        Returns:
            Complete Album if ready, None if still collecting
        """
        if not message.media_group_id:
            return None
        
        group_id = message.media_group_id
        
        async with self._locks[group_id]:
            # Create or get album
            if group_id not in self._albums:
                self._albums[group_id] = Album(
                    media_group_id=group_id,
                    chat_id=message.chat.id,
                    collected_at=asyncio.get_running_loop().time()
                )
            
            album = self._albums[group_id]
            
            # Create album message
            album_msg = AlbumMessage.from_message(message, position=message.id)
            if album_msg:
                album.add(album_msg)
            
            # Check if complete
            if album.is_complete:
                return self._finalize(group_id)
            
            # Start/restart timeout timer
            if group_id in self._timers:
                self._timers[group_id].cancel()
            
            async def timeout_handler():
                await asyncio.sleep(self._timeout)
                async with self._locks[group_id]:
                    if group_id in self._albums:
                        completed = self._finalize(group_id)
                        if on_complete and completed:
                            await on_complete(completed)
            
            self._timers[group_id] = asyncio.create_task(timeout_handler())
        
        return None
    
    def _finalize(self, group_id: str) -> Optional[Album]:
        """Remove and return completed album."""
        album = self._albums.pop(group_id, None)
        if group_id in self._timers:
            self._timers[group_id].cancel()
            del self._timers[group_id]
        return album
    
    def get_pending_count(self) -> int:
        """Get number of albums being collected."""
        return len(self._albums)


class AlbumHandler:
    """
    Safe album send handler with entity validation.
    
    Features:
    - Atomic album sending (all or nothing)
    - Entity-safe caption handling
    - Caption overflow as follow-up message
    - Order preservation
    - Pyrofork compatibility
    """
    
    def __init__(self, client: Client):
        self.client = client
    
    async def send_album(
        self,
        target_chat_id: int,
        album: Album,
        reply_to_message_id: Optional[int] = None
    ) -> Tuple[List[Message], Optional[Message]]:
        """
        Send album to target chat.
        
        CRITICAL: Uses entity-safe caption handling.
        - Caption goes on first media only
        - Long captions are split, overflow sent as follow-up
        - Never uses parse_mode when entities are present
        
        Args:
            target_chat_id: Target chat
            album: Album to send
            reply_to_message_id: Optional reply target
        
        Returns:
            (album_messages, overflow_message or None)
        """
        if not album.messages:
            return [], None
        
        # Get caption with entity-safe splitting
        caption, caption_entities = album.get_combined_caption()
        
        caption_kwargs = {}
        overflow_chunks = None
        
        if caption:
            cap_chunk, overflow_chunks = split_caption_with_entities(
                caption, caption_entities or []
            )
            
            if cap_chunk.text:
                caption_kwargs['caption'] = cap_chunk.text
                if cap_chunk.entities:
                    caption_kwargs['caption_entities'] = cap_chunk.entities
        
        # Build input media list
        media_list = []
        for i, album_msg in enumerate(album.messages):
            # Only first media gets caption
            media_caption = caption_kwargs.get('caption') if i == 0 else None
            media_entities = caption_kwargs.get('caption_entities') if i == 0 else None
            
            input_media = self._create_input_media(
                album_msg,
                caption=media_caption,
                caption_entities=media_entities
            )
            if input_media:
                media_list.append(input_media)
        
        if not media_list:
            logger.warning(f"No valid media in album {album.media_group_id}")
            return [], None
        
        # Send album with retry
        sent_messages = await self._send_media_group_safe(
            target_chat_id,
            media_list,
            reply_to_message_id
        )
        
        # Send overflow caption if needed
        overflow_msg = None
        if overflow_chunks and sent_messages:
            overflow_msg = await self._send_overflow(
                target_chat_id,
                overflow_chunks
            )
        
        return sent_messages, overflow_msg
    
    def _create_input_media(
        self,
        album_msg: AlbumMessage,
        caption: Optional[str] = None,
        caption_entities: Optional[List] = None
    ) -> Optional[Any]:
        """Create appropriate InputMedia* object."""
        
        # Build kwargs - NEVER mix parse_mode with entities
        kwargs = {'media': album_msg.file_id}
        
        if caption:
            kwargs['caption'] = caption
            if caption_entities:
                # Validate entities before sending
                safe_entities = prepare_entities_for_send(caption, caption_entities)
                if safe_entities:
                    kwargs['caption_entities'] = safe_entities
        
        media_map = {
            'photo': InputMediaPhoto,
            'video': InputMediaVideo,
            'document': InputMediaDocument,
            'audio': InputMediaAudio,
        }
        
        media_class = media_map.get(album_msg.media_type)
        if not media_class:
            return None
        
        try:
            return media_class(**kwargs)
        except Exception as e:
            logger.warning(f"Failed to create InputMedia: {e}")
            return None
    
    async def _send_media_group_safe(
        self,
        chat_id: int,
        media: List,
        reply_to_message_id: Optional[int] = None,
        max_retries: int = 3
    ) -> List[Message]:
        """
        Send media group with retry and Pyrofork compatibility.
        
        Handles:
        - FloodWait with backoff
        - 'topics' argument error (Pyrofork quirk)
        - Other transient errors
        """
        for attempt in range(max_retries):
            try:
                return await self.client.send_media_group(
                    chat_id=chat_id,
                    media=media,
                    reply_to_message_id=reply_to_message_id
                )
            
            except FloodWait as e:
                wait = min(getattr(e, 'value', getattr(e, 'x', 30)), 60)
                logger.warning(f"Album FloodWait: {wait}s")
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait)
                else:
                    raise
            
            except TypeError as te:
                # Pyrofork 'topics' argument issue
                error_str = str(te).lower()
                if 'topics' in error_str or 'messages' in error_str:
                    logger.warning(f"Pyrofork compatibility issue (non-fatal): {te}")
                    # Return empty - caller should check
                    return []
                raise
            
            except MediaEmpty:
                logger.warning("MediaEmpty - file may have expired")
                return []
            
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    logger.error(f"Album send failed: {e}")
                    raise
        
        return []
    
    async def _send_overflow(
        self,
        chat_id: int,
        chunks: List[SplitChunk]
    ) -> Optional[Message]:
        """Send caption overflow as follow-up message."""
        if not chunks:
            return None
        
        from core.safe_send import safe_send_message
        
        # Add continuation header to first chunk
        first_chunk = chunks[0]
        text = f"[Caption continued]\n\n{first_chunk.text}"
        
        # Adjust entity offsets for added header
        header_len = utf16_length("[Caption continued]\n\n")
        adjusted_entities = []
        for e in first_chunk.entities:
            try:
                from pyrogram.types import MessageEntity
                adjusted = MessageEntity(
                    type=e.type,
                    offset=e.offset + header_len,
                    length=e.length,
                    url=getattr(e, 'url', None),
                    user=getattr(e, 'user', None),
                    language=getattr(e, 'language', None),
                )
                adjusted_entities.append(adjusted)
            except Exception:
                pass
        
        # Validate adjusted entities
        safe_entities = prepare_entities_for_send(text, adjusted_entities)
        
        try:
            if safe_entities:
                return await self.client.send_message(
                    chat_id=chat_id,
                    text=text,
                    entities=safe_entities
                )
            else:
                return await self.client.send_message(
                    chat_id=chat_id,
                    text=text
                )
        except Exception as e:
            logger.warning(f"Failed to send overflow: {e}")
            return None


async def safe_get_media_group(
    client: Client,
    chat_id: int,
    message_id: int
) -> List[Message]:
    """
    Safely get media group, handling Pyrofork compatibility issues.
    
    CRITICAL: Handles 'Messages.__init__() missing required keyword-only argument: topics'
    This is a non-fatal Pyrofork quirk.
    
    FIX: Added manual album fetching as fallback for Pyrofork compatibility.
    
    Args:
        client: Pyrogram client
        chat_id: Chat ID
        message_id: Any message ID in the media group
    
    Returns:
        List of Message objects, or empty list on failure
    """
    try:
        result = await client.get_media_group(chat_id, message_id)
        
        if result is None:
            return []
        if isinstance(result, list):
            return result
        
        # Try to convert Messages object to list
        try:
            return list(result)
        except TypeError:
            return [result] if result else []
            
    except TypeError as te:
        error_str = str(te).lower()
        if 'topics' in error_str or 'messages' in error_str:
            logger.debug(f"Pyrofork Messages compatibility issue: {te}")
            # FIX: Enhanced fallback - fetch album manually by scanning adjacent messages
            return await _fetch_album_manually(client, chat_id, message_id)
        else:
            logger.warning(f"get_media_group TypeError: {te}")
        return []
        
    except Exception as e:
        logger.debug(f"get_media_group error: {e}")
        return []


async def _fetch_album_manually(
    client: Client,
    chat_id: int,
    message_id: int
) -> List[Message]:
    """
    Manually fetch album messages when get_media_group fails (Pyrofork workaround).
    
    Algorithm:
    1. Get the target message
    2. If it has media_group_id, scan adjacent messages (±10 IDs)
    3. Collect all with same media_group_id
    4. Sort by message ID
    """
    try:
        # Get the original message first
        msg = await client.get_messages(chat_id, message_id)
        if not msg or getattr(msg, 'empty', False):
            return []
        
        # If no media_group_id, it's not an album
        if not msg.media_group_id:
            return [msg]
        
        media_group_id = msg.media_group_id
        album_messages = [msg]
        
        # Scan adjacent messages (albums are usually sequential)
        # Telegram albums are max 10 items, so scan ±10 IDs
        ids_to_check = []
        for offset in range(-10, 11):
            if offset != 0:  # Skip the one we already have
                ids_to_check.append(message_id + offset)
        
        try:
            adjacent_msgs = await client.get_messages(chat_id, ids_to_check)
            if not isinstance(adjacent_msgs, list):
                adjacent_msgs = [adjacent_msgs] if adjacent_msgs else []
            
            for adj_msg in adjacent_msgs:
                if adj_msg and not getattr(adj_msg, 'empty', False):
                    if adj_msg.media_group_id == media_group_id:
                        if adj_msg.id not in [m.id for m in album_messages]:
                            album_messages.append(adj_msg)
        except Exception as e:
            logger.debug(f"Error fetching adjacent messages: {e}")
        
        # Sort by message ID to preserve order
        album_messages.sort(key=lambda m: m.id)
        
        logger.debug(f"Manual album fetch: found {len(album_messages)} messages for group {media_group_id}")
        return album_messages
        
    except Exception as e:
        logger.warning(f"_fetch_album_manually error: {e}")
        return []


async def clone_album(
    client: Client,
    source_messages: List[Message],
    target_chat_id: int,
    reply_to_message_id: Optional[int] = None
) -> Tuple[List[Message], Optional[Message]]:
    """
    Clone an album to target chat with entity-safe caption handling.
    
    Convenience wrapper around AlbumHandler.
    
    Args:
        client: Pyrogram client
        source_messages: Original album messages
        target_chat_id: Target chat ID
        reply_to_message_id: Optional reply target
    
    Returns:
        (sent_messages, overflow_message or None)
    """
    if not source_messages:
        return [], None
    
    # Build Album from messages
    first_msg = source_messages[0]
    album = Album(
        media_group_id=first_msg.media_group_id or str(first_msg.id),
        chat_id=first_msg.chat.id
    )
    
    for i, msg in enumerate(source_messages):
        album_msg = AlbumMessage.from_message(msg, position=i)
        if album_msg:
            album.add(album_msg)
    
    handler = AlbumHandler(client)
    return await handler.send_album(target_chat_id, album, reply_to_message_id)
