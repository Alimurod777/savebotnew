"""
core/safe_copy.py - Safe message/media copying with entity preservation.

DESIGN: Copy messages WITHOUT any Markdown parsing.
Uses ONLY MessageEntity for formatting - 100% reliable.

CRITICAL RULES:
1. NEVER use parse_mode when entities are present
2. NEVER convert entities to Markdown
3. ALWAYS validate entities before sending
4. For long text: split with entity offset recalculation

This module provides the SAFEST way to copy messages with hyperlinks.
"""

import asyncio
import logging
from typing import Optional, List, Tuple, Any, Union
from dataclasses import dataclass

from pyrogram import Client
from pyrogram.types import (
    Message, MessageEntity,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
)
from pyrogram.enums import MessageMediaType, MessageEntityType
from pyrogram.errors import FloodWait, MediaEmpty

from core.entity_validator import (
    utf16_length,
    prepare_entities_for_send,
    split_with_entities,
    split_caption_with_entities,
    CAPTION_LIMIT,
    MESSAGE_LIMIT,
    SplitChunk
)

logger = logging.getLogger(__name__)


@dataclass
class CopyResult:
    """Result of a copy operation."""
    success: bool
    messages: List[Message]
    error: Optional[str] = None
    overflow_sent: bool = False


def clone_entity(entity: MessageEntity, offset_delta: int = 0) -> Optional[MessageEntity]:
    """
    Clone a MessageEntity with optional offset adjustment.
    
    Args:
        entity: Original entity
        offset_delta: Amount to add/subtract from offset
    
    Returns:
        New MessageEntity or None if invalid
    """
    new_offset = entity.offset + offset_delta
    if new_offset < 0:
        return None
    
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
    except Exception:
        return None


def extract_content(msg: Message) -> Tuple[Optional[str], Optional[List[MessageEntity]]]:
    """
    Extract text/caption and entities from message.
    
    Returns:
        (text, entities) - NEVER returns Markdown, always raw text + entities
    """
    if msg.text:
        return msg.text, list(msg.entities) if msg.entities else None
    elif msg.caption:
        return msg.caption, list(msg.caption_entities) if msg.caption_entities else None
    return None, None


async def safe_copy_text_message(
    client: Client,
    target_chat_id: int,
    source_msg: Message,
    reply_to_message_id: Optional[int] = None
) -> CopyResult:
    """
    Copy a text message with all entities preserved.
    
    SAFE: Uses only MessageEntity, never parse_mode.
    Handles long messages with entity-aware splitting.
    
    Args:
        client: Pyrogram client
        target_chat_id: Target chat
        source_msg: Source message to copy
        reply_to_message_id: Optional reply target
    
    Returns:
        CopyResult with sent messages
    """
    text, entities = extract_content(source_msg)
    
    if not text:
        return CopyResult(success=False, messages=[], error="No text content")
    
    text_len = utf16_length(text)
    
    # Short message - send directly
    if text_len <= MESSAGE_LIMIT:
        safe_entities = prepare_entities_for_send(text, entities) if entities else None
        
        try:
            msg = await _send_text_safe(
                client, target_chat_id, text, safe_entities, reply_to_message_id
            )
            return CopyResult(success=True, messages=[msg] if msg else [])
        except Exception as e:
            return CopyResult(success=False, messages=[], error=str(e))
    
    # Long message - split with entity preservation
    chunks = split_with_entities(text, entities or [], MESSAGE_LIMIT)
    
    sent = []
    for i, chunk in enumerate(chunks):
        reply_id = reply_to_message_id if i == 0 else None
        
        try:
            msg = await _send_text_safe(
                client, target_chat_id, chunk.text, chunk.entities or None, reply_id
            )
            if msg:
                sent.append(msg)
        except Exception as e:
            logger.warning(f"Failed to send chunk {i}: {e}")
        
        if i < len(chunks) - 1:
            await asyncio.sleep(0.3)
    
    return CopyResult(
        success=len(sent) > 0,
        messages=sent,
        overflow_sent=len(chunks) > 1
    )


async def _send_text_safe(
    client: Client,
    chat_id: int,
    text: str,
    entities: Optional[List[MessageEntity]],
    reply_to_message_id: Optional[int]
) -> Optional[Message]:
    """Internal: Send text with entities only (no parse_mode)."""
    kwargs = {
        'chat_id': chat_id,
        'text': text,
        'disable_web_page_preview': True,
    }
    
    if entities:
        kwargs['entities'] = entities
    
    if reply_to_message_id:
        try:
            return await client.send_message(**kwargs, reply_to_message_id=reply_to_message_id)
        except Exception:
            pass  # Silent fallback without reply
    
    try:
        return await client.send_message(**kwargs)
    except FloodWait as e:
        wait = min(getattr(e, 'value', getattr(e, 'x', 30)), 60)
        await asyncio.sleep(wait)
        return await client.send_message(**kwargs)
    except Exception as e:
        # If entity error, try plain text
        if 'ENTITY' in str(e).upper():
            try:
                del kwargs['entities']
                return await client.send_message(**kwargs)
            except Exception:
                pass
        logger.warning(f"send_text_safe failed: {e}")
        return None


async def safe_copy_media_message(
    client: Client,
    target_chat_id: int,
    source_msg: Message,
    file_path: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
    progress: Any = None
) -> CopyResult:
    """
    Copy a media message with caption entities preserved.
    
    SAFE: Uses only caption_entities, never parse_mode.
    Handles long captions with overflow to follow-up message.
    
    Args:
        client: Pyrogram client
        target_chat_id: Target chat
        source_msg: Source message with media
        file_path: Path to downloaded file (required)
        reply_to_message_id: Optional reply target
        progress: Upload progress callback
    
    Returns:
        CopyResult with sent messages
    """
    if not file_path:
        return CopyResult(success=False, messages=[], error="No file path provided")
    
    caption, entities = extract_content(source_msg)
    
    # Determine media type
    media_type = _get_media_type(source_msg)
    if not media_type:
        return CopyResult(success=False, messages=[], error="Unknown media type")
    
    # Prepare caption
    caption_kwargs = {}
    overflow_chunks = None
    
    if caption:
        cap_len = utf16_length(caption)
        
        if cap_len <= CAPTION_LIMIT:
            # Short caption - use directly
            safe_entities = prepare_entities_for_send(caption, entities) if entities else None
            caption_kwargs['caption'] = caption
            if safe_entities:
                caption_kwargs['caption_entities'] = safe_entities
        else:
            # Long caption - split
            cap_chunk, overflow_chunks = split_caption_with_entities(caption, entities or [])
            caption_kwargs['caption'] = cap_chunk.text
            if cap_chunk.entities:
                caption_kwargs['caption_entities'] = cap_chunk.entities
    
    # Build send kwargs
    send_kwargs = {
        'chat_id': target_chat_id,
        media_type: file_path,
        **caption_kwargs
    }
    
    if progress:
        send_kwargs['progress'] = progress
    
    # Add media-specific params
    if media_type == 'video':
        if source_msg.video:
            send_kwargs['duration'] = source_msg.video.duration or 0
            send_kwargs['width'] = source_msg.video.width or 0
            send_kwargs['height'] = source_msg.video.height or 0
            send_kwargs['supports_streaming'] = True
    elif media_type == 'audio':
        if source_msg.audio:
            send_kwargs['duration'] = source_msg.audio.duration or 0
            send_kwargs['performer'] = source_msg.audio.performer
            send_kwargs['title'] = source_msg.audio.title
    elif media_type == 'voice':
        if source_msg.voice:
            send_kwargs['duration'] = source_msg.voice.duration or 0
    elif media_type == 'video_note':
        if source_msg.video_note:
            send_kwargs['duration'] = source_msg.video_note.duration or 0
            send_kwargs['length'] = source_msg.video_note.length or 0
    
    # Send media
    sent = []
    
    send_func = getattr(client, f'send_{media_type}')
    
    # Try with reply first
    media_msg = None
    if reply_to_message_id:
        try:
            media_msg = await send_func(**send_kwargs, reply_to_message_id=reply_to_message_id)
        except Exception:
            pass
    
    if not media_msg:
        try:
            media_msg = await send_func(**send_kwargs)
        except FloodWait as e:
            wait = min(getattr(e, 'value', getattr(e, 'x', 30)), 60)
            await asyncio.sleep(wait)
            media_msg = await send_func(**send_kwargs)
        except Exception as e:
            # Try without entities
            if 'ENTITY' in str(e).upper() and 'caption_entities' in send_kwargs:
                try:
                    del send_kwargs['caption_entities']
                    media_msg = await send_func(**send_kwargs)
                except Exception:
                    pass
            if not media_msg:
                return CopyResult(success=False, messages=[], error=str(e))
    
    if media_msg:
        sent.append(media_msg)
    
    # Send overflow caption
    if overflow_chunks and media_msg:
        for chunk in overflow_chunks:
            # Prepend continuation header
            overflow_text = f"[Caption continued]\n\n{chunk.text}"
            # Adjust entity offsets for header
            header_len = utf16_length("[Caption continued]\n\n")
            adjusted_entities = []
            for e in (chunk.entities or []):
                new_e = clone_entity(e, header_len)
                if new_e:
                    adjusted_entities.append(new_e)
            
            safe_entities = prepare_entities_for_send(overflow_text, adjusted_entities)
            
            try:
                overflow_msg = await _send_text_safe(
                    client, target_chat_id, overflow_text, safe_entities, None
                )
                if overflow_msg:
                    sent.append(overflow_msg)
            except Exception as e:
                logger.warning(f"Failed to send overflow: {e}")
    
    return CopyResult(
        success=len(sent) > 0,
        messages=sent,
        overflow_sent=overflow_chunks is not None
    )


def _get_media_type(msg: Message) -> Optional[str]:
    """Get send method name for message's media type."""
    if msg.photo:
        return 'photo'
    elif msg.video:
        return 'video'
    elif msg.document:
        return 'document'
    elif msg.audio:
        return 'audio'
    elif msg.voice:
        return 'voice'
    elif msg.video_note:
        return 'video_note'
    elif msg.animation:
        return 'animation'
    elif msg.sticker:
        return 'sticker'
    return None


async def safe_copy_album(
    client: Client,
    target_chat_id: int,
    album_messages: List[Message],
    file_paths: List[str],
    reply_to_message_id: Optional[int] = None
) -> CopyResult:
    """
    Copy an album (media group) with caption entities preserved.
    
    SAFE: Uses only caption_entities on first media, never parse_mode.
    
    Args:
        client: Pyrogram client
        target_chat_id: Target chat
        album_messages: List of album messages (in order)
        file_paths: Corresponding file paths
        reply_to_message_id: Optional reply target
    
    Returns:
        CopyResult with sent messages
    """
    if not album_messages or not file_paths:
        return CopyResult(success=False, messages=[], error="No album data")
    
    if len(album_messages) != len(file_paths):
        return CopyResult(success=False, messages=[], error="Message/file count mismatch")
    
    # Get caption from first message with caption
    caption = None
    entities = None
    for msg in album_messages:
        if msg.caption:
            caption = msg.caption
            entities = list(msg.caption_entities) if msg.caption_entities else None
            break
    
    # Prepare caption for first media
    caption_kwargs = {}
    overflow_chunks = None
    
    if caption:
        cap_len = utf16_length(caption)
        
        if cap_len <= CAPTION_LIMIT:
            safe_entities = prepare_entities_for_send(caption, entities) if entities else None
            caption_kwargs['caption'] = caption
            if safe_entities:
                caption_kwargs['caption_entities'] = safe_entities
        else:
            cap_chunk, overflow_chunks = split_caption_with_entities(caption, entities or [])
            caption_kwargs['caption'] = cap_chunk.text
            if cap_chunk.entities:
                caption_kwargs['caption_entities'] = cap_chunk.entities
    
    # Build InputMedia list
    media_list = []
    for i, (msg, path) in enumerate(zip(album_messages, file_paths)):
        # Caption only on first media
        media_caption = caption_kwargs.get('caption') if i == 0 else None
        media_entities = caption_kwargs.get('caption_entities') if i == 0 else None
        
        input_media = _build_input_media(msg, path, media_caption, media_entities)
        if input_media:
            media_list.append(input_media)
    
    if not media_list:
        return CopyResult(success=False, messages=[], error="No valid media")
    
    # Send album
    sent = []
    try:
        album_msgs = await client.send_media_group(
            chat_id=target_chat_id,
            media=media_list,
            reply_to_message_id=reply_to_message_id
        )
        if album_msgs:
            sent.extend(album_msgs if isinstance(album_msgs, list) else [album_msgs])
    except TypeError as te:
        # Pyrofork 'topics' quirk - message likely sent
        if 'topics' in str(te).lower():
            logger.debug(f"Pyrofork album quirk (likely sent): {te}")
        else:
            return CopyResult(success=False, messages=[], error=str(te))
    except FloodWait as e:
        wait = min(getattr(e, 'value', getattr(e, 'x', 30)), 60)
        await asyncio.sleep(wait)
        album_msgs = await client.send_media_group(
            chat_id=target_chat_id,
            media=media_list
        )
        if album_msgs:
            sent.extend(album_msgs if isinstance(album_msgs, list) else [album_msgs])
    except Exception as e:
        return CopyResult(success=False, messages=[], error=str(e))
    
    # Send overflow
    if overflow_chunks and sent:
        for chunk in overflow_chunks:
            overflow_text = f"[Caption continued]\n\n{chunk.text}"
            header_len = utf16_length("[Caption continued]\n\n")
            adjusted = [clone_entity(e, header_len) for e in (chunk.entities or [])]
            adjusted = [e for e in adjusted if e]
            safe_entities = prepare_entities_for_send(overflow_text, adjusted)
            
            try:
                overflow_msg = await _send_text_safe(
                    client, target_chat_id, overflow_text, safe_entities, None
                )
                if overflow_msg:
                    sent.append(overflow_msg)
            except Exception:
                pass
    
    return CopyResult(
        success=len(sent) > 0,
        messages=sent,
        overflow_sent=overflow_chunks is not None
    )


def _build_input_media(
    msg: Message,
    file_path: str,
    caption: Optional[str],
    entities: Optional[List[MessageEntity]]
) -> Optional[Any]:
    """Build InputMedia* object for album."""
    kwargs = {'media': file_path}
    
    if caption:
        kwargs['caption'] = caption
        if entities:
            kwargs['caption_entities'] = entities
    
    if msg.photo:
        return InputMediaPhoto(**kwargs)
    elif msg.video:
        kwargs['duration'] = msg.video.duration or 0
        kwargs['width'] = msg.video.width or 0
        kwargs['height'] = msg.video.height or 0
        kwargs['supports_streaming'] = True
        return InputMediaVideo(**kwargs)
    elif msg.document:
        return InputMediaDocument(**kwargs)
    elif msg.audio:
        kwargs['duration'] = msg.audio.duration or 0
        kwargs['performer'] = msg.audio.performer
        kwargs['title'] = msg.audio.title
        return InputMediaAudio(**kwargs)
    
    return None


async def safe_forward_or_copy(
    client: Client,
    target_chat_id: int,
    source_msg: Message,
    file_path: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
    progress: Any = None
) -> CopyResult:
    """
    High-level function: Forward or copy any message type safely.
    
    Decision logic:
    1. Text message -> safe_copy_text_message
    2. Media message with file_path -> safe_copy_media_message
    3. Poll/Quiz -> convert to text and send
    
    This is the RECOMMENDED entry point for message copying.
    """
    # Text message
    if source_msg.text and not source_msg.media:
        return await safe_copy_text_message(
            client, target_chat_id, source_msg, reply_to_message_id
        )
    
    # Poll/Quiz
    if source_msg.poll:
        from core.message_utils import normalize_poll_to_text
        poll_text = normalize_poll_to_text(
            question=source_msg.poll.question,
            options=[opt.text for opt in source_msg.poll.options],
            correct_option_id=getattr(source_msg.poll, 'correct_option_id', None),
            explanation=getattr(source_msg.poll, 'explanation', None),
            is_quiz=source_msg.poll.type.name == 'QUIZ' if source_msg.poll.type else False
        )
        try:
            msg = await _send_text_safe(client, target_chat_id, poll_text, None, reply_to_message_id)
            return CopyResult(success=msg is not None, messages=[msg] if msg else [])
        except Exception as e:
            return CopyResult(success=False, messages=[], error=str(e))
    
    # Media message
    if source_msg.media and file_path:
        return await safe_copy_media_message(
            client, target_chat_id, source_msg, file_path, reply_to_message_id, progress
        )
    
    return CopyResult(success=False, messages=[], error="Unsupported message type or missing file")
