"""
Safe Message Sending for Pyrofork (MTProto Layer >= 220)

CRITICAL DESIGN RULES:

1. ENTITY VALIDATION:
   - ALL entities MUST be validated before sending
   - NEVER mix parse_mode with entities parameter
   - Use sanitize_entities() or prepare_entities_for_send() always

2. CAPTION/MESSAGE HANDLING:
   - If text/caption length <= limits: Markdown MAY be used
   - If text/caption exceeds limits: MUST use entity-aware splitting
   - After splitting: NEVER use Markdown, only entities

3. REPLY STRATEGY:
   - Attempt reply_to_message_id ONLY ONCE for single, unsplit messages
   - On ANY warning/exception, silently fall back to normal send
   - NEVER retry replies
   - NEVER force reply on split messages, reconnects, or background tasks

This eliminates ENTITY_BOUNDS_INVALID and Pyrofork reply warnings.
"""

import asyncio
import logging
from typing import Optional, Any, List, Union, Callable, Tuple
from functools import wraps

from event_loop_bootstrap import ensure_current_event_loop

ensure_current_event_loop()

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageIdInvalid, MessageNotModified

from core.message_utils import (
    split_text, split_caption, sanitize_markdown,
    MAX_MESSAGE_LENGTH, MAX_CAPTION_LENGTH
)
from core.entity_validator import (
    utf16_length, prepare_entities_for_send, sanitize_entities,
    split_with_entities, split_caption_with_entities,
    CAPTION_LIMIT, MESSAGE_LIMIT, SplitChunk
)

logger = logging.getLogger(__name__)


def get_parse_mode():
    """Get default parse mode (DISABLED to avoid markdown/entity splitting issues)."""
    try:
        from pyrogram.enums import ParseMode
        return ParseMode.DISABLED
    except (ImportError, AttributeError):
        return None


def get_disabled_mode():
    """Get disabled parse mode for fallback."""
    try:
        from pyrogram.enums import ParseMode
        return ParseMode.DISABLED
    except (ImportError, AttributeError):
        return None


def _normalize_parse_mode(parse_mode: Any):
    """
    Normalize parse_mode to project-safe value.

    Rule:
      - MARKDOWN / HTML (enum or string) -> DISABLED
      - None -> default get_parse_mode() (already DISABLED)
      - Others -> pass through
    """
    if parse_mode is None:
        return get_parse_mode()

    # Enum case (ParseMode.MARKDOWN / ParseMode.HTML)
    mode_name = getattr(parse_mode, "name", "")
    if mode_name in ("MARKDOWN", "HTML"):
        return get_disabled_mode()

    # String case ("markdown", "html")
    if isinstance(parse_mode, str) and parse_mode.strip().lower() in ("markdown", "html"):
        return get_disabled_mode()

    return parse_mode
async def safe_send_message(
    client: Client,
    chat_id: int,
    text: str,
    reply_to_message_id: Optional[int] = None,
    disable_web_page_preview: bool = True,
    reply_markup: Any = None,
    parse_mode: Any = None,
    **kwargs
) -> Optional[Union[Message, List[Message]]]:
    """
    Send message with SILENT FALLBACK reply strategy.
    
    BEHAVIOR:
    1. For single messages (<=4096 chars):
       - Attempt with reply_to_message_id ONCE
       - On ANY error/warning, retry WITHOUT reply (silent fallback)
    
    2. For split messages (>4096 chars):
       - First chunk: attempt reply ONCE, fallback to no-reply
       - Subsequent chunks: NEVER use reply
    
    3. Never raise exceptions for reply failures
    4. Never log noisy warnings for reply fallbacks
    
    Args:
        client: Pyrogram Client
        chat_id: Target chat ID
        text: Message text
        reply_to_message_id: Optional reply target (best-effort, may be ignored)
        disable_web_page_preview: Disable link preview
        reply_markup: Optional inline keyboard
        parse_mode: Parse mode (default: Markdown)
        **kwargs: Additional send_message parameters
    
    Returns:
        Message or list of Messages on success, None on failure
    """
    if not text:
        return None
    
    parse_mode = _normalize_parse_mode(parse_mode)
    
    # Check if splitting is needed
    needs_split = utf16_length(text) > MAX_MESSAGE_LENGTH
    
    if needs_split:
        return await _send_split_message(
            client, chat_id, text, reply_to_message_id,
            disable_web_page_preview, reply_markup, parse_mode, kwargs
        )
    else:
        return await _send_single_message(
            client, chat_id, text, reply_to_message_id,
            disable_web_page_preview, reply_markup, parse_mode, kwargs
        )


async def _send_single_message(
    client: Client,
    chat_id: int,
    text: str,
    reply_to_message_id: Optional[int],
    disable_web_page_preview: bool,
    reply_markup: Any,
    parse_mode: Any,
    extra_kwargs: dict
) -> Optional[Message]:
    """Send single message with silent reply fallback."""
    
    # Build base kwargs
    base_kwargs = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
        **extra_kwargs
    }
    
    if reply_markup:
        base_kwargs["reply_markup"] = reply_markup
    
    # ATTEMPT 1: With reply (if provided)
    if reply_to_message_id:
        try:
            return await client.send_message(
                **base_kwargs,
                reply_to_message_id=reply_to_message_id
            )
        except Exception:
            # SILENT FALLBACK - no logging, just retry without reply
            pass
    
    # ATTEMPT 2: Without reply (or if no reply was requested)
    try:
        return await client.send_message(**base_kwargs)
    except Exception as e:
        # Check if parse error - try plain text
        if _is_parse_error(e):
            try:
                base_kwargs["parse_mode"] = get_disabled_mode()
                return await client.send_message(**base_kwargs)
            except Exception:
                pass
        
        logger.debug(f"send_message failed: {e}")
        return None


async def _send_split_message(
    client: Client,
    chat_id: int,
    text: str,
    reply_to_message_id: Optional[int],
    disable_web_page_preview: bool,
    reply_markup: Any,
    parse_mode: Any,
    extra_kwargs: dict
) -> Optional[List[Message]]:
    """
    Send split message - reply ONLY on first chunk, with fallback.
    
    Uses entity-aware splitting from entity_splitter to guarantee
    hyperlinks and formatting are never broken across chunks.
    When the text is plain (no entities), uses smart UTF-16 splitting
    with word-boundary awareness.
    """
    from core.entity_splitter import split_text_with_entities, utf16_len

    # Try entity-aware split first (handles both entity and plain text)
    # Pass empty entities list — the splitter still respects UTF-16 
    # boundaries and finds safe split points (newlines, word breaks)
    chunks = split_text_with_entities(text, [], MESSAGE_LIMIT)

    if not chunks:
        return None

    results = []

    for i, chunk in enumerate(chunks):
        # Only first chunk gets reply_to and reply_markup
        chunk_reply_id = reply_to_message_id if i == 0 else None
        chunk_markup = reply_markup if i == 0 else None

        msg = await _send_single_message(
            client, chat_id, chunk.text, chunk_reply_id,
            disable_web_page_preview, chunk_markup, parse_mode, extra_kwargs
        )

        if msg:
            results.append(msg)

        # Small delay between chunks to avoid rate limits
        if i < len(chunks) - 1:
            await asyncio.sleep(0.5)

    return results if results else None


async def safe_reply(
    message: Message,
    text: str,
    quote: bool = True,
    disable_web_page_preview: bool = True,
    reply_markup: Any = None,
    parse_mode: Any = None,
    **kwargs
) -> Optional[Union[Message, List[Message]]]:
    """
    Reply to message with SILENT FALLBACK strategy.
    
    BEHAVIOR:
    1. For single messages: attempt reply ONCE, fallback to normal send
    2. For split messages: first chunk attempts reply, others don't
    3. Never raises exceptions for reply failures
    
    Args:
        message: Message to reply to
        text: Reply text
        quote: Whether to quote (reply to) the message
        disable_web_page_preview: Disable link preview
        reply_markup: Optional inline keyboard
        parse_mode: Parse mode (default: Markdown)
        **kwargs: Additional parameters
    
    Returns:
        Message(s) on success, None on failure
    """
    if not text:
        return None
    
    parse_mode = _normalize_parse_mode(parse_mode)
    
    # Determine reply_to_message_id
    reply_to_id = message.id if quote else None
    
    # Check if splitting needed
    needs_split = utf16_length(text) > MAX_MESSAGE_LENGTH
    
    if needs_split:
        chunks = split_text(text, MAX_MESSAGE_LENGTH)
        results = []
        
        for i, chunk in enumerate(chunks):
            # Only first chunk gets reply
            should_quote = quote if i == 0 else False
            chunk_markup = reply_markup if i == 0 else None
            
            msg = await _reply_single(
                message, chunk, should_quote, disable_web_page_preview,
                chunk_markup, parse_mode, kwargs
            )
            
            if msg:
                results.append(msg)
            
            if i < len(chunks) - 1:
                await asyncio.sleep(0.5)
        
        return results if results else None
    else:
        return await _reply_single(
            message, text, quote, disable_web_page_preview,
            reply_markup, parse_mode, kwargs
        )


async def _reply_single(
    message: Message,
    text: str,
    quote: bool,
    disable_web_page_preview: bool,
    reply_markup: Any,
    parse_mode: Any,
    extra_kwargs: dict
) -> Optional[Message]:
    """Reply to single message with silent fallback."""
    
    base_kwargs = {
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
        **extra_kwargs
    }
    
    if reply_markup:
        base_kwargs["reply_markup"] = reply_markup
    
    # ATTEMPT 1: With quote
    if quote:
        try:
            return await message.reply(**base_kwargs, quote=True)
        except FloodWait as e:
            # PATCH 5: FloodWait safe handling — pause then retry once
            wait_time = getattr(e, 'value', getattr(e, 'x', 30))
            logger.warning(f"FloodWaitHandled: {wait_time}s in reply (with quote)")
            await asyncio.sleep(min(wait_time, 300))
            try:
                return await message.reply(**base_kwargs, quote=True)
            except Exception:
                pass
        except Exception:
            # SILENT FALLBACK
            pass
    
    # ATTEMPT 2: Without quote (or fallback)
    try:
        return await message.reply(**base_kwargs, quote=False)
    except FloodWait as e:
        # PATCH 5: FloodWait safe handling — pause then retry once
        wait_time = getattr(e, 'value', getattr(e, 'x', 30))
        logger.warning(f"FloodWaitHandled: {wait_time}s in reply (no quote)")
        await asyncio.sleep(min(wait_time, 300))
        try:
            return await message.reply(**base_kwargs, quote=False)
        except Exception:
            pass
    except Exception as e:
        if _is_parse_error(e):
            try:
                base_kwargs["parse_mode"] = get_disabled_mode()
                return await message.reply(**base_kwargs, quote=False)
            except Exception:
                pass
        
        logger.debug(f"reply failed: {e}")
        return None


async def safe_edit_message(
    client_or_message: Union[Client, Message],
    chat_id: Optional[int] = None,
    message_id: Optional[int] = None,
    text: str = "",
    parse_mode: Any = None,
    **kwargs
) -> Optional[Message]:
    """
    Edit message with automatic parse_mode fallback.
    
    Can be called with:
    - Message object: safe_edit_message(message, text="new text")
    - Client + IDs: safe_edit_message(client, chat_id, message_id, text="new text")
    
    Returns:
        Edited Message on success, None on failure/no change
    """
    parse_mode = _normalize_parse_mode(parse_mode)
    
    is_message_obj = hasattr(client_or_message, 'edit')
    
    try:
        if is_message_obj:
            return await client_or_message.edit(text=text, parse_mode=parse_mode, **kwargs)
        else:
            return await client_or_message.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode=parse_mode,
                **kwargs
            )
    except MessageNotModified:
        return None
    except MessageIdInvalid:
        return None
    except FloodWait as e:
        # PATCH 5: FloodWait safe handling — pause then retry once
        wait_time = getattr(e, 'value', getattr(e, 'x', 30))
        logger.warning(f"FloodWaitHandled: {wait_time}s in edit_message")
        await asyncio.sleep(min(wait_time, 300))
        try:
            if is_message_obj:
                return await client_or_message.edit(text=text, parse_mode=parse_mode, **kwargs)
            else:
                return await client_or_message.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=text, parse_mode=parse_mode, **kwargs
                )
        except Exception:
            return None
    except Exception as e:
        if _is_parse_error(e):
            try:
                if is_message_obj:
                    return await client_or_message.edit(
                        text=text, parse_mode=get_disabled_mode(), **kwargs
                    )
                else:
                    return await client_or_message.edit_message_text(
                        chat_id=chat_id, message_id=message_id,
                        text=text, parse_mode=get_disabled_mode(), **kwargs
                    )
            except Exception:
                pass
        
        logger.debug(f"edit failed: {e}")
        return None


async def safe_send_photo(
    client: Client,
    chat_id: int,
    photo: Any,
    caption: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
    reply_markup: Any = None,
    parse_mode: Any = None,
    **kwargs
) -> Optional[Message]:
    """
    Send photo with safe caption handling and silent reply fallback.
    
    Handles:
    - Caption splitting (1024 char limit)
    - Reply fallback
    - Parse mode fallback
    
    Returns:
        Tuple of (photo_message, overflow_message) or (photo_message, None)
    """
    parse_mode = _normalize_parse_mode(parse_mode)
    
    # Handle caption splitting
    media_caption, overflow = split_caption(caption) if caption else (None, None)
    
    base_kwargs = {
        "chat_id": chat_id,
        "photo": photo,
        "parse_mode": parse_mode,
        **kwargs
    }
    
    if media_caption:
        base_kwargs["caption"] = media_caption
    if reply_markup:
        base_kwargs["reply_markup"] = reply_markup
    
    # ATTEMPT 1: With reply
    photo_msg = None
    if reply_to_message_id:
        try:
            photo_msg = await client.send_photo(
                **base_kwargs,
                reply_to_message_id=reply_to_message_id
            )
        except Exception:
            pass
    
    # ATTEMPT 2: Without reply (or fallback)
    if not photo_msg:
        try:
            photo_msg = await client.send_photo(**base_kwargs)
        except Exception as e:
            if _is_parse_error(e):
                try:
                    base_kwargs["parse_mode"] = get_disabled_mode()
                    photo_msg = await client.send_photo(**base_kwargs)
                except Exception:
                    pass
            
            if not photo_msg:
                logger.debug(f"send_photo failed: {e}")
                return None
    
    # Send overflow caption as separate message
    if overflow and photo_msg:
        await safe_send_message(
            client, chat_id,
            f"**[Caption continued]**\n\n{overflow}",
            parse_mode=parse_mode
        )
    
    return photo_msg


async def safe_send_video(
    client: Client,
    chat_id: int,
    video: Any,
    caption: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
    reply_markup: Any = None,
    parse_mode: Any = None,
    progress: Callable = None,
    **kwargs
) -> Optional[Message]:
    """Send video with safe caption handling and silent reply fallback."""
    parse_mode = _normalize_parse_mode(parse_mode)
    
    media_caption, overflow = split_caption(caption) if caption else (None, None)
    
    base_kwargs = {
        "chat_id": chat_id,
        "video": video,
        "parse_mode": parse_mode,
        **kwargs
    }
    
    if media_caption:
        base_kwargs["caption"] = media_caption
    if reply_markup:
        base_kwargs["reply_markup"] = reply_markup
    if progress:
        base_kwargs["progress"] = progress
    
    video_msg = None
    if reply_to_message_id:
        try:
            video_msg = await client.send_video(
                **base_kwargs,
                reply_to_message_id=reply_to_message_id
            )
        except Exception:
            pass
    
    if not video_msg:
        try:
            video_msg = await client.send_video(**base_kwargs)
        except Exception as e:
            if _is_parse_error(e):
                try:
                    base_kwargs["parse_mode"] = get_disabled_mode()
                    video_msg = await client.send_video(**base_kwargs)
                except Exception:
                    pass
            
            if not video_msg:
                logger.debug(f"send_video failed: {e}")
                return None
    
    if overflow and video_msg:
        await safe_send_message(client, chat_id, f"**[Caption continued]**\n\n{overflow}")
    
    return video_msg


async def safe_send_document(
    client: Client,
    chat_id: int,
    document: Any,
    caption: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
    reply_markup: Any = None,
    parse_mode: Any = None,
    progress: Callable = None,
    **kwargs
) -> Optional[Message]:
    """Send document with safe caption handling and silent reply fallback."""
    parse_mode = _normalize_parse_mode(parse_mode)
    
    media_caption, overflow = split_caption(caption) if caption else (None, None)
    
    base_kwargs = {
        "chat_id": chat_id,
        "document": document,
        "parse_mode": parse_mode,
        **kwargs
    }
    
    if media_caption:
        base_kwargs["caption"] = media_caption
    if reply_markup:
        base_kwargs["reply_markup"] = reply_markup
    if progress:
        base_kwargs["progress"] = progress
    
    doc_msg = None
    if reply_to_message_id:
        try:
            doc_msg = await client.send_document(
                **base_kwargs,
                reply_to_message_id=reply_to_message_id
            )
        except Exception:
            pass
    
    if not doc_msg:
        try:
            doc_msg = await client.send_document(**base_kwargs)
        except Exception as e:
            if _is_parse_error(e):
                try:
                    base_kwargs["parse_mode"] = get_disabled_mode()
                    doc_msg = await client.send_document(**base_kwargs)
                except Exception:
                    pass
            
            if not doc_msg:
                logger.debug(f"send_document failed: {e}")
                return None
    
    if overflow and doc_msg:
        await safe_send_message(client, chat_id, f"**[Caption continued]**\n\n{overflow}")
    
    return doc_msg


async def safe_send_audio(
    client: Client,
    chat_id: int,
    audio: Any,
    caption: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
    parse_mode: Any = None,
    **kwargs
) -> Optional[Message]:
    """Send audio with safe caption handling and silent reply fallback."""
    parse_mode = _normalize_parse_mode(parse_mode)
    
    media_caption, overflow = split_caption(caption) if caption else (None, None)
    
    base_kwargs = {"chat_id": chat_id, "audio": audio, "parse_mode": parse_mode, **kwargs}
    if media_caption:
        base_kwargs["caption"] = media_caption
    
    audio_msg = None
    if reply_to_message_id:
        try:
            audio_msg = await client.send_audio(**base_kwargs, reply_to_message_id=reply_to_message_id)
        except Exception:
            pass
    
    if not audio_msg:
        try:
            audio_msg = await client.send_audio(**base_kwargs)
        except Exception as e:
            if _is_parse_error(e):
                try:
                    base_kwargs["parse_mode"] = get_disabled_mode()
                    audio_msg = await client.send_audio(**base_kwargs)
                except Exception:
                    pass
            if not audio_msg:
                return None
    
    if overflow and audio_msg:
        await safe_send_message(client, chat_id, f"**[Caption continued]**\n\n{overflow}")
    
    return audio_msg


def _is_parse_error(e: Exception) -> bool:
    """Check if exception is a parse/formatting error."""
    error_str = str(e).lower()
    parse_indicators = ['parse', 'entity', 'markdown', 'tag', "can't", 'invalid']
    return any(ind in error_str for ind in parse_indicators)


def safe_handler(func):
    """
    Decorator that wraps handlers to prevent dispatcher crashes.
    
    - Catches all exceptions and logs them
    - Never crashes the dispatcher
    - Handles FloodWait gracefully
    
    Usage:
        @app.on_message(filters.command("test"))
        @safe_handler
        async def test_handler(client, message):
            ...
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except FloodWait as e:
            wait_time = getattr(e, 'value', getattr(e, 'x', 30))
            logger.warning(f"Handler {func.__name__} FloodWait: {wait_time}s")
            await asyncio.sleep(min(wait_time, 60))
        except Exception as e:
            logger.error(f"Handler {func.__name__} error: {e}", exc_info=True)
        return None
    
    return wrapper


# ==================== ENTITY-SAFE SEND FUNCTIONS ====================
# These functions NEVER mix parse_mode with entities, and always validate.


async def entity_safe_send_message(
    client: Client,
    chat_id: int,
    text: str,
    entities: Optional[List] = None,
    reply_to_message_id: Optional[int] = None,
    reply_markup: Any = None,
    disable_web_page_preview: bool = True,
    **kwargs
) -> Optional[Union[Message, List[Message]]]:
    """
    Send message with VALIDATED entities (never uses parse_mode).
    
    CRITICAL: This is the SAFE way to send messages with entities.
    - Validates all entities before sending
    - Splits long messages with entity offset recalculation
    - Never mixes parse_mode with entities
    
    Args:
        client: Pyrogram Client
        chat_id: Target chat ID
        text: Message text
        entities: List of MessageEntity objects
        reply_to_message_id: Optional reply target
        reply_markup: Optional inline keyboard
        disable_web_page_preview: Disable link preview
    
    Returns:
        Message or list of Messages on success, None on failure
    """
    if not text:
        return None
    
    text_utf16 = utf16_length(text)
    
    # No split needed - validate and send
    if text_utf16 <= MESSAGE_LIMIT:
        safe_entities = prepare_entities_for_send(text, entities) if entities else None
        return await _send_single_entity_message(
            client, chat_id, text, safe_entities,
            reply_to_message_id, reply_markup, disable_web_page_preview, kwargs
        )
    
    # Split needed - use entity-aware splitting
    chunks = split_with_entities(text, entities or [], MESSAGE_LIMIT)
    if not chunks:
        return None
    
    results = []
    for i, chunk in enumerate(chunks):
        # Only first chunk gets reply_to and markup
        reply_id = reply_to_message_id if i == 0 else None
        markup = reply_markup if i == 0 else None
        
        msg = await _send_single_entity_message(
            client, chat_id, chunk.text, chunk.entities or None,
            reply_id, markup, disable_web_page_preview, kwargs
        )
        
        if msg:
            results.append(msg)
        
        # Small delay between chunks
        if i < len(chunks) - 1:
            await asyncio.sleep(0.3)
    
    return results if results else None


async def _send_single_entity_message(
    client: Client,
    chat_id: int,
    text: str,
    entities: Optional[List],
    reply_to_message_id: Optional[int],
    reply_markup: Any,
    disable_web_page_preview: bool,
    extra_kwargs: dict
) -> Optional[Message]:
    """Send single message with entities (internal helper)."""
    
    base_kwargs = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
        **extra_kwargs
    }
    
    # CRITICAL: Only add entities if valid, NEVER add parse_mode
    if entities:
        base_kwargs["entities"] = entities
    
    if reply_markup:
        base_kwargs["reply_markup"] = reply_markup
    
    # Attempt with reply
    if reply_to_message_id:
        try:
            return await client.send_message(
                **base_kwargs,
                reply_to_message_id=reply_to_message_id
            )
        except Exception:
            pass  # Silent fallback
    
    # Without reply
    try:
        return await client.send_message(**base_kwargs)
    except Exception as e:
        # If entity error, try plain text
        if _is_entity_error(e):
            try:
                del base_kwargs["entities"]
                return await client.send_message(**base_kwargs)
            except Exception:
                pass
        logger.debug(f"entity_safe_send failed: {e}")
        return None


async def entity_safe_send_photo(
    client: Client,
    chat_id: int,
    photo: Any,
    caption: Optional[str] = None,
    caption_entities: Optional[List] = None,
    reply_to_message_id: Optional[int] = None,
    reply_markup: Any = None,
    **kwargs
) -> Tuple[Optional[Message], Optional[Message]]:
    """
    Send photo with entity-safe caption handling.
    
    CRITICAL:
    - Never uses parse_mode when entities are provided
    - Splits long captions with entity recalculation
    - Overflow goes as follow-up message
    
    Returns:
        (photo_message, overflow_message or None)
    """
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
    
    base_kwargs = {
        "chat_id": chat_id,
        "photo": photo,
        **caption_kwargs,
        **kwargs
    }
    
    if reply_markup:
        base_kwargs["reply_markup"] = reply_markup
    
    # Attempt with reply
    photo_msg = None
    if reply_to_message_id:
        try:
            photo_msg = await client.send_photo(
                **base_kwargs,
                reply_to_message_id=reply_to_message_id
            )
        except Exception:
            pass
    
    if not photo_msg:
        try:
            photo_msg = await client.send_photo(**base_kwargs)
        except Exception as e:
            if _is_entity_error(e) and 'caption_entities' in caption_kwargs:
                try:
                    del base_kwargs['caption_entities']
                    photo_msg = await client.send_photo(**base_kwargs)
                except Exception:
                    pass
            if not photo_msg:
                logger.debug(f"entity_safe_send_photo failed: {e}")
                return None, None
    
    # Send overflow
    overflow_msg = None
    if overflow_chunks and photo_msg:
        for chunk in overflow_chunks:
            overflow_msg = await entity_safe_send_message(
                client, chat_id,
                f"[Caption continued]\n\n{chunk.text}",
                entities=_adjust_entities_for_header(chunk.entities, "[Caption continued]\n\n")
            )
    
    return photo_msg, overflow_msg


async def entity_safe_send_video(
    client: Client,
    chat_id: int,
    video: Any,
    caption: Optional[str] = None,
    caption_entities: Optional[List] = None,
    reply_to_message_id: Optional[int] = None,
    reply_markup: Any = None,
    progress: Callable = None,
    **kwargs
) -> Tuple[Optional[Message], Optional[Message]]:
    """Send video with entity-safe caption handling."""
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
    
    base_kwargs = {
        "chat_id": chat_id,
        "video": video,
        **caption_kwargs,
        **kwargs
    }
    
    if reply_markup:
        base_kwargs["reply_markup"] = reply_markup
    if progress:
        base_kwargs["progress"] = progress
    
    video_msg = None
    if reply_to_message_id:
        try:
            video_msg = await client.send_video(
                **base_kwargs,
                reply_to_message_id=reply_to_message_id
            )
        except Exception:
            pass
    
    if not video_msg:
        try:
            video_msg = await client.send_video(**base_kwargs)
        except Exception as e:
            if _is_entity_error(e) and 'caption_entities' in caption_kwargs:
                try:
                    del base_kwargs['caption_entities']
                    video_msg = await client.send_video(**base_kwargs)
                except Exception:
                    pass
            if not video_msg:
                logger.debug(f"entity_safe_send_video failed: {e}")
                return None, None
    
    # Send overflow
    overflow_msg = None
    if overflow_chunks and video_msg:
        for chunk in overflow_chunks:
            overflow_msg = await entity_safe_send_message(
                client, chat_id,
                f"[Caption continued]\n\n{chunk.text}",
                entities=_adjust_entities_for_header(chunk.entities, "[Caption continued]\n\n")
            )
    
    return video_msg, overflow_msg


async def entity_safe_send_document(
    client: Client,
    chat_id: int,
    document: Any,
    caption: Optional[str] = None,
    caption_entities: Optional[List] = None,
    reply_to_message_id: Optional[int] = None,
    reply_markup: Any = None,
    progress: Callable = None,
    **kwargs
) -> Tuple[Optional[Message], Optional[Message]]:
    """Send document with entity-safe caption handling."""
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
    
    base_kwargs = {
        "chat_id": chat_id,
        "document": document,
        **caption_kwargs,
        **kwargs
    }
    
    if reply_markup:
        base_kwargs["reply_markup"] = reply_markup
    if progress:
        base_kwargs["progress"] = progress
    
    doc_msg = None
    if reply_to_message_id:
        try:
            doc_msg = await client.send_document(
                **base_kwargs,
                reply_to_message_id=reply_to_message_id
            )
        except Exception:
            pass
    
    if not doc_msg:
        try:
            doc_msg = await client.send_document(**base_kwargs)
        except Exception as e:
            if _is_entity_error(e) and 'caption_entities' in caption_kwargs:
                try:
                    del base_kwargs['caption_entities']
                    doc_msg = await client.send_document(**base_kwargs)
                except Exception:
                    pass
            if not doc_msg:
                logger.debug(f"entity_safe_send_document failed: {e}")
                return None, None
    
    # Send overflow
    overflow_msg = None
    if overflow_chunks and doc_msg:
        for chunk in overflow_chunks:
            overflow_msg = await entity_safe_send_message(
                client, chat_id,
                f"[Caption continued]\n\n{chunk.text}",
                entities=_adjust_entities_for_header(chunk.entities, "[Caption continued]\n\n")
            )
    
    return doc_msg, overflow_msg


def _is_entity_error(e: Exception) -> bool:
    """Check if exception is specifically an entity-related error."""
    error_str = str(e).upper()
    entity_indicators = ['ENTITY', 'BOUNDS', 'INVALID', 'OFFSET', 'LENGTH']
    return any(ind in error_str for ind in entity_indicators)


def _adjust_entities_for_header(
    entities: Optional[List],
    header: str
) -> Optional[List]:
    """Adjust entity offsets after prepending header text."""
    if not entities:
        return None
    
    header_utf16 = utf16_length(header)
    adjusted = []
    
    for e in entities:
        try:
            from pyrogram.types import MessageEntity
            new_entity = MessageEntity(
                type=e.type,
                offset=e.offset + header_utf16,
                length=e.length,
                url=getattr(e, 'url', None),
                user=getattr(e, 'user', None),
                language=getattr(e, 'language', None),
            )
            adjusted.append(new_entity)
        except Exception:
            pass
    
    return adjusted if adjusted else None
