"""
core/repost_router.py - Dynamic Premium→Non-Premium repost pipeline.

AUTO-DETECTED WORKFLOW MODES:
  MODE A — User IS Telegram Premium → direct copy via user session
  MODE B — System Premium session exists → relay via system premium session
  MODE C — Full Non-Premium Fallback → strip custom emoji, rebuild entities,
            hyperlink-safe UTF-16 split, caption overflow as replies

ARCHITECTURE RULES (enforced):
  ✅ MTProto (Pyrogram user sessions only) for all uploads
  ✅ UTF-16 entity math everywhere
  ✅ Entity reconstruction (fresh MessageEntity per chunk)
  ✅ Runtime session switching (hot-load, no restart)
  ❌ No Bot API for upload
  ❌ No copy_message in non-premium mode
  ❌ No Markdown re-parsing
  ❌ No raw entity reuse
  ❌ No UTF-8 length logic
"""

import os
import asyncio
import time
import uuid
import logging
from typing import Optional, List, Tuple, Any

from pyrogram import Client
from pyrogram.types import Message, MessageEntity
from pyrogram.enums import MessageEntityType
from pyrogram.errors import (
    FloodWait, AuthKeyUnregistered, AuthKeyInvalid,
    SessionRevoked, SessionExpired, UserDeactivated,
    RPCError, Timeout,
)

from config import API_ID, API_HASH, get_client_params
from core.entity_splitter import (
    utf16_len,
    utf16_to_char_index,
    char_to_utf16_offset,
    build_entity_spans,
    clone_entity_with_offset,
    TextChunk,
    CAPTION_LIMIT,
    MESSAGE_LIMIT,
)
from core.entity_rebuilder import (
    strip_custom_emoji_entities as _strip_custom_emoji_entities,
    validate_entities as _validate_entities_rebuilder,
)
from core.premium_logic import (
    check_user_premium,
    get_system_session,
    has_system_premium,
    set_system_session as _set_system_session,
    remove_system_session,
    get_user_upload_setting,
    UPLOAD_AUTO,
    UPLOAD_FORCE_SPLIT,
    UPLOAD_NO_SPLIT,
    get_caption_limit,
    SystemPremiumSession,
)

logger = logging.getLogger(__name__)

# Fatal session errors that mean the session is permanently dead
_FATAL_ERRORS = (
    AuthKeyUnregistered, AuthKeyInvalid,
    SessionRevoked, SessionExpired, UserDeactivated,
)

# Per-session rate limiters: session_id -> asyncio.Lock
_session_locks: dict = {}

# Per-session flood cooldown: id(client) -> (cooldown_until_monotonic, wait_seconds)
_flood_cooldowns: dict = {}


def _get_session_lock(session_id: str) -> asyncio.Lock:
    """Get or create a per-session rate-limit lock."""
    if session_id not in _session_locks:
        _session_locks[session_id] = asyncio.Lock()
    return _session_locks[session_id]


# ==================== CUSTOM EMOJI STRIPPING ====================

def strip_custom_emoji(
    text: str,
    entities: Optional[List[MessageEntity]],
) -> Tuple[str, List[MessageEntity]]:
    """
    Remove ALL CUSTOM_EMOJI entity overlays and rebuild remaining entity offsets.

    TEXT IS NOT MODIFIED — fallback Unicode glyphs already present in *text*
    are preserved. Only the CUSTOM_EMOJI MessageEntity objects are dropped and
    all other entity offsets are recomputed in UTF-16 units using
    core.entity_rebuilder (the canonical implementation).

    Returns:
        (text_unchanged, rebuilt_entities_without_custom_emoji)
    """
    # Delegate entirely to entity_rebuilder which has the correct UTF-16 math
    return _strip_custom_emoji_entities(text, entities)


# ==================== HYPERLINK-SAFE SPLITTING ====================

def split_text_respecting_hyperlinks(
    text: str,
    entities: Optional[List[MessageEntity]],
    max_len: int = CAPTION_LIMIT,
) -> List[TextChunk]:
    """
    Split text into UTF-16-limited chunks without breaking hyperlinks.
    
    RULES:
    1. Never split inside TEXT_LINK or URL entities
    2. If split point hits a hyperlink, extend chunk to hyperlink end
    3. Recalculate entity offsets per chunk (UTF-16 based)
    4. CUSTOM_EMOJI are completely removed, not replaced
    
    Args:
        text: Raw text to split
        entities: Original MessageEntity list
        max_len: Maximum UTF-16 code units per chunk
    
    Returns:
        List of TextChunk with fresh, recalculated entities
    """
    if not text:
        return []

    # Step 1: Strip custom emoji
    text, entities = strip_custom_emoji(text, entities)

    if not text.strip():
        return []

    text_utf16 = utf16_len(text)

    # No split needed
    if text_utf16 <= max_len:
        clean_ents = _reconstruct_entities(text, entities, 0, text)
        return [TextChunk(text=text, entities=clean_ents)]

    # Step 2: Build entity spans for hyperlink detection
    spans = build_entity_spans(text, entities or [])

    # Identify hyperlink spans (TEXT_LINK and URL entities)
    hyperlink_ranges = []
    for span in spans:
        if span.entity.type in (MessageEntityType.TEXT_LINK, MessageEntityType.URL):
            hyperlink_ranges.append((span.start_utf16, span.end_utf16, span.start_char, span.end_char))

    # Step 3: Split with hyperlink awareness
    chunks = []
    remaining_text = text
    remaining_entities = list(entities) if entities else []
    global_char_offset = 0  # Track where we are in original text

    while remaining_text:
        remaining_utf16 = utf16_len(remaining_text)

        if remaining_utf16 <= max_len:
            # Last chunk
            chunk_ents = _reconstruct_entities(text, remaining_entities, global_char_offset, remaining_text)
            chunks.append(TextChunk(text=remaining_text, entities=chunk_ents))
            break

        # Find split point
        split_char = utf16_to_char_index(remaining_text, max_len)

        # Check if split point is inside a hyperlink
        split_utf16_global = char_to_utf16_offset(text, global_char_offset + split_char)

        for h_start, h_end, h_start_char, h_end_char in hyperlink_ranges:
            if h_start < split_utf16_global < h_end:
                # Split point inside hyperlink — extend to hyperlink end
                extended_char = h_end_char - global_char_offset
                if extended_char > 0 and extended_char <= len(remaining_text):
                    split_char = extended_char
                break

        # Try to find a good word boundary near split point (but not inside hyperlinks)
        best_split = split_char
        search_start = max(0, split_char - split_char // 3)

        for sep in ['\n\n', '\n', ' ', '.', ',']:
            pos = remaining_text.rfind(sep, search_start, split_char)
            if pos > 0:
                candidate_pos = pos + len(sep) if sep in ['\n\n', '\n', ' '] else pos + 1
                # Verify not inside hyperlink
                candidate_utf16_global = char_to_utf16_offset(text, global_char_offset + candidate_pos)
                inside_link = False
                for h_start, h_end, _, _ in hyperlink_ranges:
                    if h_start < candidate_utf16_global < h_end:
                        inside_link = True
                        break
                if not inside_link:
                    best_split = candidate_pos
                    break

        if best_split <= 0:
            best_split = split_char
        if best_split <= 0:
            best_split = 1  # Force progress

        chunk_text = remaining_text[:best_split]

        # Reconstruct entities for the rstripped chunk text so entity
        # bounds match what is actually stored in the TextChunk.
        chunk_stripped = chunk_text.rstrip()
        if chunk_stripped:
            chunk_ents = _reconstruct_entities(
                text, remaining_entities, global_char_offset, chunk_stripped,
            )
            chunks.append(TextChunk(text=chunk_stripped, entities=chunk_ents))

        # Advance position: track how many chars lstrip() consumed so
        # global_char_offset stays in sync with the original text.
        after_split = remaining_text[best_split:]
        stripped_remaining = after_split.lstrip()
        whitespace_consumed = len(after_split) - len(stripped_remaining)
        global_char_offset += best_split + whitespace_consumed
        remaining_text = stripped_remaining

    return chunks if chunks else [TextChunk(text=text[:utf16_to_char_index(text, max_len)], entities=[])]


def _reconstruct_entities(
    original_text: str,
    entities: List[MessageEntity],
    chunk_start_char: int,
    chunk_text: str,
) -> List[MessageEntity]:
    """
    Reconstruct fresh MessageEntity objects for a text chunk.
    
    Uses UTF-16 math exclusively:
      new_offset = utf16_len(chunk[:position])
      new_length = utf16_len(entity_text)
    
    Creates FRESH MessageEntity — never reuses originals.
    """
    if not entities:
        return []

    chunk_utf16_len = utf16_len(chunk_text)
    chunk_start_utf16 = char_to_utf16_offset(original_text, chunk_start_char)
    chunk_end_utf16 = chunk_start_utf16 + chunk_utf16_len
    chunk_end_char = chunk_start_char + len(chunk_text)

    new_entities = []
    for e in entities:
        ent_start_utf16 = e.offset
        ent_end_utf16 = e.offset + e.length

        # Skip entities completely outside this chunk
        if ent_end_utf16 <= chunk_start_utf16 or ent_start_utf16 >= chunk_end_utf16:
            continue

        # Skip CUSTOM_EMOJI (should already be stripped, but safety check)
        if e.type == MessageEntityType.CUSTOM_EMOJI:
            continue

        # Calculate new offset within chunk
        new_offset = max(0, ent_start_utf16 - chunk_start_utf16)

        # Calculate length (clamp to chunk boundary)
        new_end = min(ent_end_utf16, chunk_end_utf16) - chunk_start_utf16
        new_length = new_end - new_offset

        if new_length <= 0:
            continue

        # Create FRESH entity (try/except for unknown entity types like WEB_PAGE)
        try:
            new_entity = MessageEntity(
                type=e.type,
                offset=new_offset,
                length=new_length,
                url=getattr(e, 'url', None),
                user=getattr(e, 'user', None),
                language=getattr(e, 'language', None),
                custom_emoji_id=None,
            )
        except Exception as exc:
            logger.debug("Failed to construct entity type=%s: %s", e.type, exc)
            continue

        # Final bounds validation
        if new_entity.offset >= 0 and new_entity.offset + new_entity.length <= chunk_utf16_len:
            new_entities.append(new_entity)

    return new_entities


# ==================== SAFE NON-PREMIUM UPLOAD ====================

async def safe_non_premium_upload(
    client: Client,
    original: Message,
    target_chat: int,
    reply_to_message_id: Optional[int] = None,
    file_path: Optional[str] = None,
    progress: Any = None,
) -> List[Message]:
    """
    Full non-premium repost: strip custom emoji, rebuild entities,
    hyperlink-safe split, and send with caption overflow.
    
    For media messages:
      1. Send media with first caption chunk (≤1024 UTF-16)
      2. Send remaining chunks as reply chain with "📌 Davomi X/N" headers
    
    For text messages:
      1. Split text into ≤4096 UTF-16 chunks
      2. Send as chained messages
    
    NEVER uses copy_message. NEVER uses parse_mode.
    """
    sent_messages = []

    # Extract content
    if original.text:
        text = original.text
        entities = list(original.entities) if original.entities else []
        # Use attribute-based media detection — never trust original.media enum
        # (WEB_PAGE previews must NOT go through downloader pipeline)
        from core.media_classifier import has_downloadable_media
        is_media = has_downloadable_media(original)
    elif original.caption:
        text = original.caption
        entities = list(original.caption_entities) if original.caption_entities else []
        from core.media_classifier import has_downloadable_media
        is_media = has_downloadable_media(original)
    else:
        text = ""
        entities = []
        from core.media_classifier import has_downloadable_media
        is_media = has_downloadable_media(original)

    # Early exit: if no real downloadable file → treat as text, skip downloader
    if is_media and file_path is None:
        file_id = _get_file_id(original)
        if not file_id:
            logger.info(
                "safe_non_premium_upload: no file_path and no file_id — "
                "treating as text (likely web preview or empty media)"
            )
            is_media = False

    # Strip custom emoji and rebuild
    if text:
        text, entities = strip_custom_emoji(text, entities)

    if is_media:
        # Media message handling
        return await _non_premium_media_upload(
            client, original, target_chat, text, entities,
            reply_to_message_id, file_path, progress
        )
    else:
        # Text-only message
        return await _non_premium_text_upload(
            client, target_chat, text, entities, reply_to_message_id
        )


async def _non_premium_text_upload(
    client: Client,
    target_chat: int,
    text: str,
    entities: List[MessageEntity],
    reply_to_message_id: Optional[int],
) -> List[Message]:
    """Send text message with non-premium safe splitting."""
    if not text:
        return []

    chunks = split_text_respecting_hyperlinks(text, entities, MESSAGE_LIMIT)
    sent = []
    reply_to = reply_to_message_id

    for i, chunk in enumerate(chunks):
        kwargs = {
            'chat_id': target_chat,
            'text': chunk.text,
            'disable_web_page_preview': True,
        }
        if chunk.entities:
            kwargs['entities'] = chunk.entities
        if reply_to:
            kwargs['reply_to_message_id'] = reply_to

        msg = await _send_with_flood_retry(client.send_message, kwargs)
        if msg:
            sent.append(msg)
            reply_to = msg.id

        if i < len(chunks) - 1:
            await asyncio.sleep(0.3)

    return sent


async def _non_premium_media_upload(
    client: Client,
    original: Message,
    target_chat: int,
    caption: str,
    entities: List[MessageEntity],
    reply_to_message_id: Optional[int],
    file_path: Optional[str],
    progress: Any,
) -> List[Message]:
    """Send media with non-premium caption splitting and overflow."""
    sent = []

    # Determine media type
    media_type = _detect_media_type(original)
    if not media_type:
        logger.warning("Unknown media type, falling back to document")
        media_type = 'document'

    # Determine media source
    media_source = file_path
    if not media_source:
        # Use file_id from original message
        media_source = _get_file_id(original)
        if not media_source:
            logger.error("No file_path and no file_id available")
            return []

    # Split caption if needed
    caption_limit = CAPTION_LIMIT
    caption_chunks = []

    if caption:
        caption_chunks = split_text_respecting_hyperlinks(caption, entities, caption_limit)

    # Build media send kwargs
    media_kwargs = {
        'chat_id': target_chat,
        media_type: media_source,
    }

    # Add media-specific attributes
    _add_media_attributes(media_kwargs, original, media_type)

    if progress:
        media_kwargs['progress'] = progress

    # First caption chunk goes with media
    if caption_chunks:
        first_chunk = caption_chunks[0]
        media_kwargs['caption'] = first_chunk.text
        if first_chunk.entities:
            media_kwargs['caption_entities'] = first_chunk.entities

    if reply_to_message_id:
        media_kwargs['reply_to_message_id'] = reply_to_message_id

    # Send media
    send_func = getattr(client, f'send_{media_type}', None)
    if not send_func:
        logger.error(f"No send method for media type: {media_type}")
        return []

    media_msg = await _send_with_flood_retry(send_func, media_kwargs)

    if not media_msg:
        # Fallback: try without entities
        if 'caption_entities' in media_kwargs:
            del media_kwargs['caption_entities']
            media_msg = await _send_with_flood_retry(send_func, media_kwargs)

    if media_msg:
        sent.append(media_msg)
    else:
        logger.error("Failed to send media even without entities")
        return []

    # Send overflow caption as reply chain
    if len(caption_chunks) > 1 and media_msg:
        total_parts = len(caption_chunks)
        reply_to = media_msg.id

        for i, chunk in enumerate(caption_chunks[1:], start=2):
            header = f"📌 Davomi {i}/{total_parts}\n\n"
            overflow_text = header + chunk.text

            # Adjust entity offsets for header
            header_utf16 = utf16_len(header)
            adjusted_entities = []
            for e in chunk.entities:
                new_e = MessageEntity(
                    type=e.type,
                    offset=e.offset + header_utf16,
                    length=e.length,
                    url=getattr(e, 'url', None),
                    user=getattr(e, 'user', None),
                    language=getattr(e, 'language', None),
                )
                # Validate
                if new_e.offset + new_e.length <= utf16_len(overflow_text):
                    adjusted_entities.append(new_e)

            kwargs = {
                'chat_id': target_chat,
                'text': overflow_text,
                'reply_to_message_id': reply_to,
                'disable_web_page_preview': True,
            }
            if adjusted_entities:
                kwargs['entities'] = adjusted_entities

            msg = await _send_with_flood_retry(client.send_message, kwargs)
            if msg:
                sent.append(msg)
                reply_to = msg.id

            await asyncio.sleep(0.3)

    return sent


# ==================== DYNAMIC UPLOAD ROUTER ====================

async def dynamic_upload_router(
    client: Client,
    original: Message,
    target_chat: int,
    user_id: int,
    reply_to_message_id: Optional[int] = None,
    file_path: Optional[str] = None,
    progress: Any = None,
    user_session_string: Optional[str] = None,
) -> List[Message]:
    """
    Central repost router — auto-selects upload mode.
    
    MODE A: User is Telegram Premium → copy via user session (no split)
    MODE B: System Premium session exists → relay via system session
    MODE C: Non-Premium fallback → strip, rebuild, split, send
    
    Args:
        client: The active Pyrogram client (user session)
        original: Source message to repost
        target_chat: Destination chat ID
        user_id: User's Telegram ID (for premium check & settings)
        reply_to_message_id: Optional reply target in destination
        file_path: Path to downloaded file (for media)
        progress: Upload progress callback
        user_session_string: User's session string (for Mode A relay)
    
    Returns:
        List of sent Message objects
    """
    # Check user's upload setting
    user_setting = get_user_upload_setting(user_id)

    # ==================== MODE A: USER IS PREMIUM ====================
    try:
        me = await client.get_me()
        user_is_premium = getattr(me, 'is_premium', False) or False
    except Exception as e:
        logger.warning(f"get_me() failed, assuming non-premium: {e}")
        user_is_premium = False

    if user_is_premium and user_setting != UPLOAD_FORCE_SPLIT:
        logger.info(f"MODE A: User {user_id} is Premium — direct copy")
        try:
            result = await _premium_direct_copy(
                client, original, target_chat,
                reply_to_message_id, file_path, progress
            )
            if result:
                return result
            logger.warning("MODE A failed, falling through to MODE B/C")
        except _FATAL_ERRORS as e:
            logger.warning(f"MODE A fatal error: {type(e).__name__}, trying fallback")
        except Exception as e:
            logger.warning(f"MODE A error: {e}, trying fallback")

    # ==================== MODE B: SYSTEM PREMIUM RELAY ====================
    sys_session = get_system_session()
    if sys_session and user_setting != UPLOAD_FORCE_SPLIT:
        logger.info(f"MODE B: Using system Premium relay for user {user_id}")
        try:
            result = await _premium_relay_upload(
                sys_session, original, target_chat,
                reply_to_message_id, file_path, progress
            )
            if result:
                return result
            logger.warning("MODE B failed, falling through to MODE C")
        except _FATAL_ERRORS as e:
            logger.warning(f"MODE B fatal: {type(e).__name__} — system session dead")
        except Exception as e:
            logger.warning(f"MODE B error: {e}, falling through to MODE C")

    # ==================== MODE C: NON-PREMIUM FALLBACK ====================
    logger.info(f"MODE C: Non-Premium fallback for user {user_id}")
    return await safe_non_premium_upload(
        client, original, target_chat,
        reply_to_message_id, file_path, progress
    )


# ==================== PREMIUM DIRECT COPY (MODE A) ====================

async def _premium_direct_copy(
    client: Client,
    original: Message,
    target_chat: int,
    reply_to_message_id: Optional[int],
    file_path: Optional[str],
    progress: Any,
) -> Optional[List[Message]]:
    """
    Mode A: Direct copy using user's own Premium session.
    Preserves full Premium formatting including custom emoji.
    """
    try:
        kwargs = {}
        if reply_to_message_id:
            kwargs['reply_to_message_id'] = reply_to_message_id

        # Try copy_message first (preserves everything for Premium)
        msg = await client.copy_message(
            chat_id=target_chat,
            from_chat_id=original.chat.id,
            message_id=original.id,
            **kwargs,
        )
        return [msg] if msg else None
    except Exception as e:
        logger.warning(f"copy_message failed in Mode A: {e}")
        # Fall back to manual media send if file_path available
        if file_path and original.media:
            return await _send_media_via_client(
                client, original, target_chat,
                reply_to_message_id, file_path, progress,
                strip_emoji=False
            )
        return None


# ==================== PREMIUM RELAY (MODE B) ====================

async def _premium_relay_upload(
    sys_session: SystemPremiumSession,
    original: Message,
    target_chat: int,
    reply_to_message_id: Optional[int],
    file_path: Optional[str],
    progress: Any,
) -> Optional[List[Message]]:
    """
    Mode B: Upload via system Premium session.
    Premium session is used as a relay — sends to target chat.
    """
    session_id = f"sys_{sys_session.user_id}"
    lock = _get_session_lock(session_id)

    async with lock:
        client_name = f"premium_relay_{uuid.uuid4().hex[:8]}"
        fp = get_client_params()
        relay_client = Client(
            client_name,
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=sys_session.session_string,
            in_memory=True,
            no_updates=True,
            sleep_threshold=60,
            max_concurrent_transmissions=4,
            device_model=fp['device_model'],
            system_version=fp['system_version'],
            app_version=fp['app_version'],
            lang_code=fp['lang_code'],
        )

        try:
            await asyncio.wait_for(relay_client.start(), timeout=20.0)

            if file_path and original.media:
                result = await _send_media_via_client(
                    relay_client, original, target_chat,
                    reply_to_message_id, file_path, progress,
                    strip_emoji=False
                )
                return result
            elif original.text:
                # Text message — copy via relay
                text = original.text
                entities = list(original.entities) if original.entities else []
                kwargs = {
                    'chat_id': target_chat,
                    'text': text,
                    'disable_web_page_preview': True,
                }
                if entities:
                    kwargs['entities'] = entities
                if reply_to_message_id:
                    kwargs['reply_to_message_id'] = reply_to_message_id

                msg = await relay_client.send_message(**kwargs)
                return [msg] if msg else None
            else:
                return None
        except FloodWait as e:
            wait = getattr(e, 'value', getattr(e, 'x', 30))
            logger.warning(f"Premium relay FloodWait: {wait}s")
            if wait <= 60:
                await asyncio.sleep(wait)
                return None  # Don't retry, let caller fall through
            return None
        except asyncio.TimeoutError:
            logger.warning("Premium relay connection timed out")
            return None
        finally:
            try:
                if relay_client.is_connected:
                    await asyncio.wait_for(relay_client.stop(), timeout=5.0)
            except Exception:
                pass


# ==================== HOT-LOAD PREMIUM SESSION ====================

async def load_premium_session_runtime(session_string: str) -> Tuple[bool, str]:
    """
    Hot-load a Premium session at runtime — NO RESTART REQUIRED.
    
    Process:
    1. Validate session by connecting and checking get_me()
    2. Verify account is Telegram Premium
    3. Load into memory immediately
    4. Persist to disk for next startup
    
    Args:
        session_string: Pyrogram session string
    
    Returns:
        (success, human-readable message)
    """
    return await _set_system_session(session_string)


async def remove_premium_session_runtime() -> Tuple[bool, str]:
    """Remove premium relay session at runtime. No restart needed."""
    return await remove_system_session()


# ==================== HELPER FUNCTIONS ====================

async def _send_media_via_client(
    client: Client,
    original: Message,
    target_chat: int,
    reply_to_message_id: Optional[int],
    file_path: str,
    progress: Any,
    strip_emoji: bool = True,
) -> Optional[List[Message]]:
    """
    Send media via a client session with optional emoji stripping.
    
    Used by both Mode A and Mode B.
    """
    media_type = _detect_media_type(original)
    if not media_type:
        media_type = 'document'

    caption = original.caption or ""
    entities = list(original.caption_entities) if original.caption_entities else []

    if strip_emoji and caption:
        caption, entities = strip_custom_emoji(caption, entities)

    # Determine caption limit
    is_premium = not strip_emoji  # If we're not stripping, we're in premium mode
    cap_limit = get_caption_limit(is_premium)

    media_kwargs = {
        'chat_id': target_chat,
        media_type: file_path,
    }
    _add_media_attributes(media_kwargs, original, media_type)

    if progress:
        media_kwargs['progress'] = progress
    if reply_to_message_id:
        media_kwargs['reply_to_message_id'] = reply_to_message_id

    # Handle caption
    overflow_chunks = []
    if caption:
        if utf16_len(caption) <= cap_limit:
            media_kwargs['caption'] = caption
            if entities:
                media_kwargs['caption_entities'] = entities
        else:
            chunks = split_text_respecting_hyperlinks(caption, entities, cap_limit)
            if chunks:
                media_kwargs['caption'] = chunks[0].text
                if chunks[0].entities:
                    media_kwargs['caption_entities'] = chunks[0].entities
                overflow_chunks = chunks[1:]

    send_func = getattr(client, f'send_{media_type}', None)
    if not send_func:
        return None

    sent = []
    media_msg = await _send_with_flood_retry(send_func, media_kwargs)

    if not media_msg and 'caption_entities' in media_kwargs:
        del media_kwargs['caption_entities']
        media_msg = await _send_with_flood_retry(send_func, media_kwargs)

    if media_msg:
        sent.append(media_msg)

    # Send overflow
    if overflow_chunks and media_msg:
        total = len(overflow_chunks) + 1
        reply_to = media_msg.id
        for i, chunk in enumerate(overflow_chunks, start=2):
            header = f"📌 Davomi {i}/{total}\n\n"
            text = header + chunk.text
            h_utf16 = utf16_len(header)
            adj_ents = []
            for e in chunk.entities:
                ne = MessageEntity(
                    type=e.type, offset=e.offset + h_utf16, length=e.length,
                    url=getattr(e, 'url', None), user=getattr(e, 'user', None),
                    language=getattr(e, 'language', None),
                )
                if ne.offset + ne.length <= utf16_len(text):
                    adj_ents.append(ne)

            kw = {
                'chat_id': target_chat, 'text': text,
                'reply_to_message_id': reply_to,
                'disable_web_page_preview': True,
            }
            if adj_ents:
                kw['entities'] = adj_ents

            m = await _send_with_flood_retry(client.send_message, kw)
            if m:
                sent.append(m)
                reply_to = m.id
            await asyncio.sleep(0.3)

    return sent if sent else None


def _detect_media_type(msg: Message) -> Optional[str]:
    """Detect Pyrogram send method name from message media type.

    Uses attribute-based detection only — never checks msg.media enum
    or MessageMediaType, which is unreliable/missing in Pyrofork 2.3.69.
    Web-page preview messages (link previews) are correctly classified as
    None (text), never as downloadable media.
    """
    from core.media_classifier import get_media_send_method
    return get_media_send_method(msg)


def _get_file_id(msg: Message) -> Optional[str]:
    """Extract file_id from message for re-sending."""
    for attr in ['photo', 'video', 'document', 'audio', 'voice', 'video_note', 'animation', 'sticker']:
        media = getattr(msg, attr, None)
        if media:
            if attr == 'photo':
                return media.file_id
            return media.file_id
    return None


def _add_media_attributes(kwargs: dict, original: Message, media_type: str):
    """Add media-specific attributes (duration, dimensions, etc.)."""
    if media_type == 'video' and original.video:
        kwargs['duration'] = original.video.duration or 0
        kwargs['width'] = original.video.width or 0
        kwargs['height'] = original.video.height or 0
        kwargs['supports_streaming'] = True
    elif media_type == 'audio' and original.audio:
        kwargs['duration'] = original.audio.duration or 0
        kwargs['performer'] = original.audio.performer
        kwargs['title'] = original.audio.title
    elif media_type == 'voice' and original.voice:
        kwargs['duration'] = original.voice.duration or 0
    elif media_type == 'video_note' and original.video_note:
        kwargs['duration'] = original.video_note.duration or 0
        kwargs['length'] = original.video_note.length or 0
    elif media_type == 'animation' and original.animation:
        kwargs['duration'] = original.animation.duration or 0
        kwargs['width'] = original.animation.width or 0
        kwargs['height'] = original.animation.height or 0


async def _send_with_flood_retry(
    send_func,
    kwargs: dict,
    max_retries: int = 3,
) -> Optional[Message]:
    """Send with per-session FloodWait retry and entity error fallback.

    Uses send_func's bound client as session key so FloodWait cooldowns
    are tracked per-session.  Multiple users never block each other.
    """
    # Derive per-session key from the bound method's client instance
    session_key = id(getattr(send_func, '__self__', None)) or 0

    # Honour active per-session FloodWait cooldown before attempting
    if session_key and session_key in _flood_cooldowns:
        cooldown_until, _ = _flood_cooldowns[session_key]
        remaining = cooldown_until - time.monotonic()
        if remaining > 0:
            logger.info(
                "Session %s in FloodWait cooldown, waiting %.1fs",
                session_key, remaining,
            )
            await asyncio.sleep(remaining)
        _flood_cooldowns.pop(session_key, None)

    for attempt in range(max_retries):
        try:
            result = await send_func(**kwargs)
            # Success — clear any stale cooldown for this session
            _flood_cooldowns.pop(session_key, None)
            return result
        except FloodWait as e:
            wait = min(getattr(e, 'value', getattr(e, 'x', 30)), 60)
            if session_key:
                _flood_cooldowns[session_key] = (time.monotonic() + wait, wait)
            logger.warning(
                "FloodWait: %ds (session=%s, attempt %d/%d)",
                wait, session_key, attempt + 1, max_retries,
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(wait)
            else:
                logger.error("FloodWait exceeded retries for session %s", session_key)
                return None
        except Exception as e:
            err = str(e).upper()
            if 'ENTITY' in err or 'BOUNDS' in err:
                logger.warning(f"Entity error: {e}")
                if 'entities' in kwargs:
                    kwargs = {k: v for k, v in kwargs.items() if k != 'entities'}
                    continue
                if 'caption_entities' in kwargs:
                    kwargs = {k: v for k, v in kwargs.items() if k != 'caption_entities'}
                    continue
            if attempt < max_retries - 1:
                logger.warning(f"Send error: {e}, retrying...")
                await asyncio.sleep(1)
            else:
                logger.error(f"Send failed after {max_retries} attempts: {e}")
                return None
    return None
