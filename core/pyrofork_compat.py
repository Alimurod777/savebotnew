"""
Pyrofork 2.3.69+ Compatibility Layer

Production-grade compatibility layer for Pyrofork migration (MTProto layer >= 220).

STRICT REQUIREMENTS:
- pyrofork >= 2.3.69
- tgcrypto-pyrofork == 1.2.8 (MANDATORY, fork-specific)
- Markdown parse_mode ONLY (HTML is FORBIDDEN)
- Auto-split messages at 4096 UTF-8 characters

CRITICAL: Silent fallback + minimal reply strategy
- Attempt reply_to_message_id ONLY ONCE for single, unsplit messages
- On ANY warning/exception, silently fall back to normal send
- NEVER retry replies, NEVER force reply on split messages

Key features:
- Safe message sending with Markdown-only parse mode
- Auto-split long messages preserving Markdown integrity
- Graceful fallback to plain text on parse failure
- Silent reply fallback (eliminates Pyrofork reply warnings)
- Handler registration safety (no duplicates, no orphans)
- Dispatcher stability fixes
- BUGFIX: Patches Pyrofork utils.get_reply_to() parameter name mismatch
  (reply_to_message_id → reply_to_msg_id, etc.)

Usage:
    from core.pyrofork_compat import safe_send_message, safe_reply

    await safe_send_message(client, chat_id, text)
    await safe_reply(message, text)
"""

import asyncio
import logging
from typing import Optional, Any, List, Union

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# PYROFORK BUGFIX: utils.get_reply_to() parameter name mismatch
# ═══════════════════════════════════════════════════════════════════════════════
# Pyrofork 2.3.69 Layer 220 has a bug in utils.get_reply_to():
#   It passes reply_to_message_id, message_thread_id, reply_to_chat to
#   InputReplyToMessage, but the TLObject expects:
#     reply_to_msg_id, top_msg_id, reply_to_peer_id
#   This causes TypeError on ANY send with reply_to_message_id != None.
#
# This monkey-patch fixes it at runtime so it works even if venv is reinstalled.
# ═══════════════════════════════════════════════════════════════════════════════

def _patch_pyrofork_get_reply_to():
    """
    Monkey-patch pyrogram.utils.get_reply_to to use correct parameter names
    for InputReplyToMessage TLObject.
    """
    try:
        import pyrogram.utils as _utils
        from pyrogram.raw import types as _raw_types

        # Check if the bug exists: InputReplyToMessage expects reply_to_msg_id
        _slots = getattr(_raw_types.InputReplyToMessage, '__slots__', [])
        if 'reply_to_msg_id' not in _slots:
            # Different version — no patch needed
            return

        _original_get_reply_to = _utils.get_reply_to

        async def _patched_get_reply_to(
            client,
            chat_id=None,
            reply_to_message_id=None,
            reply_to_story_id=None,
            message_thread_id=None,
            reply_to_monoforum_id=None,
            reply_to_chat_id=None,
            quote_text=None,
            quote_entities=None,
            quote_offset=None,
            parse_mode=None,
        ):
            reply_to = None
            reply_to_chat = None
            if reply_to_monoforum_id:
                peer = await client.resolve_peer(reply_to_monoforum_id)
                reply_to = _raw_types.InputReplyToMonoforum(
                    monoforum_peer=peer
                )
            elif reply_to_message_id or message_thread_id:
                text, entities = (await _utils.parse_text_entities(
                    client, quote_text, parse_mode, quote_entities
                )).values()
                if reply_to_chat_id is not None:
                    reply_to_chat = await client.resolve_peer(reply_to_chat_id)
                # FIXED: Use correct TLObject parameter names
                reply_to = _raw_types.InputReplyToMessage(
                    reply_to_msg_id=reply_to_message_id,
                    top_msg_id=message_thread_id,
                    reply_to_peer_id=reply_to_chat,
                    quote_text=text,
                    quote_entities=entities,
                    quote_offset=quote_offset,
                )
            elif reply_to_story_id:
                peer = await client.resolve_peer(chat_id)
                reply_to = _raw_types.InputReplyToStory(
                    peer=peer,
                    story_id=reply_to_story_id,
                )
            return reply_to

        _utils.get_reply_to = _patched_get_reply_to
        logger.info("Pyrofork utils.get_reply_to() patched — reply_to_msg_id fix applied")
    except Exception as e:
        logger.debug("Pyrofork get_reply_to patch skipped: %s", e)


# Apply patch on import
_patch_pyrofork_get_reply_to()

# Import from centralized modules
from core.message_utils import (
    MAX_MESSAGE_LENGTH,
    MAX_CAPTION_LENGTH,
    split_text,
    split_caption as _split_caption_tuple,
    sanitize_markdown,
    escape_markdown,
    normalize_poll_to_text,
)
from core.safe_send import (
    safe_send_message,
    safe_reply,
    safe_edit_message,
    safe_send_photo,
    safe_send_video,
    safe_send_document,
    safe_send_audio,
    safe_handler,
    get_parse_mode as get_markdown_mode,
    get_disabled_mode,
)
from core.handler_manager import (
    SafeHandlerManager,
    get_handler_manager,
    temporary_handler,
    cleanup_on_disconnect,
    cleanup_global,
    WaitingHandler,
)


# Backwards-compatible split_message that returns List[str]
def split_message(
    text: str,
    max_length: int = MAX_MESSAGE_LENGTH,
    preserve_markdown: bool = True
) -> List[str]:
    """Split message into chunks fitting Telegram's limit."""
    return split_text(text, max_length, preserve_markdown)


def split_caption(text: str) -> List[str]:
    """Split caption for media (1024 char limit). Returns list of chunks."""
    if not text:
        return []
    
    if len(text) <= MAX_CAPTION_LENGTH:
        return [text]
    
    # Use the tuple version and combine
    media_cap, overflow = _split_caption_tuple(text)
    result = []
    if media_cap:
        result.append(media_cap)
    if overflow:
        result.append(overflow)
    return result


async def safe_get_media_group(
    client,
    chat_id: int,
    message_id: int
) -> List[Any]:
    """
    Safely get media group, handling Pyrofork compatibility issues.
    
    CRITICAL: Handles these Pyrofork quirks:
    - 'Messages.__init__() missing required keyword-only argument: topics'
    - 'Messages.__init__() got an unexpected keyword argument'
    - Other Messages class initialization errors
    
    These errors are NON-FATAL. The message was likely sent successfully.
    We treat them as success and try fallback methods.
    
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
        # Known Pyrofork compatibility issues - treat as non-fatal
        pyrofork_patterns = ['topics', 'messages', '__init__', 'keyword', 'argument']
        if any(p in error_str for p in pyrofork_patterns):
            logger.debug(f"Pyrofork Messages compatibility issue (expected, non-fatal): {te}")
            # Fallback: try to get the single message
            try:
                msg = await client.get_messages(chat_id, message_id)
                if msg and not getattr(msg, 'empty', False):
                    return [msg]
            except Exception:
                pass
        else:
            logger.warning(f"Unexpected TypeError in get_media_group: {te}")
        return []
    
    except AttributeError as ae:
        # Another potential Pyrofork quirk
        logger.debug(f"AttributeError in get_media_group (Pyrofork quirk): {ae}")
        try:
            msg = await client.get_messages(chat_id, message_id)
            if msg and not getattr(msg, 'empty', False):
                return [msg]
        except Exception:
            pass
        return []
        
    except Exception as e:
        logger.debug(f"get_media_group error: {e}")
        return []


async def safe_send_media_group(
    client,
    chat_id: int,
    media: List[Any],
    reply_to_message_id: Optional[int] = None,
    **kwargs
) -> List[Any]:
    """
    Safely send media group, handling Pyrofork compatibility issues.
    
    CRITICAL: Handles 'topics' argument errors which are NON-FATAL.
    The send likely succeeded even if this error occurs.
    
    Args:
        client: Pyrogram client
        chat_id: Target chat ID
        media: List of InputMedia objects
        reply_to_message_id: Optional reply target
        **kwargs: Additional arguments
    
    Returns:
        List of sent Messages, or empty list on failure
    """
    try:
        result = await client.send_media_group(
            chat_id=chat_id,
            media=media,
            reply_to_message_id=reply_to_message_id,
            **kwargs
        )
        
        if result is None:
            return []
        if isinstance(result, list):
            return result
        try:
            return list(result)
        except TypeError:
            return [result] if result else []
            
    except TypeError as te:
        error_str = str(te).lower()
        pyrofork_patterns = ['topics', 'messages', '__init__', 'keyword', 'argument']
        if any(p in error_str for p in pyrofork_patterns):
            # Non-fatal Pyrofork issue - message was likely sent
            logger.debug(f"Pyrofork send_media_group issue (non-fatal): {te}")
            return []  # Return empty - caller should check via get_messages if needed
        logger.warning(f"send_media_group TypeError: {te}")
        raise
    
    except AttributeError as ae:
        logger.debug(f"Pyrofork send_media_group AttributeError (non-fatal): {ae}")
        return []
        
    except Exception as e:
        logger.warning(f"send_media_group error: {e}")
        raise


# Re-export everything for backwards compatibility
__all__ = [
    # Message sending
    'safe_send_message',
    'safe_reply', 
    'safe_edit_message',
    'safe_send_photo',
    'safe_send_video',
    'safe_send_document',
    'safe_send_audio',
    'safe_handler',
    # Parse modes
    'get_markdown_mode',
    'get_disabled_mode',
    # Message utilities
    'split_message',
    'split_caption',
    'sanitize_markdown',
    'escape_markdown',
    'normalize_poll_to_text',
    # Handler management
    'SafeHandlerManager',
    'get_handler_manager',
    'temporary_handler',
    'cleanup_on_disconnect',
    'cleanup_global',
    'WaitingHandler',
    # Pyrofork compatibility
    'safe_get_media_group',
    'safe_send_media_group',
    # Constants
    'MAX_MESSAGE_LENGTH',
    'MAX_CAPTION_LENGTH',
    # Version utilities
    'has_listen_support',
    'safe_listen',
    'get_message_media_type',
    'get_pyrogram_version',
    'is_pyrofork',
    'log_client_info',
]


# ============================================================================
# PYROFORK-SPECIFIC FEATURES
# ============================================================================

def has_listen_support(client) -> bool:
    """Check if client has listen() support (Pyrofork native)."""
    return hasattr(client, 'listen') and callable(getattr(client, 'listen'))


async def safe_listen(
    client,
    chat_id: int,
    timeout: float = 300,
    filters=None
) -> Optional[Any]:
    """
    Safely wait for a message from a user.
    
    Uses Pyrofork's native listen() or falls back to WaitingHandler.
    
    Args:
        client: Pyrogram Client
        chat_id: Chat to listen in
        timeout: Timeout in seconds
        filters: Optional message filters
    
    Returns:
        Message object or None on timeout
    """
    # Try native listen() first
    if has_listen_support(client):
        try:
            return await client.listen(
                chat_id=chat_id,
                filters=filters,
                timeout=timeout
            )
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.debug(f"Native listen() failed: {e}, using WaitingHandler")
    
    # Fallback to our WaitingHandler
    waiter = WaitingHandler(client, chat_id, timeout, filters)
    return await waiter.wait()


# ============================================================================
# MEDIA TYPE COMPATIBILITY
# ============================================================================

def get_message_media_type(message) -> Optional[str]:
    """
    Get the media type of a message in a compatible way.
    
    Returns string name of media type for consistency across versions.
    """
    try:
        from pyrogram.enums import MessageMediaType
        
        if message.media is None:
            return None
        
        media_type = message.media
        if hasattr(media_type, 'name'):
            return media_type.name.lower()
        return str(media_type).lower()
    except Exception:
        # Fallback for older versions
        if message.photo:
            return "photo"
        elif message.video:
            return "video"
        elif message.document:
            return "document"
        elif message.audio:
            return "audio"
        elif message.voice:
            return "voice"
        elif message.video_note:
            return "video_note"
        elif message.sticker:
            return "sticker"
        elif message.animation:
            return "animation"
        elif message.poll:
            return "poll"
        return None


# ============================================================================
# VERSION DETECTION
# ============================================================================

def get_pyrogram_version() -> str:
    """Get the installed Pyrogram/Pyrofork version."""
    try:
        import pyrogram
        return getattr(pyrogram, '__version__', 'unknown')
    except ImportError:
        return 'not installed'


def is_pyrofork() -> bool:
    """Check if running on Pyrofork (vs original Pyrogram)."""
    try:
        import pyrogram
        version = getattr(pyrogram, '__version__', '')
        # Pyrofork has version >= 2.3.x
        major, minor = version.split('.')[:2]
        return int(minor) >= 3
    except Exception:
        return False


def log_client_info():
    """Log information about the Pyrogram client being used."""
    version = get_pyrogram_version()
    is_fork = is_pyrofork()
    logger.info(f"Pyrogram version: {version}, Pyrofork: {is_fork}")
