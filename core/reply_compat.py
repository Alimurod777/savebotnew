"""Reply/link-preview keyword helpers for Wzgram.

Wzgram's native ReplyParameters is detected from the send_message signature.
The legacy boolean quote and allow_sending_without_reply arguments are retained
for callers but are not passed to Wzgram's text-only quote constructor.
"""

from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

# Cache for version detection
_use_reply_parameters: Optional[bool] = None


def _detect_reply_style() -> bool:
    """
    Detect which reply style to use.
    
    Returns:
        True if ReplyParameters is available and should be used,
        False for legacy reply_to_message_id style.
    """
    global _use_reply_parameters
    
    if _use_reply_parameters is not None:
        return _use_reply_parameters
    
    from wzgram import Client
    import inspect
    _use_reply_parameters = "reply_parameters" in inspect.signature(Client.send_message).parameters

    return _use_reply_parameters


def build_reply_kwargs(
    reply_to_message_id: Optional[int] = None,
    *,
    quote: Optional[bool] = None,
    allow_sending_without_reply: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Build reply-related keyword arguments compatible with any Pyrogram version.
    
    Uses native Wzgram ReplyParameters when supported, with a legacy
    reply_to_message_id fallback.
    
    Args:
        reply_to_message_id: Message ID to reply to (None = no reply)
        quote: Legacy boolean flag, retained for caller compatibility
        allow_sending_without_reply: Legacy flag unsupported by Wzgram
    
    Returns:
        Dictionary of kwargs to spread into send_* methods
    
    Example:
        await client.send_message(
            chat_id=123,
            text="Hello",
            **build_reply_kwargs(456)
        )
        
        # With additional options (only used if ReplyParameters available):
        await client.send_photo(
            chat_id=123,
            photo="file.jpg",
            **build_reply_kwargs(456, quote=True)
        )
    """
    if reply_to_message_id is None:
        return {}
    
    # Pyrofork and most forks: use direct reply_to_message_id
    # This is simpler and more widely compatible
    result = {"reply_to_message_id": reply_to_message_id}
    
    # Only attempt ReplyParameters if detected as needed
    if _detect_reply_style():
        try:
            from wzgram.types import ReplyParameters
            
            params_kwargs = {"message_id": reply_to_message_id}
            
            # This helper takes a legacy boolean quote flag. Wzgram quote is
            # literal quoted text, so do not pass a boolean into that field.
            # Wzgram has no allow_sending_without_reply constructor parameter.
            
            return {"reply_parameters": ReplyParameters(**params_kwargs)}
        except Exception:
            pass  # Fall back to direct style
    
    return result


def build_reply_kwargs_from_message(
    message: Any,
    *,
    quote: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Build reply kwargs from an existing Message object.
    
    Convenience wrapper when replying to a received message.
    
    Args:
        message: Pyrogram Message object (or any object with .id attribute)
        quote: Whether to quote the replied message
    
    Returns:
        Dictionary of kwargs to spread into send_* methods
    
    Example:
        @client.on_message(...)
        async def handler(client, message):
            await client.send_message(
                chat_id=message.chat.id,
                text="Reply!",
                **build_reply_kwargs_from_message(message)
            )
    """
    if message is None:
        return {}
    
    msg_id = getattr(message, "id", None) or getattr(message, "message_id", None)
    return build_reply_kwargs(msg_id, quote=quote)


def build_link_preview_kwargs(is_disabled: bool = True) -> Dict[str, Any]:
    """
    Build link preview keyword arguments compatible with any Pyrogram version.
    
    Attempts to use LinkPreviewOptions (Pyrogram ≥ 2.4.x) if available,
    falls back to disable_web_page_preview (Pyrogram ≤ 2.2.x) otherwise.
    
    Args:
        is_disabled: Whether to disable link preview (default: True)
    
    Returns:
        Dictionary of kwargs to spread into send_message
    
    Example:
        await client.send_message(
            chat_id=123,
            text="https://example.com",
            **build_link_preview_kwargs(is_disabled=True)
        )
    """
    try:
        from wzgram.types import LinkPreviewOptions
        return {"link_preview_options": LinkPreviewOptions(is_disabled=is_disabled)}
    except ImportError:
        # Fallback for legacy Pyrogram/Kurigram ≤ 2.2.x
        return {"disable_web_page_preview": is_disabled}


# Type hint support (avoid runtime import)
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from wzgram.types import Message
    Any = Message
else:
    Any = object
