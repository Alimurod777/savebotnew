"""
Universal Reply Compatibility Layer

Provides seamless backward and forward compatibility between:
- Legacy Pyrogram/Kurigram (≤ 2.2.x): uses reply_to_message_id
- Modern Pyrogram (≥ 2.4.x): uses reply_parameters with ReplyParameters
- Pyrofork (2.3.x+): uses reply_to_message_id (no ReplyParameters)

Usage:
    from core.reply_compat import build_reply_kwargs
    
    await client.send_message(
        chat_id=chat_id,
        text=text,
        **build_reply_kwargs(reply_to_id)
    )

This wrapper:
- Never raises ImportError
- Never breaks message sending
- Requires zero code changes when upgrading Pyrogram
- Is safely expandable via **kwargs

PYROFORK MIGRATION NOTE:
Pyrofork does NOT use ReplyParameters. It uses the simpler reply_to_message_id
parameter directly. This layer handles both cases transparently.
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
    
    try:
        from pyrogram.types import ReplyParameters
        # ReplyParameters exists - but check if it's actually used
        # Pyrofork has the class but prefers reply_to_message_id
        import pyrogram
        version = getattr(pyrogram, '__version__', '0.0.0')
        
        # Pyrofork versions typically don't need ReplyParameters
        # They support direct reply_to_message_id
        if 'pyrofork' in str(type(pyrogram)).lower():
            _use_reply_parameters = False
        else:
            _use_reply_parameters = True
            
    except ImportError:
        _use_reply_parameters = False
    
    return _use_reply_parameters


def build_reply_kwargs(
    reply_to_message_id: Optional[int] = None,
    *,
    quote: Optional[bool] = None,
    allow_sending_without_reply: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Build reply-related keyword arguments compatible with any Pyrogram version.
    
    For Pyrofork: Uses reply_to_message_id directly (preferred method).
    For modern Pyrogram: Attempts ReplyParameters if available.
    For legacy: Falls back to reply_to_message_id.
    
    Args:
        reply_to_message_id: Message ID to reply to (None = no reply)
        quote: Whether to quote the replied message (ReplyParameters only)
        allow_sending_without_reply: Send even if reply message is deleted
    
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
            from pyrogram.types import ReplyParameters
            
            params_kwargs = {"message_id": reply_to_message_id}
            
            if quote is not None:
                params_kwargs["quote"] = quote
            if allow_sending_without_reply is not None:
                params_kwargs["allow_sending_without_reply"] = allow_sending_without_reply
            
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
        from pyrogram.types import LinkPreviewOptions
        return {"link_preview_options": LinkPreviewOptions(is_disabled=is_disabled)}
    except ImportError:
        # Fallback for legacy Pyrogram/Kurigram ≤ 2.2.x
        return {"disable_web_page_preview": is_disabled}


# Type hint support (avoid runtime import)
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyrogram.types import Message
    Any = Message
else:
    Any = object
