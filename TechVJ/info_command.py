"""
/info command - Get detailed channel/group information from Telegram links.

Supports:
- Public channels: https://t.me/channelname/123
- Private channels: https://t.me/c/1234567890/123
- Topic links: https://t.me/c/1234567890/123/456
- Thread links: https://t.me/c/1234567890/123?thread=456

Returns:
- Title, username, ID
- Member count
- Last post ID, total posts
- Comments enabled status
- Topics enabled status
"""

from TechVJ.lang import get_string, get_report_error, get_report_type
import re
import asyncio
from wzgram import Client, filters
from wzgram.types import Message
from wzgram.enums import ChatType
from core.reply_compat import build_reply_kwargs_from_message
from wzgram.raw import functions, types

from config import API_ID, API_HASH, OWNER_ID, get_client_params
from database.async_db import async_db


def get(obj, key, default=None):
    """Safely get value from dict."""
    try:
        return obj.get(key, default) if hasattr(obj, 'get') else obj[key]
    except:
        return default


async def create_user_client(session_string: str, name: str = "info_client"):
    """
    Create and connect a Pyrogram user client.
    
    DEPRECATED: Use create_user_session from session_handler.py instead.
    Kept for backward compatibility.
    """
    import uuid
    try:
        fp = get_client_params()
        client = Client(
            f"{name}_{uuid.uuid4().hex[:8]}",
            session_string=session_string,
            api_hash=API_HASH,
            api_id=API_ID,
            no_updates=True,
            in_memory=True,
            sleep_threshold=30,
            device_model=fp["device_model"],
            system_version=fp["system_version"],
            app_version=fp["app_version"],
            lang_code=fp["lang_code"],
        )
        await client.connect()
        return client, None
    except Exception as e:
        return None, str(e)


async def safe_disconnect(client):
    """Safely disconnect client."""
    if client:
        try:
            await client.disconnect()
        except:
            pass


def parse_info_link(url: str, user_id: int = 0):
    """
    Parse Telegram link for /info command.
    
    Returns: (chat_id, message_id, topic_id, thread_id, link_type, error)
    """
    url = url.strip()
    
    # Topic link: https://t.me/c/1234567890/123/456 (chat/topic/msg)
    topic_match = re.search(r'https?://t\.me/c/(\d+)/(\d+)/(\d+)', url)
    if topic_match:
        chat_id = int("-100" + topic_match.group(1))
        topic_id = int(topic_match.group(2))
        message_id = int(topic_match.group(3))
        return chat_id, message_id, topic_id, None, "topic", None
    
    # Thread link: https://t.me/c/1234567890/123?thread=456
    thread_match = re.search(r'https?://t\.me/c/(\d+)/(\d+)\?thread=(\d+)', url)
    if thread_match:
        chat_id = int("-100" + thread_match.group(1))
        message_id = int(thread_match.group(2))
        thread_id = int(thread_match.group(3))
        return chat_id, message_id, None, thread_id, "thread", None
    
    # Private channel: https://t.me/c/1234567890/123
    private_match = re.search(r'https?://t\.me/c/(\d+)/(\d+)', url)
    if private_match:
        chat_id = int("-100" + private_match.group(1))
        message_id = int(private_match.group(2))
        return chat_id, message_id, None, None, "private", None
    
    # Public channel: https://t.me/channelname/123
    public_match = re.search(r'https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)', url)
    if public_match:
        username = public_match.group(1)
        message_id = int(public_match.group(2))
        return username, message_id, None, None, "public", None
    
    # Channel only (no message): https://t.me/channelname
    channel_only = re.search(r'https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/?$', url)
    if channel_only:
        username = channel_only.group(1)
        return username, None, None, None, "channel", None
    
    return None, None, None, None, None, get_string(user_id, 'info_input_invalid')


async def get_chat_info(client: Client, chat_id, user_id: int = 0) -> dict:
    """
    Get comprehensive chat information.
    
    Returns dict with all available info including:
    - Title, username, ID
    - Type (channel, group, supergroup)
    - Member count
    - Last post ID
    - Creation date (if available)
    - Description (if available)
    - Linked discussion group (if exists)
    """
    result = {
        "title": None,
        "username": None,
        "id": None,
        "type": None,
        "member_count": None,
        "description": None,
        "linked_chat_id": None,
        "linked_chat_title": None,
        "comments_enabled": False,
        "topics_enabled": False,
        "last_message_id": None,
        "creation_date": None,
        "is_verified": False,
        "is_restricted": False,
        "is_scam": False,
        "is_fake": False,
        "slow_mode": None,
        "error": None
    }
    
    try:
        chat = await client.get_chat(chat_id)
        
        result["title"] = chat.title or chat.first_name or "Unknown"
        result["username"] = chat.username
        result["id"] = chat.id
        result["type"] = str(chat.type).split(".")[-1] if chat.type else "Unknown"
        result["member_count"] = chat.members_count
        result["description"] = chat.description
        # Handle verification status (Pyrogram 2.x uses different attribute)
        # Try new API first, fall back to legacy
        try:
            if hasattr(chat, 'is_verified'):
                result["is_verified"] = chat.is_verified
            else:
                result["is_verified"] = False
        except:
            result["is_verified"] = False
            
        result["is_restricted"] = getattr(chat, 'is_restricted', False)
        result["is_scam"] = getattr(chat, 'is_scam', False)
        result["is_fake"] = getattr(chat, 'is_fake', False)
        
        # Check if it's a forum (topics enabled)
        result["topics_enabled"] = getattr(chat, 'is_forum', False)
        
        # Get slow mode delay
        result["slow_mode"] = getattr(chat, 'slow_mode_delay', None)
        
        # Check linked chat (discussion group)
        if chat.linked_chat:
            result["linked_chat_id"] = chat.linked_chat.id
            result["linked_chat_title"] = chat.linked_chat.title
            result["comments_enabled"] = True
        
        # Try to get last message ID
        try:
            async for msg in client.get_chat_history(chat_id, limit=1):
                result["last_message_id"] = msg.id
                # Try to extract creation date from first message ID
                # Telegram message IDs are sequential, so ID ~1 is near creation
                break
        except:
            pass
        
        # Try to estimate creation date from chat
        if hasattr(chat, 'date') and chat.date:
            result["creation_date"] = chat.date
        
    except Exception as e:
        error_str = str(e).upper()
        if "CHANNEL_PRIVATE" in error_str:
            result["error"] = get_string(user_id, "report_error_private")
        elif "USERNAME_NOT_OCCUPIED" in error_str:
            result["error"] = get_string(user_id, "report_error_invalid")
        elif "CHAT_ID_INVALID" in error_str:
            result["error"] = get_string(user_id, "report_error_invalid")
        elif "PEER_ID_INVALID" in error_str:
            result["error"] = get_string(user_id, "report_error_private")
        else:
            result["error"] = get_string(user_id, "request_error", e=e)
    
    return result


def format_info_response(info: dict, link_type: str, topic_id: int = None, thread_id: int = None, user_id: int = 0) -> str:
    """Render translated labels without changing source names/descriptions."""
    tr = lambda key, **kwargs: get_string(user_id, "report_" + key, **kwargs)
    if info.get('error'):
        return info['error']
    lines = [f"**{tr('info_heading')}**\n"]
    for label, field in [('title','title'), ('username','username'), ('id','id'), ('type','type'), ('members','member_count'), ('last_post','last_message_id')]:
        value = info.get(field)
        if not value:
            continue
        if field == 'type':
            value = get_report_type(user_id, value)
        elif field == 'username':
            value = '@' + value
        elif field == 'member_count':
            value = f"{value:,}"
        lines.append(f"**{tr(label)}:** {value}")
    if info.get('creation_date'):
        try:
            lines.append(f"**{tr('created')}:** {info['creation_date'].strftime('%Y-%m-%d')}")
        except (AttributeError, TypeError, ValueError):
            pass
    lines.append(f"\n**{tr('comments')}:** {tr('enabled' if info.get('comments_enabled') else 'disabled')}")
    lines.append(f"**{tr('topics')}:** {tr('enabled' if info.get('topics_enabled') else 'disabled')}")
    if info.get('slow_mode'):
        lines.append(f"**{tr('slow')}:** {info['slow_mode']} s")
    if topic_id:
        lines.append(f"\n**{tr('topic_id')}:** {topic_id}")
    if thread_id:
        lines.append(f"\n**{tr('thread_id')}:** {thread_id}")
    if info.get('linked_chat_id'):
        lines.append(f"\n**{tr('discussion')}:**")
        if info.get('linked_chat_title'):
            lines.append(f"  • {tr('title')}: {info['linked_chat_title']}")
        lines.append(f"  • ID: `{info['linked_chat_id']}`")
    if info.get('description'):
        description = str(info['description'])
        if len(description) > 200:
            description = description[:200] + '...'
        lines.append(f"\n**{tr('description')}:**\n{description}")
    flags = [tr(label) for field, label in [('is_verified','verified'), ('is_scam','scam'), ('is_fake','fake'), ('is_restricted','restricted')] if info.get(field)]
    if flags:
        lines.append(f"\n**{tr('flags')}:** {', '.join(flags)}")
    return "\n".join(lines)


@Client.on_message(filters.command("info") & filters.private)
async def info_command_handler(client: Client, message: Message):
    """
    /info command handler.
    
    Usage:
        /info https://t.me/channelname/123
        /info https://t.me/c/1234567890/123
        /info https://t.me/channelname
    """
    user_id = message.chat.id
    
    # Parse command arguments
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            get_string(user_id, 'info_usage'),
            **build_reply_kwargs_from_message(message)
        )
        return
    
    url = parts[1].strip()
    
    # Parse link
    chat_id, message_id, topic_id, thread_id, link_type, error = parse_info_link(url, user_id=user_id)
    
    if error:
        await message.reply(
            get_string(user_id, "error_generic", error=error),
            **build_reply_kwargs_from_message(message)
        )
        return
    
    status_msg = await message.reply(get_string(user_id, 'info_fetching'))
    
    # Try with bot client first (for public channels)
    if link_type == "public" or link_type == "channel":
        try:
            info = await get_chat_info(client, chat_id, user_id=user_id)
            if not info.get("error"):
                response = format_info_response(info, link_type, topic_id, thread_id, user_id=user_id)
                await status_msg.edit_text(response)
                return
        except:
            pass
    
    # Use user session for private channels
    user_data = await async_db.find_user(user_id)
    if not get(user_data, 'logged_in', False) or not user_data.get('session'):
        await status_msg.edit_text(
            get_string(user_id, "need_login")
        )
        return
    
    acc, error = await create_user_client(user_data['session'])
    if error:
        await status_msg.edit_text(get_string(user_id, "connection_error", e=error))
        return
    
    try:
        info = await get_chat_info(acc, chat_id, user_id=user_id)
        response = format_info_response(info, link_type, topic_id, thread_id, user_id=user_id)
        await status_msg.edit_text(response)
    finally:
        await safe_disconnect(acc)
