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

import re
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ChatType
from core.reply_compat import build_reply_kwargs_from_message
from pyrogram.raw import functions, types

from config import API_ID, API_HASH, OWNER_ID
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
        client = Client(
            f"{name}_{uuid.uuid4().hex[:8]}",
            session_string=session_string,
            api_hash=API_HASH,
            api_id=API_ID,
            no_updates=True,
            in_memory=True,
            sleep_threshold=30
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


def parse_info_link(url: str):
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
    
    return None, None, None, None, None, "Invalid Telegram link format"


async def get_chat_info(client: Client, chat_id) -> dict:
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
            result["error"] = "Channel is private. You need to join it first."
        elif "USERNAME_NOT_OCCUPIED" in error_str:
            result["error"] = "This username does not exist."
        elif "CHAT_ID_INVALID" in error_str:
            result["error"] = "Invalid chat ID."
        elif "PEER_ID_INVALID" in error_str:
            result["error"] = "Cannot access this chat. Make sure you're a member."
        else:
            result["error"] = f"Error: {e}"
    
    return result


def format_info_response(info: dict, link_type: str, topic_id: int = None, thread_id: int = None) -> str:
    """Format the info response message."""
    if info.get("error"):
        return f"**Error:** {info['error']}"
    
    lines = ["**📊 Channel/Group Info**\n"]
    
    if info["title"]:
        lines.append(f"**Title:** {info['title']}")
    
    if info["username"]:
        lines.append(f"**Username:** @{info['username']}")
    
    if info["id"]:
        lines.append(f"**ID:** `{info['id']}`")
    
    if info["type"]:
        lines.append(f"**Type:** {info['type']}")
    
    if info["member_count"]:
        lines.append(f"**Members:** {info['member_count']:,}")
    
    if info["last_message_id"]:
        lines.append(f"**Last Post ID:** {info['last_message_id']}")
    
    if info.get("creation_date"):
        try:
            lines.append(f"**Created:** {info['creation_date'].strftime('%Y-%m-%d')}")
        except:
            pass
    
    lines.append(f"\n**Comments:** {'✅ Enabled' if info['comments_enabled'] else '❌ Disabled'}")
    lines.append(f"**Topics/Forum:** {'✅ Enabled' if info['topics_enabled'] else '❌ Disabled'}")
    
    if info.get("slow_mode"):
        lines.append(f"**Slow Mode:** {info['slow_mode']}s")
    
    if topic_id:
        lines.append(f"\n**Topic ID:** {topic_id}")
    
    if thread_id:
        lines.append(f"\n**Thread ID:** {thread_id}")
    
    if info["linked_chat_id"]:
        linked_title = info.get("linked_chat_title", "")
        lines.append(f"\n**Discussion Group:**")
        if linked_title:
            lines.append(f"  • Name: {linked_title}")
        lines.append(f"  • ID: `{info['linked_chat_id']}`")
    
    if info.get("description"):
        # Truncate long descriptions
        desc = info["description"]
        if len(desc) > 200:
            desc = desc[:200] + "..."
        lines.append(f"\n**Description:**\n{desc}")
    
    # Status flags
    flags = []
    if info.get("is_verified"):
        flags.append("✅ Verified")
    if info.get("is_scam"):
        flags.append("⚠️ SCAM")
    if info.get("is_fake"):
        flags.append("⚠️ FAKE")
    if info.get("is_restricted"):
        flags.append("🔒 Restricted")
    
    if flags:
        lines.append(f"\n**Flags:** {', '.join(flags)}")
    
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
            "**Usage:** `/info <telegram_link>`\n\n"
            "**Examples:**\n"
            "• `/info https://t.me/channelname`\n"
            "• `/info https://t.me/channelname/123`\n"
            "• `/info https://t.me/c/1234567890/123`\n"
            "• `/info https://t.me/c/1234567890/123/456` (topic)",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    url = parts[1].strip()
    
    # Parse link
    chat_id, message_id, topic_id, thread_id, link_type, error = parse_info_link(url)
    
    if error:
        await message.reply(
            f"**Error:** {error}",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    status_msg = await message.reply("Fetching info...")
    
    # Try with bot client first (for public channels)
    if link_type == "public" or link_type == "channel":
        try:
            info = await get_chat_info(client, chat_id)
            if not info.get("error"):
                response = format_info_response(info, link_type, topic_id, thread_id)
                await status_msg.edit_text(response)
                return
        except:
            pass
    
    # Use user session for private channels
    user_data = await async_db.find_user(user_id)
    if not get(user_data, 'logged_in', False) or not user_data.get('session'):
        await status_msg.edit_text(
            "**Error:** You need to /login first to access private channels."
        )
        return
    
    acc, error = await create_user_client(user_data['session'])
    if error:
        await status_msg.edit_text(f"**Connection Error:** {error}")
        return
    
    try:
        info = await get_chat_info(acc, chat_id)
        response = format_info_response(info, link_type, topic_id, thread_id)
        await status_msg.edit_text(response)
    finally:
        await safe_disconnect(acc)
