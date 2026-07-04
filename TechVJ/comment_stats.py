"""
Channel Post Comment Statistics Handler
Uses USER session (api_id + api_hash) with Pyrogram/Pyrofork

Supports link formats:
- https://t.me/channelname/123     (public channels)
- https://t.me/c/1234567890/123    (private channels)

FIX: Directly fetches discussion replies instead of relying on post.replies
     which is unreliable for private /c/ links.
"""

import re
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode

from config import API_ID, API_HASH, get_client_params
from database.db import database


def get(obj, key, default=None):
    """Helper: safely get value from dict."""
    try:
        return obj[key]
    except:
        return default


async def create_user_client(session_string: str, name: str = "comment_checker"):
    """Create and connect a Pyrogram user client from session string."""
    try:
        fp = get_client_params()
        client = Client(
            name,
            session_string=session_string,
            api_hash=API_HASH,
            api_id=API_ID,
            no_updates=True,  # Don't listen for updates - improves performance
            in_memory=True,   # Don't save session to disk
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
    """Safely disconnect the client, ignoring any errors."""
    if client:
        try:
            await client.disconnect()
        except:
            pass


def parse_telegram_link(url: str):
    """
    Parse Telegram post link and extract chat_id and message_id.
    
    Supports:
    - Public:  https://t.me/channelname/123
    - Private: https://t.me/c/1234567890/123
    
    Returns: (chat_id, message_id, error_message)
    """
    url = url.strip()
    
    # Private channel: https://t.me/c/1234567890/123
    # The /c/ format uses raw channel ID without -100 prefix
    private_match = re.search(r'https://t\.me/c/(\d+)/(\d+)', url)
    if private_match:
        # Telegram internal IDs for channels start with -100
        chat_id = int("-100" + private_match.group(1))
        message_id = int(private_match.group(2))
        return chat_id, message_id, None
    
    # Public channel: https://t.me/channelname/123
    public_match = re.search(r'https://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)', url)
    if public_match:
        chat_id = public_match.group(1)  # Keep as username string
        message_id = int(public_match.group(2))
        return chat_id, message_id, None
    
    return None, None, "Invalid Telegram post link format"


async def get_comment_stats(acc: Client, chat_id, message_id: int):
    """
    Fetch comment statistics for a channel post.
    
    KEY FIX: Instead of checking post.replies (unreliable for /c/ links),
    we directly attempt to fetch discussion replies. If it works, comments
    are enabled. If it throws MSG_ID_INVALID, comments are disabled.
    
    Returns dict with:
    - comments_enabled: bool
    - total_count: int
    - first_comment_id: int or None  (oldest comment)
    - last_comment_id: int or None   (newest comment)
    - channel_title: str
    - error: str or None
    """
    result = {
        "comments_enabled": False,
        "total_count": 0,
        "first_comment_id": None,
        "last_comment_id": None,
        "channel_title": "Unknown",
        "error": None
    }
    
    # =========================================================================
    # STEP 1: Resolve chat_id and get channel info
    # =========================================================================
    resolved_chat_id = chat_id
    
    if isinstance(chat_id, str):
        # Public channel - resolve username to ID
        try:
            chat_info = await acc.get_chat(chat_id)
            resolved_chat_id = chat_info.id
            result["channel_title"] = chat_info.title or f"@{chat_id}"
        except Exception as e:
            error_str = str(e).upper()
            if "USERNAME_NOT_OCCUPIED" in error_str:
                result["error"] = f"Channel @{chat_id} does not exist"
            elif "CHANNEL_PRIVATE" in error_str:
                result["error"] = f"Channel @{chat_id} is private. Join it first."
            else:
                result["error"] = f"Cannot access channel: {e}"
            return result
    else:
        # Private channel - try to get title
        try:
            chat_info = await acc.get_chat(resolved_chat_id)
            result["channel_title"] = chat_info.title or str(resolved_chat_id)
        except Exception as e:
            error_str = str(e).upper()
            if "CHANNEL_PRIVATE" in error_str or "CHAT_ID_INVALID" in error_str:
                result["error"] = "Cannot access this channel. Make sure you've joined it."
                return result
            # If we just can't get the title, continue anyway
            result["channel_title"] = str(resolved_chat_id)
    
    # =========================================================================
    # STEP 2: Verify the post exists
    # =========================================================================
    try:
        post = await acc.get_messages(resolved_chat_id, message_id)
        if not post or post.empty:
            result["error"] = f"Post #{message_id} not found"
            return result
    except Exception as e:
        error_str = str(e).upper()
        if "CHANNEL_PRIVATE" in error_str or "CHAT_ID_INVALID" in error_str:
            result["error"] = "Cannot access this channel. Make sure you've joined it."
        elif "MSG_ID_INVALID" in error_str:
            result["error"] = f"Message #{message_id} does not exist"
        else:
            result["error"] = f"Cannot fetch post: {e}"
        return result
    
    # =========================================================================
    # STEP 3: Directly fetch discussion replies (THE KEY FIX)
    # 
    # Instead of checking post.replies attribute (which is unreliable for
    # private /c/ links), we directly try to fetch discussion replies.
    # - If successful: comments are enabled
    # - If MSG_ID_INVALID error: comments are disabled for this post
    # =========================================================================
    try:
        comments = []
        
        # get_discussion_replies() fetches comments in the discussion thread
        # NOTE: Order is NOT guaranteed by the API, so we must sort later
        async for comment in acc.get_discussion_replies(resolved_chat_id, message_id):
            if comment and not comment.empty:
                comments.append(comment)
                # Safety limit to prevent memory issues on posts with many comments
                if len(comments) >= 10000:
                    break
        
        # If we got here without error, comments ARE enabled
        result["comments_enabled"] = True
        result["total_count"] = len(comments)
        
        if comments:
            # =========================================================
            # KEY FIX: Sort comments by ID ascending to ensure:
            # - first_comment_id = smallest ID = oldest comment
            # - last_comment_id  = largest ID  = newest comment
            # 
            # The API may return comments in inconsistent order,
            # especially for private /c/ links. Sorting guarantees
            # that first_id < last_id always.
            # =========================================================
            comments.sort(key=lambda c: c.id)
            
            result["first_comment_id"] = comments[0].id   # Smallest ID = oldest
            result["last_comment_id"] = comments[-1].id   # Largest ID = newest
        # else: comments enabled but none written yet (total_count = 0)
        
    except Exception as e:
        error_str = str(e).upper()
        
        # MSG_ID_INVALID means the post has no discussion thread
        # This is the definitive way to know comments are disabled
        if "MSG_ID_INVALID" in error_str:
            result["error"] = "Comments are disabled for this post"
            
        # PEER_ID_INVALID means we can't access the linked discussion group
        elif "PEER_ID_INVALID" in error_str:
            result["error"] = "Cannot access the discussion group. You may need to join it."
            
        # CHANNEL_PRIVATE for the discussion group
        elif "CHANNEL_PRIVATE" in error_str:
            result["error"] = "The discussion group is private. Join it first."
            
        else:
            result["error"] = f"Error fetching comments: {e}"
        
        return result
    
    return result


# =============================================================================
# HANDLER: Responds to /comments command with a post link
# Usage: /comments https://t.me/channelname/123
# =============================================================================

@Client.on_message(filters.command("comments") & filters.private)
async def comments_command_handler(client: Client, message: Message):
    """
    Handler for /comments command.
    Usage: /comments <telegram_post_link>
    """
    # Check if link is provided
    if len(message.text.split()) < 2:
        await message.reply(
            "**Usage:** `/comments <post_link>`\n\n"
            "**Examples:**\n"
            "• `/comments https://t.me/channelname/123`\n"
            "• `/comments https://t.me/c/1234567890/123`",
            parse_mode=ParseMode.DISABLED
        )
        return
    
    # Extract the link
    link = message.text.split(None, 1)[1].strip()
    
    # Validate link format
    if "t.me/" not in link:
        await message.reply("Please provide a valid Telegram post link.")
        return
    
    # Parse the link
    chat_id, message_id, parse_error = parse_telegram_link(link)
    if parse_error:
        await message.reply(f"**Error:** {parse_error}")
        return
    
    # Check user session
    user_data = database.find_one({'chat_id': message.chat.id})
    if not get(user_data, 'logged_in', False) or not user_data.get('session'):
        await message.reply(
            "You need to login first!\n"
            "Use /login to authenticate with your Telegram account."
        )
        return
    
    # Send processing status
    status_msg = await message.reply("Analyzing post...")
    
    # Create user client
    acc, error = await create_user_client(user_data['session'], f"comments_{message.chat.id}")
    if error:
        await status_msg.edit_text(f"**Connection Error:** {error}")
        return
    
    try:
        # Get comment statistics
        stats = await get_comment_stats(acc, chat_id, message_id)
        
        # Build response
        if stats["error"]:
            response = (
                f"**Channel:** {stats['channel_title']}\n"
                f"**Post ID:** {message_id}\n\n"
                f"**Error:** {stats['error']}"
            )
        elif stats["total_count"] == 0:
            response = (
                f"**Channel:** {stats['channel_title']}\n"
                f"**Post ID:** {message_id}\n\n"
                "**Comments:** Enabled\n"
                "**Status:** No comments yet"
            )
        else:
            response = (
                f"**Channel:** {stats['channel_title']}\n"
                f"**Post ID:** {message_id}\n\n"
                f"**Comments:** Enabled\n\n"
                f"**Statistics:**\n"
                f"• Total Comments: **{stats['total_count']}**\n"
                f"• First Comment ID: **{stats['first_comment_id']}**\n"
                f"• Last Comment ID: **{stats['last_comment_id']}**"
            )
        
        await status_msg.edit_text(response, parse_mode=ParseMode.DISABLED)
        
    except Exception as e:
        await status_msg.edit_text(f"**Unexpected Error:** {str(e)}")
    finally:
        await safe_disconnect(acc)


# =============================================================================
# NOTE: Auto-detect handler removed to avoid conflicts with save.py
# Use /comments command explicitly to check comment statistics
# =============================================================================
