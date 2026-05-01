"""
Command Handlers for Telegram Bot

Implements:
- /start - Welcome message
- /help - Usage instructions
- /download - Download from URL
- /stop - Cancel all user downloads
- /status - Check queue status
- /login - Login with session string
- /logout - Remove session
- /ban, /unban, /stats - Owner commands

All handlers check blocked status before processing.
"""

import re
import logging
from typing import Optional

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait, ChatAdminRequired

from core.download_manager import get_download_manager
from core.progress_manager import get_progress_manager
from core.mongo_store import get_mongo_store
from core.state_store import state_store
from core.client_factory import validate_session
from config import OWNER_ID, API_ID, API_HASH

logger = logging.getLogger(__name__)


# URL patterns for Telegram links
TG_URL_PATTERN = re.compile(
    r'https?://t\.me/(?:c/(\d+)|([a-zA-Z][a-zA-Z0-9_]{3,}))/(\d+(?:,\d+)*(?:-\d+)?)'
)


def parse_telegram_url(url: str) -> Optional[dict]:
    """
    Parse a Telegram message URL.
    
    Supported formats:
    - https://t.me/c/123456/100 (private channel)
    - https://t.me/channel/100 (public channel)
    - https://t.me/c/123456/100,101,102 (multiple messages)
    - https://t.me/c/123456/100-110 (range)
    
    Returns dict with chat_id and message_ids, or None if invalid.
    """
    match = TG_URL_PATTERN.match(url.strip())
    if not match:
        return None
    
    private_id = match.group(1)
    public_username = match.group(2)
    msg_part = match.group(3)
    
    # Parse message IDs
    if '-' in msg_part and ',' not in msg_part:
        # Range: 100-110
        parts = msg_part.split('-')
        try:
            start, end = int(parts[0]), int(parts[1])
        except ValueError:
            return None
        if start <= end:
            message_ids = list(range(start, end + 1))
        else:
            message_ids = list(range(start, end - 1, -1))
    else:
        # Comma-separated or single
        try:
            message_ids = [int(x.strip()) for x in msg_part.split(',')]
        except ValueError:
            return None
    
    # Build chat_id
    if private_id:
        chat_id = int(f"-100{private_id}")
        is_private = True
    else:
        chat_id = public_username
        is_private = False
    
    return {
        'chat_id': chat_id,
        'message_ids': message_ids,
        'is_private': is_private
    }


async def check_blocked(user_id: int) -> bool:
    """Check if user is blocked."""
    mongo = get_mongo_store()
    if mongo:
        return await mongo.is_blocked(user_id)
    return False


async def get_user_session(user_id: int) -> Optional[str]:
    """Get user's session string from MongoDB."""
    mongo = get_mongo_store()
    if mongo:
        return await mongo.get_session(user_id)
    return None


# ==================== PUBLIC COMMANDS ====================

async def start_handler(client: Client, message: Message):
    """Handle /start command."""
    if await check_blocked(message.from_user.id):
        await message.reply("You are blocked from using this bot.")
        return
    
    await message.reply(
        "**Media Downloader Bot**\n\n"
        "Send me a Telegram link to download media:\n"
        "`https://t.me/c/123456/100`\n"
        "`https://t.me/channel/100,101,102`\n"
        "`https://t.me/c/123456/100-110`\n\n"
        "**Commands:**\n"
        "/status - Check queue status\n"
        "/stop - Cancel all downloads\n"
        "/login - Login with session\n"
        "/logout - Remove session\n"
        "/help - Detailed help"
    )


async def help_handler(client: Client, message: Message):
    """Handle /help command."""
    if await check_blocked(message.from_user.id):
        return
    
    await message.reply(
        "**Usage Guide**\n\n"
        "**URL Formats:**\n"
        "- `https://t.me/c/123456/100` - Private channel\n"
        "- `https://t.me/channel/100` - Public channel\n"
        "- `https://t.me/c/123456/100,101,102` - Multiple posts\n"
        "- `https://t.me/c/123456/100-110` - Range of posts\n\n"
        "**Commands:**\n"
        "- /start - Start the bot\n"
        "- /status - Check download queue status\n"
        "- /stop - Cancel all your downloads\n"
        "- /login - Login with Pyrogram session\n"
        "- /logout - Remove your session\n\n"
        "**Notes:**\n"
        "- Login required for private channels\n"
        "- Large files (4GB+) supported\n"
        "- Downloads resume after restart"
    )


async def status_handler(client: Client, message: Message):
    """Handle /status command."""
    if await check_blocked(message.from_user.id):
        return
    
    dm = get_download_manager()
    if not dm:
        await message.reply("Download system not initialized.")
        return
    
    status = await dm.get_queue_status()
    user_tasks = await dm.get_user_task_count(message.from_user.id)
    
    await message.reply(
        f"**Queue Status**\n\n"
        f"Queue Size: {status['queue_size']}\n"
        f"Workers: {status['workers_active']}/{status['workers_total']}\n\n"
        f"Your Tasks: {user_tasks}\n\n"
        f"**Global Stats:**\n"
        f"Pending: {status.get('pending', 0)}\n"
        f"Downloading: {status.get('downloading', 0)}\n"
        f"Completed: {status.get('completed', 0)}\n"
        f"Failed: {status.get('failed', 0)}"
    )


async def stop_handler(client: Client, message: Message):
    """Handle /stop command - cancel all user downloads."""
    user_id = message.from_user.id
    
    if await check_blocked(user_id):
        return
    
    dm = get_download_manager()
    if not dm:
        await message.reply("Download system not initialized.")
        return
    
    cancelled = await dm.cancel_user_downloads(user_id)
    
    if cancelled > 0:
        await message.reply(f"Cancelled {cancelled} download(s).")
    else:
        await message.reply("No active downloads to cancel.")


async def login_handler(client: Client, message: Message):
    """Handle /login command."""
    user_id = message.from_user.id
    
    if await check_blocked(user_id):
        return
    
    # Check if already logged in
    existing = await get_user_session(user_id)
    if existing:
        await message.reply(
            "You already have a session saved.\n"
            "Use /logout first to remove it."
        )
        return
    
    await message.reply(
        "**Login**\n\n"
        "Send your Pyrogram session string.\n\n"
        "To get one, run:\n"
        "```python\n"
        "from pyrogram import Client\n"
        "app = Client('my', api_id=..., api_hash=...)\n"
        "async with app:\n"
        "    print(await app.export_session_string())\n"
        "```\n\n"
        "**Warning:** Your session will be deleted from chat for security."
    )


async def logout_handler(client: Client, message: Message):
    """Handle /logout command."""
    user_id = message.from_user.id
    
    if await check_blocked(user_id):
        return
    
    mongo = get_mongo_store()
    if not mongo:
        await message.reply("Database not available.")
        return
    
    deleted = await mongo.delete_session(user_id)
    if deleted:
        await message.reply("Session removed successfully.")
    else:
        await message.reply("You don't have a session saved.")


async def download_handler(client: Client, message: Message):
    """Handle download requests (text messages with URLs)."""
    user_id = message.from_user.id
    
    if await check_blocked(user_id):
        await message.reply("You are blocked from using this bot.")
        return
    
    text = message.text.strip() if message.text else ""
    
    # Check if it looks like a session string
    if text.startswith("1") and len(text) > 300 and " " not in text:
        await _handle_session_string(client, message, text)
        return
    
    # Parse URL
    parsed = parse_telegram_url(text)
    if not parsed:
        return  # Not a valid URL, ignore
    
    # Get user session for private channels
    session = await get_user_session(user_id)
    if not session and parsed['is_private']:
        await message.reply(
            "You need to login to download from private channels.\n"
            "Use /login to add your session."
        )
        return
    
    dm = get_download_manager()
    if not dm:
        await message.reply("Download system not available.")
        return
    
    # Process messages
    chat_id = parsed['chat_id']
    message_ids = parsed['message_ids']
    
    # Limit batch size
    if len(message_ids) > 100:
        await message.reply(f"Maximum 100 messages per request. You sent {len(message_ids)}.")
        return
    
    status_msg = await message.reply(f"Processing {len(message_ids)} message(s)...")
    
    enqueued = 0
    duplicates = 0
    no_media = 0
    errors = 0
    
    for msg_id in message_ids:
        try:
            # Fetch message
            # TODO: Use user session for private channels
            source_msg = await client.get_messages(chat_id, msg_id)
            
            if not source_msg or source_msg.empty:
                errors += 1
                continue
            
            if not source_msg.media:
                no_media += 1
                continue
            
            # Enqueue download
            task = await dm.enqueue_from_message(user_id, source_msg)
            if task:
                if task.status == "COMPLETED":
                    duplicates += 1
                elif task.status in ("PENDING", "DOWNLOADING"):
                    enqueued += 1
            else:
                duplicates += 1  # Likely duplicate
                
        except FloodWait as e:
            wait = getattr(e, 'value', getattr(e, 'x', 5))
            await status_msg.edit_text(f"Rate limited. Waiting {wait}s...")
            import asyncio
            await asyncio.sleep(wait)
            errors += 1
        except ChatAdminRequired:
            await status_msg.edit_text(
                "Cannot access this channel.\n"
                "Make sure the bot is a member, or use /login for private channels."
            )
            return
        except Exception as e:
            logger.error(f"Error processing message {msg_id}: {e}")
            errors += 1
    
    # Update status
    parts = []
    if enqueued > 0:
        parts.append(f"Enqueued: {enqueued}")
    if duplicates > 0:
        parts.append(f"Already downloaded: {duplicates}")
    if no_media > 0:
        parts.append(f"No media: {no_media}")
    if errors > 0:
        parts.append(f"Errors: {errors}")
    
    result_text = "**Result**\n\n" + "\n".join(parts) if parts else "No files processed."
    await status_msg.edit_text(result_text)


async def _handle_session_string(
    client: Client,
    message: Message,
    session_string: str
):
    """Handle session string login."""
    user_id = message.from_user.id
    
    # Delete message with session for security
    try:
        await message.delete()
    except Exception:
        pass
    
    # Validate session
    status_msg = await client.send_message(
        message.chat.id,
        "Validating session..."
    )
    
    is_valid, msg, user_info = await validate_session(
        API_ID, API_HASH, session_string
    )
    
    if not is_valid:
        await status_msg.edit_text(f"Invalid session: {msg}")
        return
    
    # Save session
    mongo = get_mongo_store()
    if not mongo:
        await status_msg.edit_text("Database not available.")
        return
    
    success = await mongo.save_session(
        user_id,
        session_string,
        metadata=user_info
    )
    
    if success:
        await status_msg.edit_text(
            f"Session saved!\n"
            f"Logged in as: {user_info.get('first_name', 'Unknown')}\n"
            f"You can now download from private channels."
        )
    else:
        await status_msg.edit_text("Failed to save session.")


# ==================== OWNER COMMANDS ====================

async def ban_handler(client: Client, message: Message):
    """Handle /ban command (owner only)."""
    if message.from_user.id != OWNER_ID:
        return
    
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        await message.reply("Usage: /ban <user_id> [reason]")
        return
    
    try:
        target_id = int(parts[1])
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    
    reason = parts[2] if len(parts) > 2 else ""
    
    mongo = get_mongo_store()
    if not mongo:
        await message.reply("Database not available.")
        return
    
    await mongo.block_user(target_id, message.from_user.id, reason)
    
    # Cancel their downloads
    dm = get_download_manager()
    if dm:
        await dm.cancel_user_downloads(target_id)
    
    await message.reply(f"User {target_id} banned." + (f" Reason: {reason}" if reason else ""))


async def unban_handler(client: Client, message: Message):
    """Handle /unban command (owner only)."""
    if message.from_user.id != OWNER_ID:
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Usage: /unban <user_id>")
        return
    
    try:
        target_id = int(parts[1])
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    
    mongo = get_mongo_store()
    if mongo:
        unblocked = await mongo.unblock_user(target_id)
        if unblocked:
            await message.reply(f"User {target_id} unbanned.")
        else:
            await message.reply(f"User {target_id} was not banned.")


async def stats_handler(client: Client, message: Message):
    """Handle /stats command (owner only)."""
    if message.from_user.id != OWNER_ID:
        return
    
    dm = get_download_manager()
    pm = get_progress_manager()
    mongo = get_mongo_store()
    
    status = await dm.get_queue_status() if dm else {}
    
    text = (
        f"**System Stats**\n\n"
        f"**Download Queue:**\n"
        f"- Size: {status.get('queue_size', 0)}\n"
        f"- Workers: {status.get('workers_active', 0)}/{status.get('workers_total', 150)}\n\n"
        f"**Database:**\n"
        f"- Pending: {status.get('pending', 0)}\n"
        f"- Downloading: {status.get('downloading', 0)}\n"
        f"- Completed: {status.get('completed', 0)}\n"
        f"- Failed: {status.get('failed', 0)}\n"
        f"- Canceled: {status.get('canceled', 0)}\n\n"
        f"**Progress Manager:**\n"
        f"- Active: {pm.get_active_count() if pm else 0}\n"
        f"- Queue: {pm.get_queue_size() if pm else 0}\n\n"
        f"**MongoDB:** {'Connected' if mongo and mongo.is_connected() else 'Disconnected'}"
    )
    
    await message.reply(text)


async def cleanup_handler(client: Client, message: Message):
    """Handle /cleanup command (owner only)."""
    if message.from_user.id != OWNER_ID:
        return
    
    parts = message.text.split()
    days = int(parts[1]) if len(parts) > 1 else 7
    
    deleted = await state_store.cleanup_old(days)
    await message.reply(f"Cleaned up {deleted} old tasks (>{days} days).")


def register_handlers(client: Client) -> None:
    """Register all handlers with the client."""
    
    # Public commands
    client.add_handler(MessageHandler(
        start_handler,
        filters.command(["start"]) & filters.private
    ))
    
    client.add_handler(MessageHandler(
        help_handler,
        filters.command(["help"]) & filters.private
    ))
    
    client.add_handler(MessageHandler(
        status_handler,
        filters.command(["status"]) & filters.private
    ))
    
    client.add_handler(MessageHandler(
        stop_handler,
        filters.command(["stop", "cancel"]) & filters.private
    ))
    
    client.add_handler(MessageHandler(
        login_handler,
        filters.command(["login"]) & filters.private
    ))
    
    client.add_handler(MessageHandler(
        logout_handler,
        filters.command(["logout"]) & filters.private
    ))
    
    # Owner commands
    client.add_handler(MessageHandler(
        ban_handler,
        filters.command(["ban"]) & filters.private
    ))
    
    client.add_handler(MessageHandler(
        unban_handler,
        filters.command(["unban"]) & filters.private
    ))
    
    client.add_handler(MessageHandler(
        stats_handler,
        filters.command(["stats"]) & filters.private
    ))
    
    client.add_handler(MessageHandler(
        cleanup_handler,
        filters.command(["cleanup"]) & filters.private
    ))
    
    # URL handler (must be last - catches all text)
    client.add_handler(MessageHandler(
        download_handler,
        filters.text & filters.private & ~filters.command([
            "start", "help", "status", "stop", "cancel",
            "login", "logout", "ban", "unban", "stats", "cleanup"
        ])
    ))
    
    logger.info("Command handlers registered")
