# Video Upload Handler - Optimal video sending with fallback support
# Sends videos as video messages when possible, falls back to document if needed
# Handles large files with automatic splitting
# Works with both bot token and user session

import os
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ChatType, ParseMode
from pyrogram.errors import FloodWait, MediaEmpty, FilePartMissing, FilePartInvalid

from TechVJ.file_splitter import (
    needs_splitting, split_file, cleanup_chunks,
    format_size, MAX_TELEGRAM_FILE_SIZE, get_file_size
)
from TechVJ.ffmpeg_utils import get_video_info, extract_thumbnail, get_ffmpeg_path

# Supported video extensions (case-insensitive matching)
VIDEO_EXTENSIONS = {'.mp4', '.mv4', '.mkv', '.mov', '.avi', '.webm', '.m4v', '.flv', '.wmv', '.3gp', '.ts'}


def is_video_file(filename: str) -> bool:
    """Check if filename has a video extension (case-insensitive)"""
    if not filename:
        return False
    ext = os.path.splitext(filename)[1].lower()
    return ext in VIDEO_EXTENSIONS


def get_file_extension(filename: str) -> str:
    """Get lowercase file extension from filename"""
    if not filename:
        return ""
    return os.path.splitext(filename)[1].lower()


# Progress callback for download/upload status
_progress_cache = {}


import logging as _logging
_logger = _logging.getLogger(__name__)


async def progress_callback(current: int, total: int, message: Message, action: str):
    """Update progress message (throttled to avoid flood)"""
    try:
        key = f"{message.chat.id}_{message.id}_{action}"
        now = asyncio.get_running_loop().time()
        
        # Throttle: har 5 sekundda bir yangilash (FloodWait oldini olish)
        if key in _progress_cache and now - _progress_cache[key] < 5:
            return
        _progress_cache[key] = now
        
        percent = (current * 100) / total if total > 0 else 0
        current_mb = current / (1024 * 1024)
        total_mb = total / (1024 * 1024)
        
        # Tezlikni hisoblash
        speed_text = ""
        eta_text = ""
        if key + "_start" not in _progress_cache:
            _progress_cache[key + "_start"] = (now, current)
        else:
            start_time, start_bytes = _progress_cache[key + "_start"]
            elapsed = now - start_time
            if elapsed > 0:
                speed = (current - start_bytes) / elapsed
                speed_mb = speed / (1024 * 1024)
                speed_text = f"\nTezlik: `{speed_mb:.1f} MB/s`"
                remaining = total - current
                if speed > 0:
                    eta_sec = int(remaining / speed)
                    if eta_sec > 60:
                        eta_text = f"\nQoldi: ~{eta_sec // 60}m {eta_sec % 60}s"
                    else:
                        eta_text = f"\nQoldi: ~{eta_sec}s"
        
        progress_bar = _make_progress_bar(percent)
        
        await message.edit_text(
            f"**{action}**\n"
            f"{progress_bar} {percent:.1f}%\n"
            f"`{current_mb:.1f} MB / {total_mb:.1f} MB`"
            f"{speed_text}{eta_text}"
        )
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else 5
        _logger.debug(f"Progress FloodWait: {wait}s")
        await asyncio.sleep(wait)
    except Exception as e:
        _logger.debug(f"Progress update error: {e}")


def _make_progress_bar(percent: float, length: int = 12) -> str:
    filled = int(length * percent / 100)
    return "█" * filled + "░" * (length - filled)


def clear_progress_cache(message: Message, action: str):
    """Clear progress cache after operation completes"""
    key = f"{message.chat.id}_{message.id}_{action}"
    _progress_cache.pop(key, None)
    _progress_cache.pop(key + "_start", None)


# Custom filter: private chats and channels, exclude t.me links and commands
def video_filter(_, client, message: Message) -> bool:
    """
    Filter for video conversion handler.
    - Must be private chat or channel
    - Must have document or video
    - Must NOT contain t.me links (handled by save.py)
    - Must NOT be a command
    - Must NOT be outgoing (bot's own messages or user session uploads)
    - Must NOT be from a bot (prevents re-processing media sent by bot itself)
    """
    if not message.chat:
        return False

    # Only private chats and channels
    if message.chat.type not in (ChatType.PRIVATE, ChatType.CHANNEL):
        return False

    # Skip outgoing messages (user session uploads TO bot chat)
    if message.outgoing:
        return False

    # Skip messages from bots (prevents re-processing bot's own forwards/copies)
    if message.from_user and message.from_user.is_bot:
        return False

    # Must have document or video
    if not message.document and not message.video:
        return False

    # Exclude messages with t.me links (handled by save.py)
    if message.text and "t.me/" in message.text:
        return False
    if message.caption and "t.me/" in message.caption:
        return False

    return True


video_media_filter = filters.create(video_filter)


@Client.on_message(video_media_filter)
async def optimal_video_handler(client: Client, message: Message):
    """
    Handler for video files - sends as video stream when possible,
    falls back to document if needed. Handles large files with splitting.
    Works for both private chats and channels.

    IMPORTANT: Skip if the user has an active session (logged in).
    When a user is logged in, save.py handles media delivery via user session.
    Without this check, user-session uploads get re-sent by bot API (duplicate).
    """
    # Skip for logged-in users — their media is handled by save.py
    if message.chat and message.chat.type == ChatType.PRIVATE and message.from_user:
        try:
            from database.async_db import async_db
            _udata = await async_db.find_user(message.from_user.id)
            if _udata and _udata.get('logged_in') and _udata.get('session'):
                return  # save.py handles this user's media
        except Exception:
            pass  # DB unavailable — let handler proceed

        # Skip pool session intermediate uploads (pool user → bot chat)
        # These are handled by copy_message + delete in save.py/session_manager
        # Detect: message sender is NOT the chat owner (user sent via pool session)
        try:
            if message.from_user.id != message.chat.id:
                # Someone else sent a video to bot's DM — likely a pool session upload
                # Check if sender has NO active bot interaction (no save.py request)
                _sender_data = await async_db.find_user(message.from_user.id)
                if not _sender_data:
                    return  # Unknown user sending video to bot — skip (pool session)
        except Exception:
            pass

    file_id = None
    file_name = None
    file_size = 0
    is_document = False
    
    try:
        # === STEP 1: Extract file information ===
        if message.document:
            file_name = message.document.file_name
            file_size = message.document.file_size or 0
            file_id = message.document.file_id
            is_document = True
            
            # Validate: must have filename
            if not file_name:
                await message.reply(
                    "Cannot process: Document has no filename.",
                    quote=True
                )
                return
            
            # Validate: must be a video file extension
            if not is_video_file(file_name):
                return  # Silently ignore non-video documents
        
        elif message.video:
            file_name = message.video.file_name or "video.mp4"
            file_size = message.video.file_size or 0
            file_id = message.video.file_id
            is_document = False
        
        else:
            return  # No supported media
        
        # === STEP 2: Send status message ===
        status_msg = await message.reply(
            f"Processing **{file_name}**...\n"
            f"Size: `{format_size(file_size)}`",
            quote=True
        )
        
        # === STEP 3: Handle based on file size ===
        
        # Case A: File needs splitting (>2GB)
        if file_size > MAX_TELEGRAM_FILE_SIZE:
            await handle_large_file(client, message, status_msg, file_id, file_name, file_size)
            return
        
        # Case B: Try to send as video message first
        send_success = await try_send_as_video(
            client, message, status_msg, file_id, file_name
        )
        
        # Case C: Fallback to document if video send failed
        if not send_success:
            await send_as_document(
                client, message, status_msg, file_id, file_name
            )
    
    except FloodWait as e:
        wait_time = e.value if hasattr(e, 'value') else 30
        await message.reply(
            f"Rate limited. Please wait {wait_time} seconds.",
            quote=True
        )

    except FilePartInvalid:
        # FILE_PART_INVALID — Pyrogram save_file worker spam.
        # Do NOT retry — this means the upload is fundamentally broken
        # (session issue, corrupted file_id, or DC mismatch).
        _logger.warning("FILE_PART_INVALID for %s — skipping (no retry)", file_name or "unknown")
        try:
            if status_msg:
                await status_msg.edit_text(
                    f"Upload failed for **{file_name or 'video'}** — file part error.\n"
                    "Try sending the file again or use /login to refresh your session."
                )
        except Exception:
            pass

    except Exception as e:
        error_str = str(e)
        # Don't spam errors for common non-critical issues
        if "QUERY_ID_INVALID" not in error_str and "MESSAGE_NOT_MODIFIED" not in error_str:
            await message.reply(
                f"Error processing video: `{error_str[:200]}`",
                quote=True
            )


async def try_send_as_video(
    client: Client,
    message: Message,
    status_msg: Message,
    file_id: str,
    file_name: str
) -> bool:
    """
    Attempt to send file as a video message (streamable).
    Returns True on success, False if fallback to document is needed.
    """
    try:
        await status_msg.edit_text(f"Sending **{file_name}** as video...")
        
        # Send as video with streaming support
        await client.send_video(
            chat_id=message.chat.id,
            video=file_id,
            caption=message.caption,
            caption_entities=message.caption_entities,
            reply_to_message_id=message.id,
            supports_streaming=True,
            file_name=file_name
        )
        
        await status_msg.edit_text(f"Sent **{file_name}** as video message.")
        return True
    
    except (MediaEmpty, FilePartMissing):
        # These errors require fallback to document
        return False
    
    except FloodWait as e:
        # Re-raise FloodWait to be handled by caller
        raise
    
    except Exception as e:
        error_msg = str(e).upper()
        
        # Errors that indicate we should try document instead
        fallback_errors = [
            "VIDEO_CONTENT_TYPE_INVALID",
            "VIDEO_FILE_INVALID", 
            "MEDIA_INVALID",
            "FILE_PARTS_INVALID",
            "DOCUMENT_INVALID",
            "MEDIA_EMPTY",
            "WEBP_STICKER_EXPECTED",
            "FILE_REFERENCE_EXPIRED"
        ]
        
        for err in fallback_errors:
            if err in error_msg:
                return False
        
        # For other errors, still try fallback
        return False


async def send_as_document(
    client: Client,
    message: Message,
    status_msg: Message,
    file_id: str,
    file_name: str
):
    """Send file as document (fallback when video send fails)"""
    try:
        await status_msg.edit_text(
            f"Video format not supported for streaming.\n"
            f"Sending **{file_name}** as document..."
        )
        
        await client.send_document(
            chat_id=message.chat.id,
            document=file_id,
            caption=message.caption,
            caption_entities=message.caption_entities,
            reply_to_message_id=message.id,
            file_name=file_name
        )
        
        await status_msg.edit_text(f"Sent **{file_name}** as document.")
    
    except FloodWait as e:
        raise
    
    except Exception as e:
        await status_msg.edit_text(f"Failed to send: `{str(e)[:200]}`")


async def handle_large_file(
    client: Client,
    message: Message,
    status_msg: Message,
    file_id: str,
    file_name: str,
    file_size: int
):
    """
    Handle files larger than Telegram's 2GB limit.
    Downloads, splits, and uploads in parts.
    """
    download_path = None
    chunk_paths = []
    
    try:
        # Notify user about large file handling
        await status_msg.edit_text(
            f"**{file_name}** exceeds 2GB limit.\n"
            f"Size: `{format_size(file_size)}`\n\n"
            f"Downloading for splitting..."
        )
        
        # Create downloads directory if needed
        os.makedirs("downloads", exist_ok=True)
        
        # Sanitize filename for filesystem
        safe_filename = "".join(c for c in file_name if c.isalnum() or c in '._-')
        download_path = f"downloads/{message.chat.id}_{message.id}_{safe_filename}"
        
        # Download the file with progress
        downloaded_file = await client.download_media(
            message,
            file_name=download_path,
            progress=progress_callback,
            progress_args=(status_msg, "Downloading")
        )
        
        # Clear download progress cache
        clear_progress_cache(status_msg, "Downloading")
        
        # Verify download
        if not downloaded_file or not os.path.exists(downloaded_file):
            await status_msg.edit_text("Download failed. File not found.")
            return
        
        # Use actual downloaded path
        download_path = downloaded_file
        
        actual_size = get_file_size(download_path)
        await status_msg.edit_text(
            f"Downloaded `{format_size(actual_size)}`\n"
            f"Splitting into parts..."
        )
        
        # Split + upload: har chunk yaratilishi bilan darhol upload qilinadi.
        # Disk: original + 1 chunk (eski usulda original + BARCHA chunk edi).
        from TechVJ.file_splitter import split_file_iter
        
        for chunk_path, part_num, total_parts in split_file_iter(download_path):
            chunk_name = os.path.basename(chunk_path)
            chunk_size_bytes = get_file_size(chunk_path)
            
            await status_msg.edit_text(
                f"Uploading part **{part_num}/{total_parts}**\n"
                f"`{chunk_name}` ({format_size(chunk_size_bytes)})"
            )
            
            # Prepare caption for first part only
            if part_num == 1 and message.caption:
                part_caption = f"**Part {part_num}/{total_parts}**\n\n{message.caption}"
            else:
                part_caption = f"**Part {part_num}/{total_parts}** - {file_name}"
            
            # Truncate caption if too long
            if part_caption and len(part_caption) > 1024:
                part_caption = part_caption[:1020] + "..."
            
            # Video metadata va thumbnail olish (har bir chunk uchun)
            thumb_path = None
            duration = 0
            width = 0
            height = 0
            
            if is_video_file(file_name):
                try:
                    info = await asyncio.to_thread(get_video_info, chunk_path)
                    if info:
                        duration = int(info.get('duration', 0))
                        width = info.get('width', 0)
                        height = info.get('height', 0)
                except Exception:
                    pass
                
                try:
                    thumb_path = await asyncio.to_thread(extract_thumbnail, chunk_path)
                except Exception:
                    pass
            
            # Try video first for video files, document for others
            sent_as_video = False
            if is_video_file(file_name):
                try:
                    await client.send_video(
                        chat_id=message.chat.id,
                        video=chunk_path,
                        caption=part_caption,
                        reply_to_message_id=message.id,
                        supports_streaming=True,
                        duration=duration,
                        width=width,
                        height=height,
                        thumb=thumb_path,
                        file_name=chunk_name,
                        progress=progress_callback,
                        progress_args=(status_msg, f"Uploading {part_num}/{total_parts}")
                    )
                    sent_as_video = True
                except Exception:
                    pass  # Will fallback to document
            
            if not sent_as_video:
                # Send as document
                await client.send_document(
                    chat_id=message.chat.id,
                    document=chunk_path,
                    caption=part_caption,
                    reply_to_message_id=message.id,
                    thumb=thumb_path,
                    file_name=chunk_name,
                    progress=progress_callback,
                    progress_args=(status_msg, f"Uploading {part_num}/{total_parts}")
                )
            
            # Clear upload progress cache
            clear_progress_cache(status_msg, f"Uploading {part_num}/{total_parts}")
            
            # Chunk va thumbnail o'chirish - disk bo'shatish
            for _tmp in [chunk_path, thumb_path]:
                try:
                    if _tmp and os.path.exists(_tmp):
                        os.remove(_tmp)
                except Exception:
                    pass
            
            # Rate limit protection between uploads
            if part_num < total_parts:
                await asyncio.sleep(3)
        
        # Final status
        await status_msg.edit_text(
            f"Upload complete!\n\n"
            f"**{file_name}** split into {total_parts} parts.\n"
            f"Use file joining software to combine."
        )
    
    except FloodWait as e:
        wait_time = e.value if hasattr(e, 'value') else 60
        await status_msg.edit_text(
            f"Rate limited. Waiting {wait_time} seconds...\n"
            f"The upload will continue automatically."
        )
        await asyncio.sleep(wait_time)
        # Retry could be implemented here
    
    except Exception as e:
        await status_msg.edit_text(f"Error handling large file: `{str(e)[:200]}`")
    
    finally:
        # Cleanup: remove downloaded file and remaining chunks
        if download_path and os.path.exists(download_path):
            try:
                os.remove(download_path)
            except Exception:
                pass
        
        if chunk_paths:
            cleanup_chunks(chunk_paths)
