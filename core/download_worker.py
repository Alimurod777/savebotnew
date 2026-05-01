"""
Download Worker - Processes download jobs from queue

This module handles the actual download and upload logic:
- Download from source using user session
- Upload to user using bot client
- Progress reporting
- Error handling with retries
- Clean cancellation
"""

import asyncio
import os
import uuid
from typing import Optional
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified
import logging

from core.download_queue import DownloadJob, DownloadStatus
from core.download_cache import download_cache
from core.progress_reporter import progress_reporter
from core.session_manager import create_user_session, SessionInvalidError, SessionConnectionError
from core.error_handler import with_retry, ItemSkipError, FileReferenceError as FileRefError
from core.reply_compat import build_reply_kwargs

logger = logging.getLogger(__name__)

DOWNLOAD_TIMEOUT = 7200  # 2 hours
UPLOAD_TIMEOUT = 7200
TEMP_DIR = "downloads/temp"


def get_media_type(msg: Message) -> Optional[str]:
    """Determine media type from message."""
    if msg.video:
        return "video"
    elif msg.audio:
        return "audio"
    elif msg.photo:
        return "photo"
    elif msg.voice:
        return "voice"
    elif msg.video_note:
        return "video_note"
    elif msg.animation:
        return "animation"
    elif msg.document:
        return "document"
    elif msg.sticker:
        return "sticker"
    return None


def get_file_info(msg: Message) -> tuple:
    """Get file_unique_id and file_size from message."""
    if msg.video:
        return msg.video.file_unique_id, msg.video.file_size or 0
    elif msg.audio:
        return msg.audio.file_unique_id, msg.audio.file_size or 0
    elif msg.photo:
        return msg.photo.file_unique_id, msg.photo.file_size or 0
    elif msg.voice:
        return msg.voice.file_unique_id, msg.voice.file_size or 0
    elif msg.video_note:
        return msg.video_note.file_unique_id, msg.video_note.file_size or 0
    elif msg.animation:
        return msg.animation.file_unique_id, msg.animation.file_size or 0
    elif msg.document:
        return msg.document.file_unique_id, msg.document.file_size or 0
    elif msg.sticker:
        return msg.sticker.file_unique_id, msg.sticker.file_size or 0
    return None, 0


def get_file_name(msg: Message) -> str:
    """Get filename from message."""
    if msg.document and msg.document.file_name:
        return msg.document.file_name
    elif msg.video and msg.video.file_name:
        return msg.video.file_name
    elif msg.audio and msg.audio.file_name:
        return msg.audio.file_name
    elif msg.photo:
        return f"photo_{msg.photo.file_unique_id}.jpg"
    elif msg.voice:
        return f"voice_{msg.voice.file_unique_id}.ogg"
    elif msg.video_note:
        return f"videonote_{msg.video_note.file_unique_id}.mp4"
    elif msg.sticker:
        ext = ".webp" if not msg.sticker.is_animated else ".tgs"
        return f"sticker_{msg.sticker.file_unique_id}{ext}"
    elif msg.animation:
        return f"animation_{msg.animation.file_unique_id}.mp4"
    return f"file_{uuid.uuid4().hex[:8]}"


async def process_download_job(
    bot: Client,
    job: DownloadJob,
    session_string: str
) -> bool:
    """
    Process a single download job.
    
    This is the main worker function that:
    1. Downloads media using user session
    2. Uploads to user via bot
    3. Reports progress
    4. Handles errors
    
    Returns:
        True if successful, False otherwise
    """
    transfer_id = f"job_{job.job_id}"
    file_path = None
    status_msg = None
    
    try:
        os.makedirs(TEMP_DIR, exist_ok=True)
        
        # Create status message
        status_msg = await bot.send_message(
            job.user_id,
            "⏳ Starting download..."
        )
        
        # Use user session to download
        async with create_user_session(session_string, job.user_id) as user_client:
            # Get the source message
            try:
                source_msg = await asyncio.wait_for(
                    user_client.get_messages(job.chat_id, job.message_id),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                raise Exception("Timeout getting message")
            
            if not source_msg or source_msg.empty:
                raise Exception("Message not found or empty")
            
            if not source_msg.media:
                # PATCH 4: Text message — skip download stage, forward text only
                if source_msg.text:
                    logger.info(f"INFO No media — skipping download stage (job {job.job_id})")
                    await bot.send_message(
                        job.user_id, source_msg.text,
                        entities=source_msg.entities,  # PATCH 2: preserve original entities
                        parse_mode=None
                    )
                    return True
                logger.info(f"INFO No media — skipping download stage (job {job.job_id})")
                raise Exception("No media in message")
            
            # Get file info
            file_unique_id, file_size = get_file_info(source_msg)
            file_name = get_file_name(source_msg)
            media_type = get_media_type(source_msg)
            
            if not file_unique_id:
                raise Exception("Could not get file info")
            
            # Check for duplicate
            if await download_cache.is_duplicate_for_user(job.user_id, file_unique_id):
                await bot.edit_message_text(
                    job.user_id,
                    status_msg.id,
                    "⚠️ File already downloaded previously"
                )
                return True
            
            # Start cache record
            cache_record = await download_cache.start_download(
                file_unique_id=file_unique_id,
                user_id=job.user_id,
                chat_id=job.chat_id,
                message_id=job.message_id,
                file_size=file_size,
                file_name=file_name,
            )
            
            # Check for resume
            resume_offset = cache_record.downloaded_bytes
            if resume_offset > 0:
                logger.info(f"Resuming download at {resume_offset} bytes")
            
            # Setup progress callback
            progress_cb = progress_reporter.create_callback(
                transfer_id=transfer_id,
                client=bot,
                message=status_msg,
                transfer_type="download",
                file_name=file_name
            )
            
            # Download
            job.status = DownloadStatus.DOWNLOADING
            file_path = os.path.join(TEMP_DIR, f"{job.job_id}_{file_name}")
            
            downloaded_path = await asyncio.wait_for(
                user_client.download_media(
                    source_msg,
                    file_name=file_path,
                    progress=progress_cb
                ),
                timeout=DOWNLOAD_TIMEOUT
            )
            
            if not downloaded_path or not os.path.exists(downloaded_path):
                raise Exception("Download failed - no file")
            
            file_path = downloaded_path
            
            # Update progress to upload phase
            await progress_reporter.finalize(transfer_id, success=True)
            progress_reporter.cleanup(transfer_id)
            
            # Upload phase
            job.status = DownloadStatus.UPLOADING
            upload_transfer_id = f"upload_{job.job_id}"
            
            upload_cb = progress_reporter.create_callback(
                transfer_id=upload_transfer_id,
                client=bot,
                message=status_msg,
                transfer_type="upload",
                file_name=file_name
            )
            
            # Get caption + entities (PATCH 2: use original entities, never reconstruct)
            caption = source_msg.caption or None
            caption_entities = source_msg.caption_entities if caption else None
            
            # Upload based on media type
            send_kwargs = {
                "chat_id": job.user_id,
                "caption": caption,
                "caption_entities": caption_entities,  # PATCH 2: preserve formatting
                "parse_mode": None,  # PATCH 2: disable parse_mode when using entities
                "progress": upload_cb,
            }
            
            if media_type == "video":
                await asyncio.wait_for(
                    bot.send_video(
                        **send_kwargs,
                        video=file_path,
                        duration=source_msg.video.duration if source_msg.video else None,
                        width=source_msg.video.width if source_msg.video else None,
                        height=source_msg.video.height if source_msg.video else None,
                    ),
                    timeout=UPLOAD_TIMEOUT
                )
            elif media_type == "audio":
                await asyncio.wait_for(
                    bot.send_audio(**send_kwargs, audio=file_path),
                    timeout=UPLOAD_TIMEOUT
                )
            elif media_type == "photo":
                await asyncio.wait_for(
                    bot.send_photo(**send_kwargs, photo=file_path),
                    timeout=UPLOAD_TIMEOUT
                )
            elif media_type == "voice":
                await asyncio.wait_for(
                    bot.send_voice(**send_kwargs, voice=file_path),
                    timeout=UPLOAD_TIMEOUT
                )
            elif media_type == "video_note":
                await asyncio.wait_for(
                    bot.send_video_note(chat_id=job.user_id, video_note=file_path, progress=upload_cb),
                    timeout=UPLOAD_TIMEOUT
                )
            elif media_type == "animation":
                await asyncio.wait_for(
                    bot.send_animation(**send_kwargs, animation=file_path),
                    timeout=UPLOAD_TIMEOUT
                )
            else:
                # Default to document
                await asyncio.wait_for(
                    bot.send_document(**send_kwargs, document=file_path),
                    timeout=UPLOAD_TIMEOUT
                )
            
            # Finalize
            await progress_reporter.finalize(upload_transfer_id, success=True)
            progress_reporter.cleanup(upload_transfer_id)
            
            # Mark as complete in cache
            await download_cache.complete_download(file_unique_id, file_path)
            
            # Delete status message
            try:
                await bot.delete_messages(job.user_id, status_msg.id)
            except:
                pass
            
            return True
            
    except asyncio.CancelledError:
        logger.info(f"Job {job.job_id} cancelled")
        if status_msg:
            try:
                await bot.edit_message_text(
                    job.user_id,
                    status_msg.id,
                    "❌ Download cancelled"
                )
            except (FloodWait, MessageNotModified, Exception):
                pass
        raise
    
    except FloodWait as e:
        # PATCH 5: Safe FloodWait handler — pause then retry once
        wait_time = getattr(e, 'value', getattr(e, 'x', 30))
        logger.warning(f"FloodWaitHandled: {wait_time}s in job {job.job_id}")
        await asyncio.sleep(min(wait_time, 300))
        return False
        
    except SessionInvalidError as e:
        logger.error(f"Session invalid for job {job.job_id}: {e}")
        if status_msg:
            try:
                await bot.edit_message_text(
                    job.user_id,
                    status_msg.id,
                    f"❌ Session expired. Please /login again.\n\nError: {e}"
                )
            except:
                pass
        return False
        
    except Exception as e:
        logger.error(f"Job {job.job_id} failed: {e}")
        job.error_message = str(e)
        
        if status_msg:
            try:
                await bot.edit_message_text(
                    job.user_id,
                    status_msg.id,
                    f"❌ Download failed\n\nError: {str(e)[:200]}"
                )
            except (FloodWait, MessageNotModified, Exception):
                pass
        
        return False
        
    finally:
        # Cleanup
        progress_reporter.cleanup(transfer_id)
        progress_reporter.cleanup(f"upload_{job.job_id}")
        
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
