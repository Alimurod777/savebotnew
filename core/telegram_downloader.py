"""
Telegram Raw API Downloader

Uses Pyrogram's raw API (upload.GetFile) for chunked downloads.

Features:
- Large file support (4GB+)
- Resume from offset using .part files
- Per-DC semaphore rate limiting
- CDN redirect handling
- FloodWait backoff
- Progress callbacks
- All file I/O via asyncio.to_thread (non-blocking)

NOTE: kurigram==2.2.17 installs under the "pyrogram" namespace.
"""

import asyncio
import os
import time
import logging
from typing import Optional, Callable, Dict, Awaitable
from dataclasses import dataclass

# Pyrogram imports (provided by kurigram==2.2.17)
from pyrogram import Client
from pyrogram.raw import functions, types
from pyrogram.raw.base import InputFileLocation
from pyrogram.errors import (
    FloodWait, FileMigrate,
    FileReferenceExpired, FileReferenceEmpty, FileReferenceInvalid,
    AuthBytesInvalid, Timeout, BadRequest, RPCError
)

# Tuple of all file reference related errors for Kurigram compatibility
FILE_REFERENCE_ERRORS = (FileReferenceExpired, FileReferenceEmpty, FileReferenceInvalid)
from pyrogram.file_id import FileId, FileType, PHOTO_TYPES

from core.models import DownloadTask, DownloadResult, SpeedTracker, ProgressEvent
from core.structured_log import op_log

logger = logging.getLogger(__name__)

# Download configuration
CHUNK_SIZE = 1024 * 1024         # 1 MB per chunk (optimized)
MAX_CHUNK_SIZE = 1024 * 1024     # 1 MB max chunk
OFFSET_SAVE_INTERVAL = 5 * 1024 * 1024  # Save offset every 5 MB (was 1MB - PERF-001)
FLUSH_INTERVAL = 10 * 1024 * 1024  # Flush to disk every 10 MB (PERF-001)
MAX_RETRIES = 5
BASE_RETRY_DELAY = 2.0

# Per-DC semaphores for rate limiting
# Limits concurrent downloads per Telegram DC
DC_SEMAPHORES: Dict[int, asyncio.Semaphore] = {}
DC_LIMITS = {
    1: 10, 2: 10, 3: 10, 4: 10, 5: 10,  # Safe limits per DC (optimized)
}


def get_dc_semaphore(dc_id: int) -> asyncio.Semaphore:
    """Get or create semaphore for DC rate limiting."""
    if dc_id not in DC_SEMAPHORES:
        limit = DC_LIMITS.get(dc_id, 8)
        DC_SEMAPHORES[dc_id] = asyncio.Semaphore(limit)
    return DC_SEMAPHORES[dc_id]


def configure_dc_limit(dc_id: int, limit: int) -> None:
    """Configure concurrent download limit for a DC."""
    DC_LIMITS[dc_id] = limit
    # Recreate semaphore if it exists
    if dc_id in DC_SEMAPHORES:
        DC_SEMAPHORES[dc_id] = asyncio.Semaphore(limit)


def build_input_file_location(task: DownloadTask) -> InputFileLocation:
    """
    Build InputFileLocation from task metadata.
    
    Handles all media types: documents, videos, photos, etc.
    """
    file_id = FileId.decode(task.file_id)
    file_type = file_id.file_type
    
    if file_type in PHOTO_TYPES:
        # Photo location
        return types.InputPhotoFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=task.file_reference or file_id.file_reference,
            thumb_size=task.thumb_size or file_id.thumbnail_size or ""
        )
    elif file_type == FileType.CHAT_PHOTO:
        # Chat photo
        if file_id.chat_id and file_id.chat_access_hash:
            peer = types.InputPeerChannel(
                channel_id=file_id.chat_id,
                access_hash=file_id.chat_access_hash
            )
        else:
            peer = types.InputPeerChat(chat_id=file_id.chat_id)
        
        return types.InputPeerPhotoFileLocation(
            peer=peer,
            photo_id=file_id.media_id,
            big=file_id.thumbnail_source == 1
        )
    else:
        # Document, video, audio, voice, etc.
        return types.InputDocumentFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=task.file_reference or file_id.file_reference,
            thumb_size=task.thumb_size or ""
        )


class TelegramDownloader:
    """
    Raw API downloader for Telegram files.
    
    Uses upload.GetFile for chunked downloads with:
    - Resume from offset support
    - Per-DC rate limiting via semaphores
    - Progress callbacks
    - FloodWait handling with exponential backoff
    - Network error retry logic
    """
    
    def __init__(self, client: Client):
        self.client = client
        self._cancel_flags: Dict[str, bool] = {}
    
    def cancel_download(self, task_id: str) -> None:
        """Signal a download to cancel."""
        self._cancel_flags[task_id] = True
    
    def is_cancelled(self, task_id: str) -> bool:
        """Check if download is cancelled."""
        return self._cancel_flags.get(task_id, False)
    
    def clear_cancel(self, task_id: str) -> None:
        """Clear cancel flag for a task."""
        self._cancel_flags.pop(task_id, None)
    
    async def download(
        self,
        task: DownloadTask,
        on_progress: Optional[Callable[[ProgressEvent], None]] = None,
        on_offset_save: Optional[Callable[[str, int], Awaitable[None]]] = None,
    ) -> DownloadResult:
        """
        Download a file using raw API.
        
        Args:
            task: Download task with file metadata
            on_progress: Callback for progress updates (sync, called frequently)
            on_offset_save: Async callback to persist offset to DB (called every ~1MB)
        
        Returns:
            DownloadResult with success status and file path
        """
        self.clear_cancel(task.task_id)
        start_time = time.time()
        
        op_log("download_start", message_id=task.message_id,
               user_id=task.user_id, operation_id=task.task_id,
               bytes_processed=0, file_size=task.file_size)
        
        # Ensure temp directory exists (non-blocking)
        temp_dir = os.path.dirname(task.temp_path)
        if temp_dir:
            await asyncio.to_thread(os.makedirs, temp_dir, exist_ok=True)
        
        # Get DC semaphore for rate limiting
        dc_id = task.dc_id or 2
        semaphore = get_dc_semaphore(dc_id)
        
        try:
            async with semaphore:
                result = await self._download_impl(
                    task=task,
                    on_progress=on_progress,
                    on_offset_save=on_offset_save,
                )
                result.elapsed_seconds = time.time() - start_time
                return result
                
        except asyncio.CancelledError:
            logger.info(f"Download cancelled: {task.task_id}")
            return DownloadResult(
                success=False,
                error="Cancelled",
                bytes_downloaded=task.downloaded_bytes,
                elapsed_seconds=time.time() - start_time
            )
        except Exception as e:
            logger.error(f"Download failed: {task.task_id} - {e}")
            return DownloadResult(
                success=False,
                error=str(e),
                bytes_downloaded=task.downloaded_bytes,
                elapsed_seconds=time.time() - start_time
            )
        finally:
            self.clear_cancel(task.task_id)
    
    async def _download_impl(
        self,
        task: DownloadTask,
        on_progress: Optional[Callable[[ProgressEvent], None]],
        on_offset_save: Optional[Callable[[str, int], Awaitable[None]]],
    ) -> DownloadResult:
        """Internal download implementation."""
        
        location = build_input_file_location(task)
        offset = task.offset
        file_size = task.file_size
        last_offset_save = offset
        last_flush = offset  # PERF-001: Track last flush position

        if file_size <= 0:
            logger.warning(f"Unknown file size for task {task.task_id}, falling back to download_media")
            fallback_path = task.dest_path or task.temp_path
            if fallback_path:
                dest_dir = os.path.dirname(fallback_path)
                if dest_dir:
                    await asyncio.to_thread(os.makedirs, dest_dir, exist_ok=True)
            try:
                file_path = await self.client.download_media(
                    task.file_id,
                    file_name=fallback_path or None
                )
                if file_path and await asyncio.to_thread(os.path.exists, file_path):
                    actual_size = await asyncio.to_thread(os.path.getsize, file_path)
                    return DownloadResult(
                        success=True,
                        file_path=file_path,
                        bytes_downloaded=actual_size
                    )
                return DownloadResult(
                    success=False,
                    error="Download produced no file",
                    bytes_downloaded=0
                )
            except Exception as e:
                return DownloadResult(
                    success=False,
                    error=str(e),
                    bytes_downloaded=0
                )
        
        speed_tracker = SpeedTracker()
        speed_tracker.add_sample(offset, time.time())
        
        # BUG-004: Verify offset against actual file size for resume
        if offset > 0 and os.path.exists(task.temp_path):
            actual_size = await asyncio.to_thread(os.path.getsize, task.temp_path)
            if actual_size != offset:
                logger.warning(
                    f"Resume offset mismatch: stored={offset}, actual={actual_size}. Using actual."
                )
                offset = actual_size
                task.offset = actual_size
        
        # Open file for writing
        mode = 'ab' if offset > 0 else 'wb'
        file = await asyncio.to_thread(open, task.temp_path, mode)
        
        try:
            retry_count = 0
            
            while offset < file_size:
                # Check cancellation
                if self.is_cancelled(task.task_id):
                    return DownloadResult(
                        success=False,
                        error="Cancelled",
                        bytes_downloaded=offset
                    )
                
                try:
                    # Calculate chunk size
                    chunk_size = min(CHUNK_SIZE, file_size - offset)
                    
                    # Request chunk from Telegram
                    result = await self.client.invoke(
                        functions.upload.GetFile(
                            location=location,
                            offset=offset,
                            limit=chunk_size
                        ),
                        sleep_threshold=60
                    )
                    
                    # Handle response type
                    if isinstance(result, types.upload.File):
                        chunk = result.bytes
                    elif isinstance(result, types.upload.FileCdnRedirect):
                        # CDN redirect - attempt reupload to get from main DC
                        chunk = await self._handle_cdn_redirect(result, offset, chunk_size)
                        if not chunk:
                            # Retry on main DC
                            await asyncio.sleep(1)
                            continue
                    else:
                        raise ValueError(f"Unexpected response type: {type(result)}")
                    
                    if not chunk:
                        # Empty chunk handling
                        if offset >= file_size:
                            break
                        retry_count += 1
                        if retry_count > MAX_RETRIES:
                            raise Exception(f"Too many empty chunks at offset {offset}")
                        await asyncio.sleep(BASE_RETRY_DELAY * retry_count)
                        continue
                    
                    # Write chunk to file (non-blocking)
                    await asyncio.to_thread(file.write, chunk)
                    
                    # Update offset
                    chunk_len = len(chunk)
                    offset += chunk_len
                    retry_count = 0
                    
                    # PERF-001: Batch flushes - only flush every FLUSH_INTERVAL
                    if (offset - last_flush) >= FLUSH_INTERVAL:
                        await asyncio.to_thread(file.flush)
                        last_flush = offset
                    
                    # Track speed
                    speed_tracker.add_sample(offset, time.time())
                    
                    # Progress callback
                    if on_progress:
                        event = ProgressEvent(
                            task_id=task.task_id,
                            user_id=task.user_id,
                            chat_id=task.progress_chat_id,
                            message_id=task.progress_message_id,
                            current_bytes=offset,
                            total_bytes=file_size,
                            speed=speed_tracker.get_speed(),
                            eta=speed_tracker.get_eta(offset, file_size),
                            status="DOWNLOADING",
                            file_name=task.file_name,
                        )
                        on_progress(event)
                    
                    # Periodic offset save
                    if on_offset_save and (offset - last_offset_save) >= OFFSET_SAVE_INTERVAL:
                        await on_offset_save(task.task_id, offset)
                        last_offset_save = offset
                
                except FloodWait as e:
                    wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                    logger.warning(f"FloodWait: {wait_time}s for {task.task_id}")
                    await asyncio.sleep(wait_time)
                    continue
                
                except FileMigrate as e:
                    new_dc = getattr(e, 'value', getattr(e, 'x', 2))
                    logger.info(f"File migrated to DC{new_dc}")
                    # Pyrogram handles DC switching internally
                    await asyncio.sleep(1)
                    continue
                
                except FILE_REFERENCE_ERRORS as e:
                    error_type = type(e).__name__
                    logger.warning(f"File reference error ({error_type}): {task.task_id}")
                    return DownloadResult(
                        success=False,
                        error="FILE_REFERENCE_EXPIRED",
                        bytes_downloaded=offset
                    )
                
                except RPCError as e:
                    error_id = getattr(e, 'ID', '') or str(e)
                    if 'FILE_REFERENCE' in error_id.upper():
                        logger.warning(f"File reference RPC error: {task.task_id} - {error_id}")
                        return DownloadResult(
                            success=False,
                            error="FILE_REFERENCE_EXPIRED",
                            bytes_downloaded=offset
                        )
                    raise
                
                except (Timeout, ConnectionError, ConnectionResetError,
                        ConnectionAbortedError, BrokenPipeError, OSError,
                        TimeoutError) as e:
                    retry_count += 1
                    
                    # FIX: Windows-specific file locking handling (antivirus, etc.)
                    if isinstance(e, PermissionError) and os.name == 'nt':
                        logger.warning(f"Windows file lock (antivirus?), retry {retry_count}: {e}")
                        if retry_count <= MAX_RETRIES:
                            await asyncio.sleep(2.0)
                            continue
                    
                    if retry_count > MAX_RETRIES:
                        # Save progress before raising so resume can continue
                        if on_offset_save:
                            try:
                                await on_offset_save(task.task_id, offset)
                            except Exception:
                                pass
                        raise Exception(f"Network error after {MAX_RETRIES} retries: {e}")
                    
                    # Exponential backoff with cap
                    wait = min(BASE_RETRY_DELAY * (2 ** (retry_count - 1)), 60.0)
                    logger.warning(
                        f"Network error (retry {retry_count}/{MAX_RETRIES}): "
                        f"{type(e).__name__}: {e}"
                    )
                    await asyncio.sleep(wait)
                    continue
            
            # Download complete - close file
            await asyncio.to_thread(file.close)
            file = None
            
            # Verify downloaded size
            actual_size = await asyncio.to_thread(os.path.getsize, task.temp_path)
            if actual_size < file_size * 0.99:  # Allow 1% tolerance
                logger.warning(f"Size mismatch: {actual_size} vs {file_size}")
            
            # Rename .part to final path
            final_path = await self._finalize_file(task)
            
            return DownloadResult(
                success=True,
                file_path=final_path,
                bytes_downloaded=offset
            )
        
        finally:
            if file:
                await asyncio.to_thread(file.close)
    
    async def _handle_cdn_redirect(
        self,
        cdn_redirect: types.upload.FileCdnRedirect,
        offset: int,
        limit: int
    ) -> bytes:
        """
        Handle CDN redirect.
        
        For simplicity, request reupload to main DC.
        Full CDN implementation would download from CDN servers.
        """
        try:
            await self.client.invoke(
                functions.upload.ReuploadCdnFile(
                    file_token=cdn_redirect.file_token,
                    request_token=b""
                )
            )
            return b""  # Trigger retry on main DC
        except Exception as e:
            logger.warning(f"CDN redirect handling failed: {e}")
            return b""
    
    async def _finalize_file(self, task: DownloadTask) -> str:
        """Move .part file to final destination with Windows retry."""
        if not task.dest_path or task.dest_path == task.temp_path:
            return task.temp_path
        
        # Ensure destination directory exists
        dest_dir = os.path.dirname(task.dest_path)
        if dest_dir:
            await asyncio.to_thread(os.makedirs, dest_dir, exist_ok=True)
        
        # FIX: Windows file operation retry (antivirus may hold lock briefly)
        max_retries = 3 if os.name == 'nt' else 1
        
        for attempt in range(max_retries):
            try:
                # Remove existing file if any
                if await asyncio.to_thread(os.path.exists, task.dest_path):
                    await asyncio.to_thread(os.remove, task.dest_path)
                
                # Rename .part to final
                await asyncio.to_thread(os.rename, task.temp_path, task.dest_path)
                return task.dest_path
                
            except PermissionError as e:
                if os.name == 'nt' and attempt < max_retries - 1:
                    logger.warning(f"Windows file lock on finalize, retry {attempt+1}: {e}")
                    await asyncio.sleep(1.5)
                    continue
                raise
        
        return task.dest_path


async def extract_file_info(
    client: Client,
    message,
    user_id: int = 0
) -> Optional[DownloadTask]:
    """
    Extract file information from a Pyrogram Message object.
    
    Creates a DownloadTask with all necessary metadata for download.
    
    Args:
        client: Pyrogram client
        message: Message object containing media
        user_id: User ID requesting the download
    
    Returns:
        DownloadTask ready for queue, or None if no media
    """
    import uuid
    
    if not message.media:
        return None
    
    # Determine media type and get file info
    media = None
    media_type = "document"
    thumb_size = ""
    
    if message.document:
        media = message.document
        media_type = "document"
    elif message.video:
        media = message.video
        media_type = "video"
    elif message.audio:
        media = message.audio
        media_type = "audio"
    elif message.voice:
        media = message.voice
        media_type = "voice"
    elif message.video_note:
        media = message.video_note
        media_type = "video_note"
    elif message.animation:
        media = message.animation
        media_type = "animation"
    elif message.sticker:
        media = message.sticker
        media_type = "sticker"
    elif message.photo:
        media = message.photo
        media_type = "photo"
    
    if not media:
        return None
    
    # Extract file_id and decode
    file_id_str = media.file_id
    file_unique_id = media.file_unique_id
    
    try:
        file_id = FileId.decode(file_id_str)
    except Exception as e:
        logger.error(f"Failed to decode file_id: {e}")
        return None
    
    # Get file size
    file_size = getattr(media, 'file_size', 0) or 0
    
    # Get filename
    file_name = getattr(media, 'file_name', None)
    if not file_name:
        ext = ""
        mime = getattr(media, 'mime_type', '')
        if mime:
            ext_map = {
                'video/mp4': '.mp4',
                'video/x-matroska': '.mkv',
                'video/webm': '.webm',
                'audio/mpeg': '.mp3',
                'audio/ogg': '.ogg',
                'audio/mp4': '.m4a',
                'audio/x-wav': '.wav',
                'audio/flac': '.flac',
                'image/jpeg': '.jpg',
                'image/png': '.png',
                'image/webp': '.webp',
                'image/gif': '.gif',
                'application/zip': '.zip',
                'application/x-rar-compressed': '.rar',
                'application/x-7z-compressed': '.7z',
                'application/pdf': '.pdf',
            }
            ext = ext_map.get(mime, '')
        file_name = f"{media_type}_{file_unique_id}{ext}"
    
    # Build paths
    task_id = str(uuid.uuid4())
    temp_path = f"downloads/temp/{task_id}.part"
    
    from_user_id = message.from_user.id if message.from_user else user_id
    dest_path = f"downloads/{from_user_id}/{file_name}"
    
    return DownloadTask(
        task_id=task_id,
        file_unique_id=file_unique_id,
        user_id=user_id or from_user_id,
        chat_id=message.chat.id,
        message_id=message.id,
        file_id=file_id_str,
        file_size=file_size,
        file_name=file_name,
        mime_type=getattr(media, 'mime_type', ''),
        media_type=media_type,
        dc_id=file_id.dc_id,
        access_hash=file_id.access_hash,
        file_reference=file_id.file_reference or b"",
        thumb_size=thumb_size,
        temp_path=temp_path,
        dest_path=dest_path,
    )
