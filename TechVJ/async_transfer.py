"""
Async Transfer Pipeline for Telegram Media

Production-grade download and upload system that:
- Uses ONLY Pyrogram async APIs
- Integrates with async_progress for real-time updates
- Handles large files (>2GB) within Telegram limits
- Provides clean cancellation and guaranteed cleanup
- No threads, executors, or blocking calls

Author: WOODcraft Production-Grade Refactor
Python: 3.10+
Platform: Windows-optimized

ARCHITECTURE:
1. TransferPipeline - Orchestrates the download->upload flow
2. AsyncDownloader - Handles media downloads with progress
3. AsyncUploader - Handles media uploads with progress
4. All operations run on a SINGLE asyncio event loop
"""

import asyncio
import os
import uuid
import logging
from typing import Optional, Callable, Union, TypeVar, Awaitable
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum, auto

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, BadRequest, RPCError
from pyrogram.enums import MessageMediaType

from TechVJ.async_progress import (
    async_progress_controller,
    TransferType,
    ProgressFormatter
)

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# Timeouts (generous for large files)
DOWNLOAD_TIMEOUT = 7200  # 2 hours max
UPLOAD_TIMEOUT = 7200    # 2 hours max

# Retry configuration
MAX_RETRIES = 2
RETRY_BASE_DELAY = 2.0

# Default temp directory
DEFAULT_TEMP_DIR = "downloads/temp"

# Maximum Telegram file size (2GB - safety margin)
MAX_TELEGRAM_SIZE = 2 * 1024 * 1024 * 1024 - 1024 * 1024  # 2GB - 1MB


# ============================================================================
# TYPE DEFINITIONS
# ============================================================================

# Generic type for check_cancelled callbacks
CancelChecker = Callable[[], Union[bool, Awaitable[bool]]]


class TransferStatus(Enum):
    """Result status for transfers."""
    SUCCESS = auto()
    CANCELLED = auto()
    ERROR = auto()
    TIMEOUT = auto()
    FILE_TOO_LARGE = auto()


@dataclass
class TransferResult:
    """Result of a transfer operation."""
    status: TransferStatus
    file_path: Optional[str] = None
    message: Optional[Message] = None
    error: Optional[str] = None
    bytes_transferred: int = 0


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def ensure_temp_dir(path: str = DEFAULT_TEMP_DIR) -> str:
    """Ensure temp directory exists."""
    os.makedirs(path, exist_ok=True)
    return path


def generate_transfer_id() -> str:
    """Generate a unique transfer ID."""
    return f"xfer_{uuid.uuid4().hex[:12]}"


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for Windows filesystem.
    
    Removes/replaces characters that are problematic on Windows.
    """
    if not filename:
        return f"file_{uuid.uuid4().hex[:8]}"
    
    # Characters not allowed in Windows filenames
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove leading/trailing spaces and dots
    filename = filename.strip(' .')
    
    # Limit length (leave room for transfer ID prefix)
    max_len = 180
    if len(filename) > max_len:
        name, ext = os.path.splitext(filename)
        filename = name[:max_len - len(ext)] + ext
    
    return filename or f"file_{uuid.uuid4().hex[:8]}"


def get_media_filename(message: Message) -> str:
    """
    Extract or generate a filename from a message's media.
    """
    filename = None
    
    if message.document and message.document.file_name:
        filename = message.document.file_name
    elif message.video and message.video.file_name:
        filename = message.video.file_name
    elif message.audio and message.audio.file_name:
        filename = message.audio.file_name
    elif message.photo:
        filename = f"photo_{message.photo.file_unique_id}.jpg"
    elif message.voice:
        filename = f"voice_{message.voice.file_unique_id}.ogg"
    elif message.video_note:
        filename = f"videonote_{message.video_note.file_unique_id}.mp4"
    elif message.sticker:
        ext = ".webp" if not message.sticker.is_animated else ".tgs"
        filename = f"sticker_{message.sticker.file_unique_id}{ext}"
    elif message.animation:
        filename = f"animation_{message.animation.file_unique_id}.mp4"
    
    return sanitize_filename(filename or f"media_{uuid.uuid4().hex[:8]}")


def get_file_size(message: Message) -> int:
    """Get file size from a message if available."""
    if message.document:
        return message.document.file_size or 0
    elif message.video:
        return message.video.file_size or 0
    elif message.audio:
        return message.audio.file_size or 0
    elif message.voice:
        return message.voice.file_size or 0
    elif message.video_note:
        return message.video_note.file_size or 0
    elif message.animation:
        return message.animation.file_size or 0
    elif message.photo:
        # Photos have sizes array, get largest
        if message.photo.file_size:
            return message.photo.file_size
    return 0


def get_media_type(message: Message) -> Optional[str]:
    """Determine the media type of a message."""
    if message.video:
        return "video"
    elif message.audio:
        return "audio"
    elif message.photo:
        return "photo"
    elif message.voice:
        return "voice"
    elif message.video_note:
        return "video_note"
    elif message.animation:
        return "animation"
    elif message.document:
        return "document"
    elif message.sticker:
        return "sticker"
    return None


async def check_cancellation(checker: Optional[CancelChecker]) -> bool:
    """
    Check if operation should be cancelled.
    
    Handles both sync and async cancel checkers.
    """
    if checker is None:
        return False
    
    result = checker()
    if asyncio.iscoroutine(result):
        return await result
    return bool(result)


def cleanup_file(file_path: Optional[str]) -> None:
    """Safely remove a file if it exists."""
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.debug(f"Cleaned up file: {file_path}")
        except Exception as e:
            logger.warning(f"Could not remove file {file_path}: {e}")


# ============================================================================
# ASYNC DOWNLOADER
# ============================================================================

class AsyncDownloader:
    """
    Async-safe media downloader with progress tracking.
    
    Uses Pyrogram's download_media with integrated progress callbacks.
    """
    
    def __init__(self, temp_dir: str = DEFAULT_TEMP_DIR):
        """
        Initialize downloader.
        
        Args:
            temp_dir: Directory for downloaded files
        """
        self.temp_dir = ensure_temp_dir(temp_dir)
    
    async def download(
        self,
        client: Client,
        message: Message,
        status_client: Client,
        status_message: Message,
        destination: Optional[str] = None,
        check_cancelled: Optional[CancelChecker] = None,
        timeout: float = DOWNLOAD_TIMEOUT
    ) -> TransferResult:
        """
        Download media from a Telegram message.
        
        Args:
            client: Pyrogram client with access to media
            message: Message containing media
            status_client: Client for editing status message
            status_message: Message to update with progress
            destination: Optional destination path
            check_cancelled: Optional cancellation checker
            timeout: Maximum download time
            
        Returns:
            TransferResult with status and file path
        """
        transfer_id = generate_transfer_id()
        file_path: Optional[str] = None
        
        try:
            # Validate message has media
            if not message.media:
                return TransferResult(
                    status=TransferStatus.ERROR,
                    error="Message has no downloadable media"
                )
            
            # Check file size
            file_size = get_file_size(message)
            if file_size > MAX_TELEGRAM_SIZE:
                return TransferResult(
                    status=TransferStatus.FILE_TOO_LARGE,
                    error=f"File too large: {ProgressFormatter.format_size(file_size)}"
                )
            
            # Generate destination path
            filename = get_media_filename(message)
            if destination is None:
                destination = os.path.join(self.temp_dir, f"{transfer_id}_{filename}")
            
            # Create progress callback
            progress_callback = async_progress_controller.create_callback(
                transfer_id=transfer_id,
                client=status_client,
                status_message=status_message,
                transfer_type=TransferType.DOWNLOAD,
                file_name=filename
            )
            
            # Download with retries
            for attempt in range(MAX_RETRIES + 1):
                try:
                    # Check cancellation before attempt
                    if await check_cancellation(check_cancelled):
                        return TransferResult(
                            status=TransferStatus.CANCELLED,
                            error="Download cancelled"
                        )
                    
                    # Perform download
                    file_path = await asyncio.wait_for(
                        client.download_media(
                            message=message,
                            file_name=destination,
                            progress=progress_callback
                        ),
                        timeout=timeout
                    )
                    
                    if file_path and os.path.exists(file_path):
                        # Success!
                        await async_progress_controller.finalize(
                            transfer_id, 
                            success=True
                        )
                        return TransferResult(
                            status=TransferStatus.SUCCESS,
                            file_path=file_path,
                            bytes_transferred=os.path.getsize(file_path)
                        )
                    else:
                        return TransferResult(
                            status=TransferStatus.ERROR,
                            error="Download returned empty path"
                        )
                
                except FloodWait as e:
                    wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                    logger.warning(f"FloodWait({wait_time}s) during download")
                    
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(min(wait_time, 60))
                        continue
                    
                    return TransferResult(
                        status=TransferStatus.ERROR,
                        error=f"Rate limited: wait {wait_time}s"
                    )
                
                except asyncio.TimeoutError:
                    if attempt < MAX_RETRIES:
                        logger.warning(f"Download timeout, attempt {attempt + 1}")
                        continue
                    
                    return TransferResult(
                        status=TransferStatus.TIMEOUT,
                        error=f"Download timeout after {timeout}s"
                    )
                
                except asyncio.CancelledError:
                    return TransferResult(
                        status=TransferStatus.CANCELLED,
                        error="Download cancelled"
                    )
                
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        logger.warning(f"Download error: {e}, retrying...")
                        await asyncio.sleep(RETRY_BASE_DELAY * (attempt + 1))
                        continue
                    raise
            
            return TransferResult(
                status=TransferStatus.ERROR,
                error="Download failed after retries"
            )
        
        except asyncio.CancelledError:
            await async_progress_controller.finalize(
                transfer_id,
                success=False,
                error_message="Cancelled"
            )
            cleanup_file(file_path or destination)
            raise
        
        except Exception as e:
            logger.error(f"Download failed: {e}")
            await async_progress_controller.finalize(
                transfer_id,
                success=False,
                error_message=str(e)
            )
            cleanup_file(file_path or destination)
            return TransferResult(
                status=TransferStatus.ERROR,
                error=str(e)
            )
        
        finally:
            async_progress_controller.cleanup(transfer_id)


# ============================================================================
# ASYNC UPLOADER
# ============================================================================

class AsyncUploader:
    """
    Async-safe media uploader with progress tracking.
    
    Uses Pyrogram's send_* methods with integrated progress callbacks.
    """
    
    async def upload(
        self,
        client: Client,
        chat_id: int,
        file_path: str,
        status_message: Message,
        media_type: str = "document",
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
        check_cancelled: Optional[CancelChecker] = None,
        timeout: float = UPLOAD_TIMEOUT,
        **kwargs
    ) -> TransferResult:
        """
        Upload a file to Telegram.
        
        Args:
            client: Pyrogram client for uploading
            chat_id: Destination chat ID
            file_path: Path to file
            status_message: Message to update with progress
            media_type: Type of media (document, video, audio, photo)
            caption: Optional caption
            reply_to_message_id: Optional message to reply to
            check_cancelled: Optional cancellation checker
            timeout: Maximum upload time
            **kwargs: Additional arguments for send method
            
        Returns:
            TransferResult with status and sent message
        """
        transfer_id = generate_transfer_id()
        
        try:
            # Validate file exists
            if not os.path.exists(file_path):
                return TransferResult(
                    status=TransferStatus.ERROR,
                    error=f"File not found: {file_path}"
                )
            
            # Check file size
            file_size = os.path.getsize(file_path)
            if file_size > MAX_TELEGRAM_SIZE:
                return TransferResult(
                    status=TransferStatus.FILE_TOO_LARGE,
                    error=f"File too large for Telegram: {ProgressFormatter.format_size(file_size)}"
                )
            
            filename = os.path.basename(file_path)
            
            # Create progress callback
            progress_callback = async_progress_controller.create_callback(
                transfer_id=transfer_id,
                client=client,
                status_message=status_message,
                transfer_type=TransferType.UPLOAD,
                file_name=filename
            )
            
            # Select upload method
            send_methods = {
                "document": client.send_document,
                "video": client.send_video,
                "audio": client.send_audio,
                "photo": client.send_photo,
                "voice": client.send_voice,
                "video_note": client.send_video_note,
                "animation": client.send_animation,
            }
            send_method = send_methods.get(media_type, client.send_document)
            
            # Build arguments
            send_kwargs = {
                "chat_id": chat_id,
                media_type: file_path,
                "progress": progress_callback,
            }
            
            if caption:
                send_kwargs["caption"] = caption
            if reply_to_message_id:
                send_kwargs["reply_to_message_id"] = reply_to_message_id
            
            # Add any extra kwargs (duration, width, height, etc.)
            send_kwargs.update(kwargs)
            
            # Upload with retries
            for attempt in range(MAX_RETRIES + 1):
                try:
                    # Check cancellation
                    if await check_cancellation(check_cancelled):
                        return TransferResult(
                            status=TransferStatus.CANCELLED,
                            error="Upload cancelled"
                        )
                    
                    # Perform upload
                    sent_message = await asyncio.wait_for(
                        send_method(**send_kwargs),
                        timeout=timeout
                    )
                    
                    # Success!
                    await async_progress_controller.finalize(
                        transfer_id,
                        success=True
                    )
                    return TransferResult(
                        status=TransferStatus.SUCCESS,
                        message=sent_message,
                        bytes_transferred=file_size
                    )
                
                except FloodWait as e:
                    wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                    logger.warning(f"FloodWait({wait_time}s) during upload")
                    
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(min(wait_time, 60))
                        continue
                    
                    return TransferResult(
                        status=TransferStatus.ERROR,
                        error=f"Rate limited: wait {wait_time}s"
                    )
                
                except asyncio.TimeoutError:
                    if attempt < MAX_RETRIES:
                        logger.warning(f"Upload timeout, attempt {attempt + 1}")
                        continue
                    
                    return TransferResult(
                        status=TransferStatus.TIMEOUT,
                        error=f"Upload timeout after {timeout}s"
                    )
                
                except asyncio.CancelledError:
                    return TransferResult(
                        status=TransferStatus.CANCELLED,
                        error="Upload cancelled"
                    )
                
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        logger.warning(f"Upload error: {e}, retrying...")
                        await asyncio.sleep(RETRY_BASE_DELAY * (attempt + 1))
                        continue
                    raise
            
            return TransferResult(
                status=TransferStatus.ERROR,
                error="Upload failed after retries"
            )
        
        except asyncio.CancelledError:
            await async_progress_controller.finalize(
                transfer_id,
                success=False,
                error_message="Cancelled"
            )
            raise
        
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            await async_progress_controller.finalize(
                transfer_id,
                success=False,
                error_message=str(e)
            )
            return TransferResult(
                status=TransferStatus.ERROR,
                error=str(e)
            )
        
        finally:
            async_progress_controller.cleanup(transfer_id)


# ============================================================================
# TRANSFER PIPELINE
# ============================================================================

class TransferPipeline:
    """
    Complete download->upload pipeline with progress tracking.
    
    Orchestrates the full flow:
    1. Download media from source
    2. Upload to destination
    3. Cleanup temp files
    
    All with real-time progress updates and cancellation support.
    """
    
    def __init__(self, temp_dir: str = DEFAULT_TEMP_DIR):
        """
        Initialize the pipeline.
        
        Args:
            temp_dir: Directory for temporary files
        """
        self.temp_dir = ensure_temp_dir(temp_dir)
        self.downloader = AsyncDownloader(temp_dir)
        self.uploader = AsyncUploader()
    
    async def transfer(
        self,
        source_client: Client,
        source_message: Message,
        dest_client: Client,
        dest_chat_id: int,
        status_message: Message,
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
        check_cancelled: Optional[CancelChecker] = None,
        cleanup_after: bool = True
    ) -> TransferResult:
        """
        Transfer media from source to destination.
        
        Args:
            source_client: Client with access to source media
            source_message: Message containing media
            dest_client: Client for uploading (typically bot)
            dest_chat_id: Destination chat ID
            status_message: Message to update with progress
            caption: Optional caption for uploaded file
            reply_to_message_id: Optional message to reply to
            check_cancelled: Optional cancellation checker
            cleanup_after: Whether to delete temp file after upload
            
        Returns:
            TransferResult with final status
        """
        file_path: Optional[str] = None
        
        try:
            # Phase 1: Download
            download_result = await self.downloader.download(
                client=source_client,
                message=source_message,
                status_client=dest_client,
                status_message=status_message,
                check_cancelled=check_cancelled
            )
            
            if download_result.status != TransferStatus.SUCCESS:
                return download_result
            
            file_path = download_result.file_path
            
            # Check cancellation between phases
            if await check_cancellation(check_cancelled):
                cleanup_file(file_path)
                return TransferResult(
                    status=TransferStatus.CANCELLED,
                    error="Cancelled between download and upload"
                )
            
            # Determine media type and prepare caption
            media_type = get_media_type(source_message) or "document"
            
            # Use original caption if not provided
            if caption is None and source_message.caption:
                caption = source_message.caption
            
            # Phase 2: Upload
            upload_result = await self.uploader.upload(
                client=dest_client,
                chat_id=dest_chat_id,
                file_path=file_path,
                status_message=status_message,
                media_type=media_type,
                caption=caption,
                reply_to_message_id=reply_to_message_id,
                check_cancelled=check_cancelled
            )
            
            return upload_result
        
        except asyncio.CancelledError:
            cleanup_file(file_path)
            raise
        
        except Exception as e:
            logger.error(f"Transfer pipeline error: {e}")
            cleanup_file(file_path)
            return TransferResult(
                status=TransferStatus.ERROR,
                error=str(e)
            )
        
        finally:
            # Cleanup temp file
            if cleanup_after and file_path:
                cleanup_file(file_path)


# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

# Convenience instances for direct import
async_downloader = AsyncDownloader()
async_uploader = AsyncUploader()
transfer_pipeline = TransferPipeline()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def download_with_progress(
    client: Client,
    message: Message,
    status_client: Client,
    status_message: Message,
    check_cancelled: Optional[CancelChecker] = None
) -> TransferResult:
    """
    Download media with progress tracking.
    
    Convenience function using global downloader.
    """
    return await async_downloader.download(
        client=client,
        message=message,
        status_client=status_client,
        status_message=status_message,
        check_cancelled=check_cancelled
    )


async def upload_with_progress(
    client: Client,
    chat_id: int,
    file_path: str,
    status_message: Message,
    media_type: str = "document",
    caption: Optional[str] = None,
    check_cancelled: Optional[CancelChecker] = None,
    **kwargs
) -> TransferResult:
    """
    Upload file with progress tracking.
    
    Convenience function using global uploader.
    """
    return await async_uploader.upload(
        client=client,
        chat_id=chat_id,
        file_path=file_path,
        status_message=status_message,
        media_type=media_type,
        caption=caption,
        check_cancelled=check_cancelled,
        **kwargs
    )
