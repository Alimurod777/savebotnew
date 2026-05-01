"""
Transfer Manager for Telegram Media Downloads and Uploads

Unified interface for downloading and uploading media with integrated
progress tracking. Handles cleanup on cancellation or failure.

Design Decisions:
- Uses Pyrogram's native async APIs only
- Integrates with ProgressController for real-time updates
- Guaranteed cleanup via try/finally patterns
- No byte-level resume (not supported by MTProto)
- Cancellation-aware with proper task cleanup

Author: WOODcraft Refactored
"""

import asyncio
import os
import uuid
import logging
from typing import Optional, Callable, Union
from pathlib import Path

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait

from TechVJ.progress_controller import progress_controller, ProgressController

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# Default timeout for downloads (in seconds)
# Large files can take a long time - be generous
DEFAULT_DOWNLOAD_TIMEOUT = 3600  # 1 hour
DEFAULT_UPLOAD_TIMEOUT = 3600    # 1 hour

# Retry settings
MAX_RETRIES = 2
RETRY_BASE_DELAY = 2.0

# Temp directory for downloads
TEMP_DOWNLOAD_DIR = "downloads/temp"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def ensure_temp_dir() -> str:
    """Ensure temp directory exists and return path."""
    os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
    return TEMP_DOWNLOAD_DIR


def generate_transfer_id() -> str:
    """Generate a unique transfer ID."""
    return f"transfer_{uuid.uuid4().hex[:12]}"


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for filesystem safety.
    
    Removes or replaces characters that are problematic on Windows.
    """
    if not filename:
        return f"file_{uuid.uuid4().hex[:8]}"
    
    # Characters not allowed in Windows filenames
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Limit length
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200-len(ext)] + ext
    
    return filename


# ============================================================================
# TRANSFER MANAGER
# ============================================================================

class TransferManager:
    """
    Manages media downloads and uploads with integrated progress tracking.
    
    Features:
    - Automatic progress callbacks
    - Cleanup on cancel/error
    - Retry logic for transient failures
    - FloodWait handling
    
    Usage:
        manager = TransferManager()
        
        # Download with progress
        file_path = await manager.download_media(
            client=user_session,
            message=source_message,
            status_client=bot_client,
            status_message=progress_msg,
            check_cancelled=lambda: task_manager.should_cancel(user_id)
        )
        
        # Upload with progress
        sent_msg = await manager.upload_document(
            client=bot_client,
            chat_id=user_id,
            file_path=file_path,
            status_message=progress_msg,
            caption="Your file",
            check_cancelled=lambda: task_manager.should_cancel(user_id)
        )
    """
    
    def __init__(self, controller: Optional[ProgressController] = None):
        """
        Initialize the transfer manager.
        
        Args:
            controller: ProgressController instance (uses global if not provided)
        """
        self._controller = controller or progress_controller
        ensure_temp_dir()
    
    async def download_media(
        self,
        client: Client,
        message: Message,
        status_client: Client,
        status_message: Message,
        destination: Optional[str] = None,
        check_cancelled: Optional[Callable[[], bool]] = None,
        timeout: float = DEFAULT_DOWNLOAD_TIMEOUT
    ) -> Optional[str]:
        """
        Download media from a Telegram message with progress updates.
        
        Args:
            client: Pyrogram client with access to the media
            message: Message containing media to download
            status_client: Client to use for editing status message
            status_message: Message to update with progress
            destination: Optional download path (auto-generated if not provided)
            check_cancelled: Optional callable that returns True if cancelled
            timeout: Maximum time for download
        
        Returns:
            Downloaded file path, or None if failed/cancelled
        
        Raises:
            asyncio.CancelledError: If cancelled via check_cancelled
        """
        transfer_id = generate_transfer_id()
        downloaded_path = None
        
        try:
            # Validate message has downloadable media
            if not message.media:
                logger.warning(f"Message {message.id} has no media")
                return None
            
            # Generate destination path if not provided
            if destination is None:
                filename = self._get_media_filename(message)
                destination = os.path.join(TEMP_DOWNLOAD_DIR, f"{transfer_id}_{filename}")
            
            # Create progress callback
            progress_cb = self._controller.create_callback(
                transfer_id=transfer_id,
                client=status_client,
                status_message=status_message,
                transfer_type="download"
            )
            
            # Download with retry logic
            for attempt in range(MAX_RETRIES + 1):
                try:
                    # Check cancellation before starting
                    if check_cancelled and check_cancelled():
                        raise asyncio.CancelledError("Download cancelled")
                    
                    # Perform download
                    downloaded_path = await asyncio.wait_for(
                        client.download_media(
                            message=message,
                            file_name=destination,
                            progress=progress_cb
                        ),
                        timeout=timeout
                    )
                    
                    if downloaded_path:
                        # Success
                        await self._controller.finalize_progress(
                            transfer_id, 
                            success=True
                        )
                        return downloaded_path
                    else:
                        logger.warning(f"Download returned None for message {message.id}")
                        return None
                    
                except FloodWait as e:
                    wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                    logger.warning(f"FloodWait({wait_time}s) during download")
                    
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                
                except asyncio.TimeoutError:
                    logger.error(f"Download timeout after {timeout}s")
                    if attempt < MAX_RETRIES:
                        continue
                    raise
                
                except asyncio.CancelledError:
                    raise
                
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                        await asyncio.sleep(RETRY_BASE_DELAY * (attempt + 1))
                        continue
                    raise
            
            return None
            
        except asyncio.CancelledError:
            logger.info(f"Download cancelled: {transfer_id}")
            await self._controller.finalize_progress(
                transfer_id,
                success=False,
                error_message="Cancelled"
            )
            raise
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            await self._controller.finalize_progress(
                transfer_id,
                success=False,
                error_message=str(e)
            )
            return None
            
        finally:
            # Always cleanup progress state
            self._controller.cleanup(transfer_id)
            
            # Cleanup partial file on failure
            if downloaded_path is None and destination and os.path.exists(destination):
                try:
                    os.remove(destination)
                except Exception:
                    pass
    
    async def upload_document(
        self,
        client: Client,
        chat_id: int,
        file_path: str,
        status_message: Message,
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
        check_cancelled: Optional[Callable[[], bool]] = None,
        timeout: float = DEFAULT_UPLOAD_TIMEOUT
    ) -> Optional[Message]:
        """
        Upload a file as a document with progress updates.
        
        Args:
            client: Pyrogram client for uploading
            chat_id: Destination chat ID
            file_path: Path to file to upload
            status_message: Message to update with progress
            caption: Optional caption for the document
            reply_to_message_id: Optional message to reply to
            check_cancelled: Optional callable that returns True if cancelled
            timeout: Maximum time for upload
        
        Returns:
            Sent message, or None if failed/cancelled
        """
        return await self._upload_media(
            client=client,
            chat_id=chat_id,
            file_path=file_path,
            status_message=status_message,
            media_type="document",
            caption=caption,
            reply_to_message_id=reply_to_message_id,
            check_cancelled=check_cancelled,
            timeout=timeout
        )
    
    async def upload_video(
        self,
        client: Client,
        chat_id: int,
        file_path: str,
        status_message: Message,
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
        check_cancelled: Optional[Callable[[], bool]] = None,
        timeout: float = DEFAULT_UPLOAD_TIMEOUT,
        **kwargs
    ) -> Optional[Message]:
        """
        Upload a file as a video with progress updates.
        
        Additional kwargs are passed to send_video (duration, width, height, etc.)
        """
        return await self._upload_media(
            client=client,
            chat_id=chat_id,
            file_path=file_path,
            status_message=status_message,
            media_type="video",
            caption=caption,
            reply_to_message_id=reply_to_message_id,
            check_cancelled=check_cancelled,
            timeout=timeout,
            **kwargs
        )
    
    async def upload_audio(
        self,
        client: Client,
        chat_id: int,
        file_path: str,
        status_message: Message,
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
        check_cancelled: Optional[Callable[[], bool]] = None,
        timeout: float = DEFAULT_UPLOAD_TIMEOUT,
        **kwargs
    ) -> Optional[Message]:
        """
        Upload a file as audio with progress updates.
        
        Additional kwargs are passed to send_audio (duration, performer, title, etc.)
        """
        return await self._upload_media(
            client=client,
            chat_id=chat_id,
            file_path=file_path,
            status_message=status_message,
            media_type="audio",
            caption=caption,
            reply_to_message_id=reply_to_message_id,
            check_cancelled=check_cancelled,
            timeout=timeout,
            **kwargs
        )
    
    async def upload_photo(
        self,
        client: Client,
        chat_id: int,
        file_path: str,
        status_message: Message,
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
        check_cancelled: Optional[Callable[[], bool]] = None,
        timeout: float = DEFAULT_UPLOAD_TIMEOUT
    ) -> Optional[Message]:
        """Upload a file as a photo with progress updates."""
        return await self._upload_media(
            client=client,
            chat_id=chat_id,
            file_path=file_path,
            status_message=status_message,
            media_type="photo",
            caption=caption,
            reply_to_message_id=reply_to_message_id,
            check_cancelled=check_cancelled,
            timeout=timeout
        )
    
    async def _upload_media(
        self,
        client: Client,
        chat_id: int,
        file_path: str,
        status_message: Message,
        media_type: str,
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
        check_cancelled: Optional[Callable[[], bool]] = None,
        timeout: float = DEFAULT_UPLOAD_TIMEOUT,
        **kwargs
    ) -> Optional[Message]:
        """
        Internal method for uploading media with progress.
        
        Handles all media types through appropriate send_* methods.
        """
        transfer_id = generate_transfer_id()
        
        try:
            # Validate file exists
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return None
            
            # Create progress callback
            progress_cb = self._controller.create_callback(
                transfer_id=transfer_id,
                client=client,
                status_message=status_message,
                transfer_type="upload"
            )
            
            # Select upload method
            upload_method = {
                "document": client.send_document,
                "video": client.send_video,
                "audio": client.send_audio,
                "photo": client.send_photo,
            }.get(media_type, client.send_document)
            
            # Upload with retry logic
            for attempt in range(MAX_RETRIES + 1):
                try:
                    # Check cancellation
                    if check_cancelled and check_cancelled():
                        raise asyncio.CancelledError("Upload cancelled")
                    
                    # Perform upload
                    sent_message = await asyncio.wait_for(
                        upload_method(
                            chat_id=chat_id,
                            **{media_type: file_path},
                            caption=caption,
                            reply_to_message_id=reply_to_message_id,
                            progress=progress_cb,
                            **kwargs
                        ),
                        timeout=timeout
                    )
                    
                    # Success
                    await self._controller.finalize_progress(
                        transfer_id,
                        success=True
                    )
                    return sent_message
                    
                except FloodWait as e:
                    wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                    logger.warning(f"FloodWait({wait_time}s) during upload")
                    
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                
                except asyncio.TimeoutError:
                    logger.error(f"Upload timeout after {timeout}s")
                    if attempt < MAX_RETRIES:
                        continue
                    raise
                
                except asyncio.CancelledError:
                    raise
                
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
                        await asyncio.sleep(RETRY_BASE_DELAY * (attempt + 1))
                        continue
                    raise
            
            return None
            
        except asyncio.CancelledError:
            logger.info(f"Upload cancelled: {transfer_id}")
            await self._controller.finalize_progress(
                transfer_id,
                success=False,
                error_message="Cancelled"
            )
            raise
            
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            await self._controller.finalize_progress(
                transfer_id,
                success=False,
                error_message=str(e)
            )
            return None
            
        finally:
            self._controller.cleanup(transfer_id)
    
    def _get_media_filename(self, message: Message) -> str:
        """
        Extract or generate a filename from a message's media.
        
        Tries to get the original filename, falls back to a generated one.
        """
        filename = None
        
        # Try to get filename from different media types
        if message.document and message.document.file_name:
            filename = message.document.file_name
        elif message.video and message.video.file_name:
            filename = message.video.file_name
        elif message.audio and message.audio.file_name:
            filename = message.audio.file_name
        elif message.photo:
            # Photos don't have filenames
            filename = f"photo_{message.photo.file_unique_id}.jpg"
        elif message.voice:
            filename = f"voice_{message.voice.file_unique_id}.ogg"
        elif message.video_note:
            filename = f"video_note_{message.video_note.file_unique_id}.mp4"
        elif message.sticker:
            ext = ".webp" if not message.sticker.is_animated else ".tgs"
            filename = f"sticker_{message.sticker.file_unique_id}{ext}"
        elif message.animation:
            filename = f"animation_{message.animation.file_unique_id}.mp4"
        
        return sanitize_filename(filename or f"file_{uuid.uuid4().hex[:8]}")
    
    def get_media_type(self, message: Message) -> Optional[str]:
        """
        Determine the media type of a message.
        
        Returns:
            "document", "video", "audio", "photo", or None
        """
        if message.video:
            return "video"
        elif message.audio:
            return "audio"
        elif message.photo:
            return "photo"
        elif message.voice:
            return "audio"
        elif message.document:
            return "document"
        elif message.video_note:
            return "video"
        elif message.animation:
            return "video"
        elif message.sticker:
            return "document"
        return None


# ============================================================================
# GLOBAL INSTANCE
# ============================================================================

# Singleton for easy import
transfer_manager = TransferManager()
