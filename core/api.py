"""
core/api.py - Public API for the download engine.

This is a SIMPLIFIED entry point for downloads.
For production use with progress tracking, use save.py directly with:
    from TechVJ.progress_controller import progress_controller
    
The canonical download pipeline is:
    save.py -> session_handler -> user_client.download_media() -> progress_controller
    
This API module provides a convenience wrapper but does NOT replace the full pipeline.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Callable, Awaitable, Any, Union
from enum import Enum
import time
import os
import logging
import asyncio

logger = logging.getLogger(__name__)


# ==================== PUBLIC MODELS ====================

class DownloadStatus(Enum):
    """Status of a download operation."""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProgressEvent:
    """Progress update sent to callback."""
    request_id: str
    current_bytes: int
    total_bytes: int
    file_name: str = ""
    file_index: int = 0
    total_files: int = 1
    status: str = "downloading"
    
    @property
    def percent(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        return min(100.0, (self.current_bytes / self.total_bytes) * 100)


@dataclass
class DownloadRequest:
    """
    Request to download Telegram media.
    
    Bot layer constructs this from user input, then passes to DownloadService.
    """
    # User who requested the download (bot user ID)
    user_id: int
    
    # Session string for the userbot client
    session_string: str
    
    # Source chat (channel/group ID)
    source_chat_id: int
    
    # Message IDs to download (single or multiple)
    source_message_ids: List[int] = field(default_factory=list)
    
    # Single message ID (convenience - will be added to source_message_ids)
    source_message_id: Optional[int] = None
    
    # Original link (for reference/logging)
    source_link: Optional[str] = None
    
    # Is this an album request
    is_album: bool = False
    
    # Target folder for downloads
    target_folder: Optional[str] = None
    
    # Progress callback (optional)
    progress_callback: Optional[Callable[[ProgressEvent], Awaitable[None]]] = None
    
    # Bot message to reply to
    reply_to_message_id: Optional[int] = None
    
    # Internal tracking
    request_id: str = field(default_factory=lambda: f"req_{int(time.time() * 1000)}")
    created_at: float = field(default_factory=time.time)
    
    def __post_init__(self):
        # Normalize: add single message_id to list if provided
        if self.source_message_id and self.source_message_id not in self.source_message_ids:
            self.source_message_ids = [self.source_message_id] + list(self.source_message_ids)


@dataclass
class DownloadResult:
    """
    Result of a download operation.
    
    Returned by DownloadService.download().
    """
    success: bool
    status: DownloadStatus = DownloadStatus.COMPLETED
    
    # Downloaded files
    file_paths: List[str] = field(default_factory=list)
    file_names: List[str] = field(default_factory=list)
    total_bytes: int = 0
    files_count: int = 0
    
    # Timing
    elapsed_seconds: float = 0.0
    
    # Error info
    error_message: str = ""
    error_code: str = ""
    
    @property
    def file_path(self) -> str:
        """First file path (convenience for single-file downloads)."""
        return self.file_paths[0] if self.file_paths else ""


# ==================== DOWNLOAD SERVICE ====================

class DownloadService:
    """
    High-level download service - the ONLY entry point for bot code.
    
    This wraps the existing download logic from TechVJ/save.py and core/.
    Internals can be refactored without changing this interface.
    """
    
    def __init__(self):
        self._initialized = False
        self._cancel_flags: dict = {}
    
    async def initialize(self) -> None:
        """Initialize the service (call once at startup)."""
        if self._initialized:
            return
        self._initialized = True
        logger.info("DownloadService initialized")
    
    async def shutdown(self) -> None:
        """Shutdown the service."""
        self._initialized = False
        self._cancel_flags.clear()
        logger.info("DownloadService shutdown")
    
    async def download(
        self,
        request: DownloadRequest,
        bot_client: Any,
        status_message: Any = None
    ) -> DownloadResult:
        """
        Download media from Telegram.
        
        Args:
            request: DownloadRequest with source info
            bot_client: Pyrogram bot client (for sending results)
            status_message: Optional message to update with progress
        
        Returns:
            DownloadResult with file paths or error
        """
        start_time = time.time()
        
        # Validate request
        if not request.source_message_ids:
            return DownloadResult(
                success=False,
                status=DownloadStatus.FAILED,
                error_message="No message IDs provided"
            )
        
        if not request.session_string:
            return DownloadResult(
                success=False,
                status=DownloadStatus.FAILED,
                error_message="No session string provided"
            )
        
        try:
            # Use existing session handler
            from TechVJ.session_handler import (
                create_user_session,
                SessionInvalidError,
                SessionConnectionError
            )
            
            async with create_user_session(request.session_string, request.user_id) as client:
                # Check for cancellation
                if self._is_cancelled(request.request_id):
                    return DownloadResult(
                        success=False,
                        status=DownloadStatus.CANCELLED,
                        error_message="Cancelled by user"
                    )
                
                # Download messages
                result = await self._download_messages(
                    request=request,
                    user_client=client,
                    bot_client=bot_client
                )
                
                result.elapsed_seconds = time.time() - start_time
                return result
        
        except SessionInvalidError as e:
            logger.warning(f"Session invalid for user {request.user_id}: {e}")
            return DownloadResult(
                success=False,
                status=DownloadStatus.FAILED,
                error_message=f"Session invalid - please /login again",
                error_code="SESSION_INVALID"
            )
        
        except SessionConnectionError as e:
            logger.warning(f"Session connection error for user {request.user_id}: {e}")
            return DownloadResult(
                success=False,
                status=DownloadStatus.FAILED,
                error_message=f"Connection error - please try again",
                error_code="CONNECTION_ERROR"
            )
        
        except asyncio.CancelledError:
            return DownloadResult(
                success=False,
                status=DownloadStatus.CANCELLED,
                error_message="Cancelled"
            )
        
        except Exception as e:
            logger.error(f"Download error: {e}", exc_info=True)
            return DownloadResult(
                success=False,
                status=DownloadStatus.FAILED,
                error_message=str(e)[:500]
            )
        
        finally:
            self._clear_cancel(request.request_id)
    
    async def _download_messages(
        self,
        request: DownloadRequest,
        user_client: Any,
        bot_client: Any
    ) -> DownloadResult:
        """Download one or more messages."""
        from config import TEMP_DOWNLOAD_DIR
        
        # Determine download folder
        download_dir = request.target_folder or TEMP_DOWNLOAD_DIR
        os.makedirs(download_dir, exist_ok=True)
        
        file_paths = []
        file_names = []
        total_bytes = 0
        failed_count = 0
        
        total_messages = len(request.source_message_ids)
        
        for idx, msg_id in enumerate(request.source_message_ids):
            # Check cancellation
            if self._is_cancelled(request.request_id):
                break
            
            try:
                # Fetch message
                msg = await user_client.get_messages(request.source_chat_id, msg_id)
                
                if not msg or msg.empty:
                    logger.debug(f"Message {msg_id} is empty")
                    failed_count += 1
                    continue
                
                # Skip non-media messages
                if not msg.media:
                    logger.debug(f"Message {msg_id} has no media")
                    continue
                
                # Create progress wrapper if callback provided
                progress_func = None
                if request.progress_callback:
                    progress_func = self._create_progress_wrapper(
                        request=request,
                        file_index=idx,
                        total_files=total_messages,
                        file_name=self._get_file_name(msg)
                    )
                
                # Download using Pyrogram
                file_path = await user_client.download_media(
                    msg,
                    file_name=f"{download_dir}/",
                    progress=progress_func
                )
                
                if file_path and os.path.exists(file_path):
                    file_size = os.path.getsize(file_path)
                    file_paths.append(file_path)
                    file_names.append(os.path.basename(file_path))
                    total_bytes += file_size
                    logger.debug(f"Downloaded: {file_path} ({file_size} bytes)")
                else:
                    failed_count += 1
                    logger.warning(f"Download produced no file for msg {msg_id}")
            
            except Exception as e:
                logger.warning(f"Failed to download message {msg_id}: {e}")
                failed_count += 1
                continue
            
            # Small delay between downloads
            if idx < total_messages - 1:
                await asyncio.sleep(0.5)
        
        # Build result
        if file_paths:
            return DownloadResult(
                success=True,
                status=DownloadStatus.COMPLETED,
                file_paths=file_paths,
                file_names=file_names,
                total_bytes=total_bytes,
                files_count=len(file_paths)
            )
        elif failed_count > 0:
            return DownloadResult(
                success=False,
                status=DownloadStatus.FAILED,
                error_message=f"All {failed_count} download(s) failed"
            )
        else:
            # No media found (all text messages)
            return DownloadResult(
                success=True,
                status=DownloadStatus.COMPLETED,
                files_count=0
            )
    
    def _create_progress_wrapper(
        self,
        request: DownloadRequest,
        file_index: int,
        total_files: int,
        file_name: str
    ) -> Callable:
        """Create a progress callback wrapper for Pyrogram."""
        async def wrapper(current: int, total: int):
            if request.progress_callback:
                event = ProgressEvent(
                    request_id=request.request_id,
                    current_bytes=current,
                    total_bytes=total,
                    file_name=file_name,
                    file_index=file_index,
                    total_files=total_files,
                    status="downloading"
                )
                try:
                    await request.progress_callback(event)
                except Exception:
                    pass  # Don't fail download due to callback error
        return wrapper
    
    def _get_file_name(self, msg: Any) -> str:
        """Extract file name from message."""
        if msg.document and msg.document.file_name:
            return msg.document.file_name
        elif msg.video and msg.video.file_name:
            return msg.video.file_name
        elif msg.audio and msg.audio.file_name:
            return msg.audio.file_name
        else:
            return "file"
    
    # ==================== CANCELLATION ====================
    
    def cancel(self, request_id: str) -> None:
        """Cancel a specific download."""
        self._cancel_flags[request_id] = True
    
    def _is_cancelled(self, request_id: str) -> bool:
        """Check if download is cancelled."""
        return self._cancel_flags.get(request_id, False)
    
    def _clear_cancel(self, request_id: str) -> None:
        """Clear cancel flag."""
        self._cancel_flags.pop(request_id, None)
    
    async def cancel_user_downloads(self, user_id: int) -> int:
        """Cancel all downloads for a user."""
        try:
            from TechVJ.task_manager import task_manager
            return await task_manager.cancel_all_tasks(user_id)
        except ImportError:
            logger.warning("task_manager not available")
            return 0


# ==================== GLOBAL INSTANCE ====================

_service_instance: Optional[DownloadService] = None


def get_download_service() -> DownloadService:
    """Get or create the global DownloadService instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = DownloadService()
    return _service_instance


async def init_download_service() -> DownloadService:
    """Initialize and return the global DownloadService."""
    service = get_download_service()
    await service.initialize()
    return service
