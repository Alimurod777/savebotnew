"""
core/downloader/engine.py - Main download engine for Telegram media.

Features:
- Async download with progress tracking
- Adaptive worker pool
- Retry logic with FloodWait handling
- Cancellation support
- FILE_REFERENCE recovery
- Resume support
"""

import asyncio
import os
import time
import uuid
import logging
from typing import Optional, Callable, Any, Dict

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, FileReferenceExpired, FileReferenceInvalid

from core.downloader.progress import ProgressTracker, progress_tracker
from core.downloader.worker import WorkerPool, DownloadTask, TaskStatus
from core.downloader.resume import ResumableDownloader, download_with_resume, ResumeDownloadManager

logger = logging.getLogger(__name__)

# Constants
DEFAULT_TEMP_DIR = "downloads/temp"
MAX_RETRIES = 5
BASE_RETRY_DELAY = 2.0


class DownloadEngine:
    """
    Main download engine for Telegram media.
    
    Adaptive worker pool: 32 default, scales up to 150.
    
    Usage:
        engine = DownloadEngine()
        await engine.start()
        
        file_path = await engine.download(
            message=msg,
            client=user_client,
            progress_callback=callback
        )
        
        await engine.shutdown()
    """
    
    def __init__(
        self,
        temp_dir: str = DEFAULT_TEMP_DIR
    ):
        self.temp_dir = temp_dir
        
        self._pool: Optional[WorkerPool] = None
        self._progress: ProgressTracker = progress_tracker
        self._running = False
        
        # Cancel tracking
        self._cancel_flags: Dict[str, bool] = {}
    
    async def start(self) -> None:
        """Start the download engine."""
        if self._running:
            return
        
        os.makedirs(self.temp_dir, exist_ok=True)
        
        self._pool = WorkerPool(
            download_handler=self._handle_download
        )
        await self._pool.start()
        
        self._running = True
        logger.info("DownloadEngine started (adaptive workers: 4-16)")
    
    async def shutdown(self) -> None:
        """Shutdown the download engine."""
        if not self._running:
            return
        
        self._running = False
        
        if self._pool:
            await self._pool.shutdown()
        
        self._progress.cleanup_all()
        logger.info("DownloadEngine shutdown complete")
    
    async def download(
        self,
        message: Message,
        client: Client,
        progress_callback: Callable = None,
        status_message: Message = None,
        user_id: int = None,
        download_dir: str = None,
        bot_client: Client = None
    ) -> Optional[str]:
        """
        Download media from a Telegram message.
        
        Args:
            message: Pyrogram Message containing media
            client: Pyrogram Client (userbot) for downloading
            progress_callback: Optional custom progress callback
            status_message: Message to update with progress
            user_id: User ID for tracking
            download_dir: Custom download directory
            bot_client: Bot client for editing progress messages (if different from download client)
        
        Returns:
            File path on success, None on failure
        """
        if not self._running:
            # Fallback to direct download if engine not started
            return await self._direct_download(
                message, client, progress_callback, download_dir
            )
        
        # Generate task ID
        task_id = f"dl_{uuid.uuid4().hex[:12]}"
        
        # Create progress callback if status message provided
        # Use bot_client for editing messages (status_msg was created by bot)
        if status_message and not progress_callback:
            progress_client = bot_client if bot_client else client
            progress_callback = self._progress.create_callback(
                client=progress_client,
                status_message=status_message,
                transfer_type="download",
                transfer_id=task_id
            )
        
        # Create task
        task = DownloadTask(
            task_id=task_id,
            user_id=user_id or (message.from_user.id if message.from_user else 0),
            message=message,
            client=client,
            progress_callback=progress_callback
        )
        task._download_dir = download_dir or self.temp_dir
        
        # Submit to pool
        result = await self._pool.submit(task)
        
        # Cleanup progress
        self._progress.cleanup(task_id)
        
        return result
    
    async def download_nowait(
        self,
        message: Message,
        client: Client,
        progress_callback: Callable = None,
        user_id: int = None
    ) -> str:
        """Submit download without waiting. Returns task_id."""
        task_id = f"dl_{uuid.uuid4().hex[:12]}"
        
        task = DownloadTask(
            task_id=task_id,
            user_id=user_id or 0,
            message=message,
            client=client,
            progress_callback=progress_callback
        )
        task._download_dir = self.temp_dir
        
        self._pool.submit_nowait(task)
        return task_id
    
    async def _handle_download(self, task: DownloadTask) -> Optional[str]:
        """
        Handle a download task with RESUME support.

        Uses stream_media with offset for resumable downloads.
        Large files use .part files, small files use RAM buffer.

        EARLY EXIT: if the message has no downloadable media (e.g. web-page
        preview), return None immediately without any retry loop.
        This prevents the "No downloadable media" × 5 spam.
        """
        message = task.message
        client = task.client
        download_dir = getattr(task, '_download_dir', self.temp_dir)
        use_resume = getattr(task, '_use_resume', True)

        # PATCH: attribute-based media check — no MessageMediaType enum
        from core.media_classifier import has_downloadable_media
        if not has_downloadable_media(message):
            logger.debug(
                "_handle_download: msg %s has no downloadable media, skipping",
                getattr(message, "id", "?"),
            )
            return None

        os.makedirs(download_dir, exist_ok=True)
        
        for attempt in range(MAX_RETRIES):
            # Check cancellation
            if self._pool.is_cancelled(task.task_id):
                return None
            
            try:
                if use_resume:
                    # Use resumable download with stream_media
                    file_path = await download_with_resume(
                        client=client,
                        message=message,
                        download_dir=download_dir,
                        progress_callback=task.progress_callback,
                        cancel_event=task.cancel_event
                    )
                else:
                    # Fallback to standard download_media
                    file_path = await client.download_media(
                        message,
                        file_name=f"{download_dir}/",
                        progress=task.progress_callback
                    )
                
                if file_path and os.path.exists(file_path):
                    if os.path.getsize(file_path) > 0:
                        return file_path
                    # Interrupted GetFile (e.g. -503 Timeout) can leave a
                    # 0-byte file that still passes os.path.exists(). Treat
                    # it as a failed attempt instead of a "successful" empty
                    # download, so the retry loop below kicks in.
                    try:
                        os.remove(file_path)
                    except OSError:
                        pass
                    raise Exception(f"Download produced 0-byte file: {file_path}")
                else:
                    raise Exception("Download produced no file")
            
            except FloodWait as e:
                wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                logger.warning(f"FloodWait: {wait_time}s on task {task.task_id}")
                
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(min(wait_time, 60))
                else:
                    raise
            
            except (FileReferenceExpired, FileReferenceInvalid) as e:
                logger.warning(f"File reference error: {e}")
                
                # Try to refresh message
                if attempt < MAX_RETRIES - 1:
                    try:
                        fresh_msg = await client.get_messages(
                            message.chat.id,
                            message.id
                        )
                        if fresh_msg and not fresh_msg.empty:
                            task.message = fresh_msg
                            message = fresh_msg
                            await asyncio.sleep(BASE_RETRY_DELAY)
                            continue
                    except Exception:
                        pass
                raise
            
            except asyncio.CancelledError:
                logger.info(f"Download cancelled: {task.task_id}")
                raise

            except FileNotFoundError as e:
                # Temp directory was deleted (user /stop) — don't retry
                logger.info(f"Download aborted (temp removed): {task.task_id}: {e}")
                return None

            except Exception as e:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BASE_RETRY_DELAY * (attempt + 1))
                else:
                    raise
        
        return None
    
    async def _direct_download(
        self,
        message: Message,
        client: Client,
        progress_callback: Callable = None,
        download_dir: str = None
    ) -> Optional[str]:
        """Direct download without worker pool (fallback)."""
        download_dir = download_dir or self.temp_dir
        os.makedirs(download_dir, exist_ok=True)
        
        for attempt in range(MAX_RETRIES):
            try:
                file_path = await client.download_media(
                    message,
                    file_name=f"{download_dir}/",
                    progress=progress_callback
                )
                
                if file_path and os.path.exists(file_path):
                    if os.path.getsize(file_path) > 0:
                        return file_path
                    try:
                        os.remove(file_path)
                    except OSError:
                        pass
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(BASE_RETRY_DELAY * (attempt + 1))
                        continue
                    raise Exception(f"Download produced 0-byte file: {file_path}")
            
            except FloodWait as e:
                wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(min(wait_time, 60))
                else:
                    raise
            
            except (FileReferenceExpired, FileReferenceInvalid):
                if attempt < MAX_RETRIES - 1:
                    try:
                        message = await client.get_messages(
                            message.chat.id,
                            message.id
                        )
                        await asyncio.sleep(BASE_RETRY_DELAY)
                        continue
                    except Exception:
                        pass
                raise
            
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BASE_RETRY_DELAY * (attempt + 1))
                else:
                    raise
        
        return None
    
    def create_progress_callback(
        self,
        client: Client,
        status_message: Message,
        transfer_type: str = "download"
    ) -> Callable:
        """
        Create a progress callback for external use.
        
        Can be used for uploads too:
            callback = engine.create_progress_callback(client, msg, "upload")
            await client.send_video(..., progress=callback)
        """
        transfer_id = f"{transfer_type}_{uuid.uuid4().hex[:8]}"
        return self._progress.create_callback(
            client=client,
            status_message=status_message,
            transfer_type=transfer_type,
            transfer_id=transfer_id
        )
    
    async def cancel_download(self, task_id: str) -> bool:
        """Cancel a specific download."""
        self._cancel_flags[task_id] = True
        self._progress.cancel(task_id)
        
        if self._pool:
            return await self._pool.cancel_task(task_id)
        return False
    
    async def cancel_user_downloads(self, user_id: int) -> int:
        """Cancel all downloads for a user."""
        if self._pool:
            return await self._pool.cancel_user_tasks(user_id)
        return 0
    
    async def cancel_all_downloads(self) -> int:
        """Cancel ALL active downloads (emergency stop)."""
        if self._pool:
            return await self._pool.cancel_all_tasks()
        return 0
    
    def get_stats(self) -> Dict:
        """Get engine statistics."""
        stats = {
            "running": self._running,
            "temp_dir": self.temp_dir
        }
        if self._pool:
            stats.update(self._pool.get_stats())
        return stats


# Global instance
_engine: Optional[DownloadEngine] = None


async def get_engine() -> DownloadEngine:
    """Get or create the global download engine (adaptive: 4-16 workers)."""
    global _engine
    if _engine is None:
        _engine = DownloadEngine()
        await _engine.start()
    return _engine


async def shutdown_engine() -> None:
    """Shutdown the global engine."""
    global _engine
    if _engine:
        await _engine.shutdown()
        _engine = None
