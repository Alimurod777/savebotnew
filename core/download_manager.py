"""
Download Manager - Adaptive Worker Pool

Central coordinator for all downloads:
- asyncio.Queue for task distribution
- Adaptive worker count based on CPU + RAM
- Resume support on startup
- Clean shutdown with state preservation
- Per-user task tracking for cancellation
"""

import asyncio
import os
import time
import logging
from typing import Optional, Dict, Set, List

from pyrogram import Client
from pyrogram.types import Message

from core.models import DownloadTask, ProgressEvent, SHUTDOWN_SENTINEL
from core.state_store import state_store
from core.telegram_downloader import TelegramDownloader, extract_file_info
from core.progress_manager import get_progress_manager

logger = logging.getLogger(__name__)

# Adaptive worker count: min(cpu_count * 4, available_ram_based_limit)
# Replaces the old fixed 150 worker count
def _calculate_download_workers() -> int:
    """Calculate download worker count based on system resources."""
    try:
        import psutil
        cpu_count = os.cpu_count() or 2
        available_ram_gb = psutil.virtual_memory().available / (1024 ** 3)
        # Each download worker uses ~10MB RAM for buffers
        ram_based_limit = int((available_ram_gb * 1024) / 10)
        workers = min(cpu_count * 4, ram_based_limit)
        # Clamp between 8 and 150
        return max(8, min(workers, 150))
    except ImportError:
        # psutil not available — conservative default
        cpu_count = os.cpu_count() or 2
        return max(8, min(cpu_count * 4, 64))

WORKER_COUNT = _calculate_download_workers()
MAX_QUEUE_SIZE = 50000
TEMP_DIR = "downloads/temp"


class DownloadManager:
    """
    Manages download queue and worker pool.
    
    Features:
    - Adaptive worker count: min(cpu_count * 4, ram_based_limit)
    - Central asyncio.Queue for task distribution
    - Resume incomplete downloads on startup
    - Clean cancellation support
    - Per-user task tracking
    """
    
    def __init__(self, client: Client):
        self.client = client
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._workers: List[asyncio.Task] = []
        self._running = False
        self._downloader: Optional[TelegramDownloader] = None
        
        # Track active downloads per user
        self._user_tasks: Dict[int, Set[str]] = {}
        
        # Cancel flags for tasks
        self._cancel_flags: Dict[str, bool] = {}
        
        # Lock for user task tracking
        self._lock = asyncio.Lock()
    
    async def start(self) -> None:
        """Start the download manager and all workers."""
        if self._running:
            logger.warning("DownloadManager already running")
            return
        
        # Ensure temp directory exists
        os.makedirs(TEMP_DIR, exist_ok=True)
        
        # Initialize downloader
        self._downloader = TelegramDownloader(self.client)
        
        # Resume incomplete tasks from database
        await self._resume_incomplete()
        
        # Start exactly 150 workers
        self._running = True
        for i in range(WORKER_COUNT):
            worker = asyncio.create_task(
                self._worker_loop(i),
                name=f"download_worker_{i}"
            )
            self._workers.append(worker)
        
        logger.info(f"DownloadManager started with {WORKER_COUNT} workers")
    
    async def stop(self) -> None:
        """Stop all workers gracefully."""
        if not self._running:
            return
        
        logger.info("Stopping DownloadManager...")
        self._running = False
        
        # Send shutdown sentinels to all workers
        for _ in range(WORKER_COUNT):
            try:
                self._queue.put_nowait(SHUTDOWN_SENTINEL)
            except asyncio.QueueFull:
                break
        
        # Wait for workers to finish (with timeout)
        if self._workers:
            done, pending = await asyncio.wait(
                self._workers,
                timeout=30.0,
                return_when=asyncio.ALL_COMPLETED
            )
            
            # Cancel any remaining workers
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._workers.clear()
        logger.info("DownloadManager stopped")
    
    async def enqueue(self, task: DownloadTask) -> bool:
        """
        Add a download task to the queue.
        
        Returns True if enqueued, False if duplicate or error.
        """
        # Check for duplicate
        existing = await state_store.check_duplicate(task.user_id, task.file_unique_id)
        if existing:
            if existing.status == "COMPLETED":
                logger.info(f"File already downloaded: {task.file_unique_id}")
                return False
            elif existing.status in ("PENDING", "DOWNLOADING"):
                logger.info(f"File already in queue: {task.file_unique_id}")
                return False
        
        # Save to database
        if not await state_store.insert_task(task):
            logger.warning(f"Failed to insert task: {task.task_id}")
            return False
        
        # Track user's task
        async with self._lock:
            if task.user_id not in self._user_tasks:
                self._user_tasks[task.user_id] = set()
            self._user_tasks[task.user_id].add(task.task_id)
        
        # Add to queue
        try:
            await self._queue.put(task)
            logger.debug(f"Task enqueued: {task.task_id}")
            return True
        except asyncio.QueueFull:
            logger.error("Download queue is full!")
            await state_store.update_status(task.task_id, "FAILED", "Queue full")
            return False
    
    async def enqueue_from_message(
        self,
        user_id: int,
        message: Message,
        priority: int = 10
    ) -> Optional[DownloadTask]:
        """
        Create and enqueue a download task from a Telegram message.
        
        Returns the task if enqueued, None otherwise.
        """
        # Extract file info from message
        task = await extract_file_info(self.client, message, user_id)
        if not task:
            logger.debug("No media found in message")
            return None
        
        # Set priority
        task.priority = priority
        task.progress_chat_id = message.chat.id
        
        # Check for duplicate
        existing = await state_store.check_duplicate(user_id, task.file_unique_id)
        if existing:
            if existing.status == "COMPLETED":
                logger.info(f"File already downloaded: {task.file_unique_id}")
                return existing  # Return existing completed task
            elif existing.status in ("PENDING", "DOWNLOADING"):
                logger.info(f"File already in queue: {task.file_unique_id}")
                return existing
        
        # Create progress message
        pm = get_progress_manager()
        if pm:
            msg_id = await pm.create_progress_message(
                chat_id=message.chat.id,
                task_id=task.task_id,
                file_name=task.file_name,
                file_size=task.file_size,
                user_id=user_id
            )
            if msg_id:
                task.progress_message_id = msg_id
                task.progress_chat_id = message.chat.id
        
        # Enqueue
        if await self.enqueue(task):
            return task
        return None
    
    async def cancel_user_downloads(self, user_id: int) -> int:
        """Cancel all downloads for a user."""
        async with self._lock:
            task_ids = self._user_tasks.get(user_id, set()).copy()
        
        for task_id in task_ids:
            self._cancel_flags[task_id] = True
            if self._downloader:
                self._downloader.cancel_download(task_id)
        
        # Update database
        cancelled = await state_store.cancel_user_tasks(user_id)
        
        # Update progress messages
        pm = get_progress_manager()
        if pm:
            for task_id in task_ids:
                await pm.update_final_status(
                    task_id=task_id,
                    success=False,
                    error="Cancelled by user"
                )
        
        # Clear tracking
        async with self._lock:
            self._user_tasks.pop(user_id, None)
        
        return cancelled
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a specific download task."""
        self._cancel_flags[task_id] = True
        if self._downloader:
            self._downloader.cancel_download(task_id)
        
        cancelled = await state_store.cancel_task(task_id)
        
        pm = get_progress_manager()
        if pm:
            await pm.update_final_status(
                task_id=task_id,
                success=False,
                error="Cancelled"
            )
        
        return cancelled
    
    def is_cancelled(self, task_id: str) -> bool:
        """Check if a task is cancelled."""
        return self._cancel_flags.get(task_id, False)
    
    async def get_queue_status(self) -> dict:
        """Get current queue status."""
        stats = await state_store.get_stats()
        return {
            'queue_size': self._queue.qsize(),
            'workers_total': WORKER_COUNT,
            'workers_active': sum(1 for w in self._workers if not w.done()),
            **stats
        }
    
    async def get_user_task_count(self, user_id: int) -> int:
        """Get number of active tasks for a user."""
        async with self._lock:
            return len(self._user_tasks.get(user_id, set()))
    
    async def _resume_incomplete(self) -> None:
        """Load and re-enqueue incomplete tasks from database."""
        tasks = await state_store.load_incomplete_tasks()
        
        if not tasks:
            logger.info("No incomplete tasks to resume")
            return
        
        logger.info(f"Resuming {len(tasks)} incomplete tasks")
        
        for task in tasks:
            # Track user's task
            async with self._lock:
                if task.user_id not in self._user_tasks:
                    self._user_tasks[task.user_id] = set()
                self._user_tasks[task.user_id].add(task.task_id)
            
            # Re-enqueue (bypass duplicate check since we're resuming)
            try:
                await self._queue.put(task)
                logger.debug(f"Resumed task: {task.task_id} at offset {task.offset}")
            except asyncio.QueueFull:
                logger.warning(f"Queue full, couldn't resume: {task.task_id}")
    
    async def _worker_loop(self, worker_id: int) -> None:
        """Worker loop - processes tasks from queue."""
        logger.debug(f"Worker {worker_id} started")
        
        while self._running:
            try:
                # Get task from queue with timeout
                try:
                    task = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Check for shutdown sentinel
                if task is SHUTDOWN_SENTINEL:
                    self._queue.task_done()
                    break
                
                # Check if cancelled
                if self.is_cancelled(task.task_id):
                    self._queue.task_done()
                    continue
                
                # Process the download
                try:
                    await self._process_task(task, worker_id)
                finally:
                    self._queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
                await asyncio.sleep(1)
        
        logger.debug(f"Worker {worker_id} stopped")
    
    async def _process_task(self, task: DownloadTask, worker_id: int) -> None:
        """Process a single download task."""
        pm = get_progress_manager()
        
        try:
            # Update status to downloading
            await state_store.update_status(task.task_id, "DOWNLOADING")
            
            # Progress callback (sync, called frequently)
            def on_progress(event: ProgressEvent):
                if pm:
                    pm.submit(event)
            
            # Offset save callback (async, called every ~1MB)
            async def on_offset_save(task_id: str, offset: int):
                await state_store.update_offset(task_id, offset)
            
            # Perform download
            result = await self._downloader.download(
                task=task,
                on_progress=on_progress,
                on_offset_save=on_offset_save
            )
            
            if result.success:
                # Update status to completed
                await state_store.update_status(task.task_id, "COMPLETED")
                
                # Update progress message
                if pm:
                    await pm.update_final_status(
                        task_id=task.task_id,
                        success=True,
                        file_path=result.file_path
                    )
                
                logger.info(
                    f"Download completed: {task.task_id} "
                    f"({result.bytes_downloaded / 1024 / 1024:.1f} MB "
                    f"in {result.elapsed_seconds:.1f}s)"
                )
                
            elif result.error == "FILE_REFERENCE_EXPIRED":
                # File reference expired - need to refresh
                await self._handle_file_reference_expired(task, pm)
                
            elif result.error == "Cancelled":
                await state_store.update_status(task.task_id, "CANCELED")
                # Progress message already updated by cancel function
                
            else:
                # Other failure - retry if attempts remain
                await self._handle_download_failure(task, result, pm)
        
        except Exception as e:
            logger.error(f"Task processing error: {task.task_id} - {e}", exc_info=True)
            await state_store.update_status(task.task_id, "FAILED", str(e))
            if pm:
                await pm.update_final_status(
                    task_id=task.task_id,
                    success=False,
                    error=str(e)
                )
        
        finally:
            # Cleanup tracking
            async with self._lock:
                if task.user_id in self._user_tasks:
                    self._user_tasks[task.user_id].discard(task.task_id)
            self._cancel_flags.pop(task.task_id, None)
    
    async def _handle_file_reference_expired(
        self,
        task: DownloadTask,
        pm
    ) -> None:
        """
        Handle file reference expiration with message reload.
        
        Strategy:
        1. Re-fetch the original message to get fresh file_reference
        2. Update task with new file_reference
        3. Re-queue for download
        4. If refresh fails, mark as failed
        """
        retry_count = await state_store.increment_retry(task.task_id)
        
        if retry_count >= task.max_retries:
            # FIX Issue 7.2: Improved user-facing error message for file reference expiry
            user_error = (
                "❌ Media link expired.\n\n"
                "The download link has expired and could not be refreshed.\n"
                "Please share a fresh link to retry."
            )
            await state_store.update_status(
                task.task_id,
                "FAILED",
                user_error
            )
            if pm:
                await pm.update_final_status(
                    task_id=task.task_id,
                    success=False,
                    error=user_error
                )
            return
        
        # Try to refresh file reference by re-fetching the message
        try:
            logger.info(f"Refreshing file reference for {task.task_id} (attempt {retry_count + 1})")
            
            # Re-fetch the message to get fresh file_reference
            fresh_message = await self.client.get_messages(
                chat_id=task.chat_id,
                message_ids=task.message_id
            )
            
            if not fresh_message or fresh_message.empty or not fresh_message.media:
                raise ValueError("Message no longer available or has no media")
            
            # Re-extract file info with fresh file_reference
            from core.telegram_downloader import extract_file_info
            fresh_task = await extract_file_info(self.client, fresh_message, task.user_id)
            
            if not fresh_task:
                raise ValueError("Could not extract file info from refreshed message")
            
            # Update task with fresh file_reference while preserving progress
            task.file_reference = fresh_task.file_reference
            task.file_id = fresh_task.file_id
            task.access_hash = fresh_task.access_hash
            task.retry_count = retry_count
            
            # Update database with new file_reference
            await state_store.update_file_reference(
                task.task_id, 
                task.file_reference,
                task.file_id
            )
            
            # Exponential backoff before retry
            backoff = min(30, 2 ** retry_count)
            await asyncio.sleep(backoff)
            
            # Re-queue for download
            try:
                await self._queue.put(task)
                logger.info(
                    f"Re-queued task {task.task_id} with refreshed file reference "
                    f"(retry {retry_count + 1}/{task.max_retries})"
                )
            except asyncio.QueueFull:
                await state_store.update_status(
                    task.task_id,
                    "FAILED",
                    "Queue full during file reference refresh"
                )
                if pm:
                    await pm.update_final_status(
                        task_id=task.task_id,
                        success=False,
                        error="Queue full during file reference refresh"
                    )
        
        except Exception as e:
            logger.error(f"Failed to refresh file reference for {task.task_id}: {e}")
            await state_store.update_status(
                task.task_id,
                "FAILED",
                f"File reference refresh failed: {str(e)[:100]}"
            )
            if pm:
                await pm.update_final_status(
                    task_id=task.task_id,
                    success=False,
                    error=f"File reference refresh failed: {str(e)[:100]}"
                )
    
    async def _handle_download_failure(
        self,
        task: DownloadTask,
        result,
        pm
    ) -> None:
        """Handle download failure with retry logic."""
        retry_count = await state_store.increment_retry(task.task_id)
        
        if retry_count < task.max_retries:
            # Save current offset and re-queue
            await state_store.update_offset(
                task.task_id,
                result.bytes_downloaded
            )
            
            # Update task for retry
            task.offset = result.bytes_downloaded
            task.downloaded_bytes = result.bytes_downloaded
            task.retry_count = retry_count
            
            # Exponential backoff
            backoff = min(30, 2 ** retry_count)
            await asyncio.sleep(backoff)
            
            # Re-queue
            try:
                await self._queue.put(task)
                logger.info(f"Retry {retry_count}/{task.max_retries} for {task.task_id}")
            except asyncio.QueueFull:
                await state_store.update_status(
                    task.task_id,
                    "FAILED",
                    f"Queue full during retry: {result.error}"
                )
                if pm:
                    await pm.update_final_status(
                        task_id=task.task_id,
                        success=False,
                        error=f"Retry failed: {result.error}"
                    )
        else:
            await state_store.update_status(
                task.task_id,
                "FAILED",
                f"Max retries exceeded: {result.error}"
            )
            if pm:
                await pm.update_final_status(
                    task_id=task.task_id,
                    success=False,
                    error=f"Failed after {task.max_retries} retries: {result.error}"
                )


# Global instance
_download_manager: Optional[DownloadManager] = None


async def init_download_manager(client: Client) -> DownloadManager:
    """Initialize global download manager."""
    global _download_manager
    _download_manager = DownloadManager(client)
    await _download_manager.start()
    return _download_manager


def get_download_manager() -> Optional[DownloadManager]:
    """Get global download manager."""
    return _download_manager
