"""
DEPRECATED: Use TechVJ/progress_controller.py instead.

This module is kept for backwards compatibility but should not be used.
The canonical progress system is TechVJ/progress_controller.py which:
- Runs on the same event loop as the bot
- Uses call_soon_threadsafe for thread-safe scheduling
- Handles FloodWait properly

Migration:
    # OLD
    from core.progress_manager import get_progress_manager
    
    # NEW
    from TechVJ.progress_controller import progress_controller
    callback = progress_controller.create_callback(...)
"""

import warnings
warnings.warn(
    "core.progress_manager is deprecated. Use TechVJ.progress_controller instead.",
    DeprecationWarning,
    stacklevel=2
)

# Original docstring:
# Progress Manager - Rate-Limited Progress Updates
# Aggregates progress events from download workers and updates
# Telegram messages with rate limiting to prevent FloodWait errors.

import asyncio
import time
import logging
from typing import Dict, Optional, Set
from dataclasses import dataclass, field

from pyrogram import Client
from pyrogram.errors import (
    FloodWait, MessageNotModified, MessageIdInvalid,
    ChatWriteForbidden, UserIsBlocked
)

from core.models import ProgressEvent

logger = logging.getLogger(__name__)

# Configuration
MIN_UPDATE_INTERVAL = 1.5  # Minimum seconds between updates per task
PROGRESS_BAR_LENGTH = 12
BATCH_INTERVAL = 0.5  # Process queue every 0.5 seconds
MAX_QUEUE_SIZE = 10000  # Prevent memory issues


@dataclass
class TaskProgress:
    """Track progress state for a single task."""
    task_id: str
    user_id: int
    chat_id: int
    message_id: int
    file_name: str = ""
    current_bytes: int = 0
    total_bytes: int = 0
    speed: float = 0.0
    eta: float = 0.0
    status: str = "PENDING"
    last_update_time: float = 0.0
    last_message_text: str = ""
    error: str = ""
    created_at: float = field(default_factory=time.time)


class ProgressManager:
    """
    Manages progress updates for all active downloads.
    
    Uses an async queue to receive progress events from workers
    and rate-limits Telegram message edits.
    """
    
    def __init__(self, client: Client):
        self.client = client
        self._queue: asyncio.Queue[ProgressEvent] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._tasks: Dict[str, TaskProgress] = {}
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._pending_updates: Set[str] = set()
    
    async def start(self) -> None:
        """Start the progress manager."""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(
            self._process_loop(),
            name="progress_manager"
        )
        logger.info("ProgressManager started")
    
    async def stop(self) -> None:
        """Stop the progress manager gracefully."""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
        self._tasks.clear()
        self._pending_updates.clear()
        logger.info("ProgressManager stopped")
    
    def submit(self, event: ProgressEvent) -> bool:
        """
        Submit a progress event (non-blocking).
        
        Returns True if event was queued, False if queue is full.
        """
        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            logger.warning("Progress queue full, dropping event")
            return False
    
    async def create_progress_message(
        self,
        chat_id: int,
        task_id: str,
        file_name: str,
        file_size: int,
        user_id: int
    ) -> Optional[int]:
        """
        Create initial progress message.
        
        Returns the message ID for future updates.
        """
        try:
            text = self._format_initial_message(file_name, file_size)
            msg = await self.client.send_message(chat_id, text)
            
            # Track this task
            self._tasks[task_id] = TaskProgress(
                task_id=task_id,
                user_id=user_id,
                chat_id=chat_id,
                message_id=msg.id,
                file_name=file_name,
                total_bytes=file_size,
                status="PENDING"
            )
            
            return msg.id
            
        except FloodWait as e:
            wait = getattr(e, 'value', getattr(e, 'x', 5))
            logger.warning(f"FloodWait creating progress message: {wait}s")
            await asyncio.sleep(wait)
            # Retry once
            try:
                text = self._format_initial_message(file_name, file_size)
                msg = await self.client.send_message(chat_id, text)
                self._tasks[task_id] = TaskProgress(
                    task_id=task_id,
                    user_id=user_id,
                    chat_id=chat_id,
                    message_id=msg.id,
                    file_name=file_name,
                    total_bytes=file_size,
                    status="PENDING"
                )
                return msg.id
            except Exception:
                return None
        except Exception as e:
            logger.error(f"Failed to create progress message: {e}")
            return None
    
    async def update_final_status(
        self,
        task_id: str,
        success: bool,
        error: str = "",
        file_path: str = ""
    ) -> None:
        """Update message with final status (success/failure)."""
        task = self._tasks.get(task_id)
        if not task:
            return
        
        try:
            if success:
                # Calculate final stats
                elapsed = time.time() - task.created_at
                size_mb = task.total_bytes / (1024 * 1024)
                avg_speed = (task.total_bytes / elapsed / (1024 * 1024)) if elapsed > 0 else 0
                
                text = (
                    f"**Download Complete**\n\n"
                    f"`{task.file_name}`\n"
                    f"Size: {size_mb:.1f} MB\n"
                    f"Avg Speed: {avg_speed:.2f} MB/s\n"
                    f"Time: {self._format_time(elapsed)}"
                )
            else:
                text = (
                    f"**Download Failed**\n\n"
                    f"`{task.file_name}`\n"
                    f"Error: {error or 'Unknown error'}"
                )
            
            await self.client.edit_message_text(
                chat_id=task.chat_id,
                message_id=task.message_id,
                text=text
            )
            
        except MessageNotModified:
            pass
        except MessageIdInvalid:
            pass
        except Exception as e:
            logger.debug(f"Failed to update final status: {e}")
        finally:
            # Remove from tracking
            self._tasks.pop(task_id, None)
            self._pending_updates.discard(task_id)
    
    async def delete_progress_message(self, task_id: str) -> None:
        """Delete progress message."""
        task = self._tasks.pop(task_id, None)
        self._pending_updates.discard(task_id)
        if task:
            try:
                await self.client.delete_messages(task.chat_id, task.message_id)
            except Exception:
                pass
    
    def remove_task(self, task_id: str) -> None:
        """Remove task from tracking without sending messages."""
        self._tasks.pop(task_id, None)
        self._pending_updates.discard(task_id)
    
    async def _process_loop(self) -> None:
        """Main loop - process progress events."""
        while self._running:
            try:
                # Collect events for a short interval
                await self._collect_events()
                
                # Process pending updates
                await self._process_pending_updates()
                
                await asyncio.sleep(BATCH_INTERVAL)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Progress loop error: {e}")
                await asyncio.sleep(1)
    
    async def _collect_events(self) -> None:
        """Collect events from queue without blocking."""
        collected = 0
        max_collect = 1000  # Limit per batch
        
        while collected < max_collect:
            try:
                event = self._queue.get_nowait()
                self._update_task_progress(event)
                self._pending_updates.add(event.task_id)
                collected += 1
            except asyncio.QueueEmpty:
                break
    
    def _update_task_progress(self, event: ProgressEvent) -> None:
        """Update internal task state from event."""
        task = self._tasks.get(event.task_id)
        if task:
            task.current_bytes = event.current_bytes
            task.total_bytes = event.total_bytes
            task.speed = event.speed
            task.eta = event.eta
            task.status = event.status
            if event.error:
                task.error = event.error
    
    async def _process_pending_updates(self) -> None:
        """Send updates for tasks that need it."""
        now = time.time()
        tasks_to_update = list(self._pending_updates)
        self._pending_updates.clear()
        
        for task_id in tasks_to_update:
            task = self._tasks.get(task_id)
            if not task:
                continue
            
            # Rate limit check
            if now - task.last_update_time < MIN_UPDATE_INTERVAL:
                self._pending_updates.add(task_id)  # Re-queue for later
                continue
            
            # Format message
            text = self._format_progress_message(task)
            
            # Skip if message hasn't changed
            if text == task.last_message_text:
                continue
            
            try:
                await self.client.edit_message_text(
                    chat_id=task.chat_id,
                    message_id=task.message_id,
                    text=text
                )
                task.last_update_time = now
                task.last_message_text = text
                
            except FloodWait as e:
                wait = getattr(e, 'value', getattr(e, 'x', 5))
                logger.warning(f"FloodWait on progress update: {wait}s")
                self._pending_updates.add(task_id)
                
            except MessageNotModified:
                task.last_update_time = now
                task.last_message_text = text
                
            except MessageIdInvalid:
                self._tasks.pop(task_id, None)
                
            except (ChatWriteForbidden, UserIsBlocked):
                self._tasks.pop(task_id, None)
                
            except Exception as e:
                logger.debug(f"Progress update error: {e}")
    
    def _format_initial_message(self, file_name: str, file_size: int) -> str:
        """Format initial progress message."""
        size_mb = file_size / (1024 * 1024)
        return (
            f"**Preparing Download**\n\n"
            f"`{file_name}`\n"
            f"Size: {size_mb:.1f} MB"
        )
    
    def _format_progress_message(self, task: TaskProgress) -> str:
        """Format progress message with bar, speed, ETA."""
        if task.total_bytes <= 0:
            return f"Processing `{task.file_name}`..."
        
        # Calculate percentage
        percent = min(100.0, (task.current_bytes / task.total_bytes) * 100)
        
        # Progress bar
        filled = int(percent / 100 * PROGRESS_BAR_LENGTH)
        bar = "=" * filled + "-" * (PROGRESS_BAR_LENGTH - filled)
        
        # Size formatting
        current_mb = task.current_bytes / (1024 * 1024)
        total_mb = task.total_bytes / (1024 * 1024)
        speed_mbs = task.speed / (1024 * 1024)
        
        # ETA formatting
        eta_str = self._format_time(task.eta) if task.eta > 0 else "calculating..."
        
        return (
            f"**Downloading**\n\n"
            f"`{task.file_name}`\n\n"
            f"[{bar}] {percent:.1f}%\n\n"
            f"{current_mb:.1f} / {total_mb:.1f} MB\n"
            f"Speed: {speed_mbs:.2f} MB/s\n"
            f"ETA: {eta_str}"
        )
    
    def _format_time(self, seconds: float) -> str:
        """Format seconds to human readable string."""
        if seconds <= 0:
            return "0s"
        elif seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds // 60)}m {int(seconds % 60)}s"
        else:
            return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"
    
    def get_active_count(self) -> int:
        """Get number of tracked tasks."""
        return len(self._tasks)
    
    def get_task_ids(self) -> list:
        """Get list of active task IDs."""
        return list(self._tasks.keys())
    
    def get_queue_size(self) -> int:
        """Get current queue size."""
        return self._queue.qsize()


# Global instance
_progress_manager: Optional[ProgressManager] = None


def init_progress_manager(client: Client) -> ProgressManager:
    """Initialize global progress manager."""
    global _progress_manager
    _progress_manager = ProgressManager(client)
    return _progress_manager


def get_progress_manager() -> Optional[ProgressManager]:
    """Get global progress manager."""
    return _progress_manager
