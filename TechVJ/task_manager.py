"""
Task Manager - Refactored for Event Loop Safety

Key Changes from v1:
1. No global mutable locks that can cross event loops
2. Lock created per-operation, not stored in dataclass
3. No MongoDB dependencies for runtime state
4. Clean task tracking with proper cleanup
5. No scheduled background cleanups
"""

import asyncio
import os
import shutil
import uuid
import re
from typing import Dict, Set, Optional, List
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

# Base temp directory
TEMP_BASE_DIR = "downloads/temp"


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to remove problematic characters"""
    if not filename:
        return "unnamed"
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)
    sanitized = sanitized.strip('. ')
    if len(sanitized) > 200:
        name, ext = os.path.splitext(sanitized)
        sanitized = name[:200-len(ext)] + ext
    return sanitized or "unnamed"


@dataclass
class UserTaskContext:
    """
    Context for managing a user's tasks and resources.
    
    NOTE: No asyncio.Lock stored here - locks must be created
    in the event loop where they're used to avoid
    "Future attached to a different loop" errors.
    """
    tasks: Set[asyncio.Task] = field(default_factory=set)
    cancel_flag: bool = False
    temp_dir: Optional[str] = None
    # Track processed albums in-memory (no MongoDB)
    processed_albums: Set[str] = field(default_factory=set)
    
    def should_cancel(self) -> bool:
        """Check if operations should be cancelled"""
        return self.cancel_flag
    
    def mark_album_processed(self, media_group_id: str) -> None:
        """Mark an album as processed (in-memory only)"""
        self.processed_albums.add(media_group_id)
    
    def is_album_processed(self, media_group_id: str) -> bool:
        """Check if album was already processed"""
        return media_group_id in self.processed_albums
    
    def clear_albums(self) -> None:
        """Clear album tracking (call on /stop or new operation)"""
        self.processed_albums.clear()


class TaskManager:
    """
    Manages asyncio tasks per user with cancellation support.
    Each user has isolated task management and temp directories.
    
    EVENT LOOP SAFETY:
    - Lock is created lazily in current event loop
    - No locks stored in dataclasses
    - All async operations acquire lock fresh
    """
    
    def __init__(self):
        self._user_contexts: Dict[int, UserTaskContext] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._bound_loop_id: Optional[int] = None
        os.makedirs(TEMP_BASE_DIR, exist_ok=True)
    
    def _get_lock(self) -> asyncio.Lock:
        """
        Get or create lock in the CURRENT event loop.
        This prevents "Future attached to a different loop" errors.
        Automatically resets if event loop changes.
        """
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
        except RuntimeError:
            # No running loop - return fresh lock (not stored)
            return asyncio.Lock()
        
        # Check if loop changed
        if self._bound_loop_id is not None and self._bound_loop_id != current_loop_id:
            logger.warning("TaskManager: Event loop changed, resetting lock")
            self._lock = None
        
        self._bound_loop_id = current_loop_id
        
        if self._lock is None:
            self._lock = asyncio.Lock()
        
        return self._lock
    
    async def get_context(self, user_id: int) -> UserTaskContext:
        """Get or create a task context for a user"""
        lock = self._get_lock()
        async with lock:
            if user_id not in self._user_contexts:
                self._user_contexts[user_id] = UserTaskContext()
            return self._user_contexts[user_id]
    
    async def register_task(self, user_id: int, task: asyncio.Task) -> None:
        """Register a task for a user"""
        context = await self.get_context(user_id)
        context.tasks.add(task)
        task.add_done_callback(lambda t: self._task_done_callback(user_id, t))
    
    def _task_done_callback(self, user_id: int, task: asyncio.Task) -> None:
        """Callback when a task completes"""
        if user_id in self._user_contexts:
            self._user_contexts[user_id].tasks.discard(task)
    
    async def create_task(self, user_id: int, coro, name: Optional[str] = None) -> asyncio.Task:
        """Create and register a task for a user"""
        task = asyncio.create_task(coro, name=name)
        await self.register_task(user_id, task)
        return task
    
    async def cancel_all_tasks(self, user_id: int) -> int:
        """
        Cancel ALL tasks for a user immediately.
        Returns the number of tasks cancelled.

        NOTE: No MongoDB calls here - album tracking is in-memory only.
        """
        context = await self.get_context(user_id)

        # Set the cancel flag FIRST - this signals all loops to stop
        context.cancel_flag = True

        cancelled_count = 0
        tasks_to_cancel = list(context.tasks)

        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
                cancelled_count += 1

        # Wait for tasks to actually finish (not just acknowledge cancellation)
        if tasks_to_cancel:
            try:
                await asyncio.wait(tasks_to_cancel, timeout=5.0)
            except Exception:
                pass

        # Clear the tasks set
        context.tasks.clear()

        # Clear in-memory album tracking
        context.clear_albums()

        # Now safe to cleanup — all download coroutines have finished
        # or timed out, so no file handles should be open
        await self.cleanup_user_temp(user_id)

        return cancelled_count
    
    async def reset_cancel_flag(self, user_id: int) -> None:
        """Reset the cancel flag for a user (call before starting new operations)"""
        context = await self.get_context(user_id)
        context.cancel_flag = False
    
    async def should_cancel(self, user_id: int) -> bool:
        """Check if operations for a user should be cancelled"""
        context = await self.get_context(user_id)
        return context.should_cancel()
    
    async def get_active_task_count(self, user_id: int) -> int:
        """Get the number of active tasks for a user"""
        context = await self.get_context(user_id)
        return len([t for t in context.tasks if not t.done()])
    
    def get_active_users(self) -> Set[int]:
        """
        Get set of user IDs with active tasks.
        Used by session cleanup to skip users with ongoing operations.
        """
        active = set()
        for user_id, context in self._user_contexts.items():
            if context.tasks and any(not t.done() for t in context.tasks):
                active.add(user_id)
            elif context.cancel_flag is False and context.temp_dir:
                # User has active temp dir but no tasks - might be mid-operation
                active.add(user_id)
        return active
    
    def has_active_tasks(self, user_id: int) -> bool:
        """Check if a user has active tasks (sync version for quick checks)"""
        if user_id not in self._user_contexts:
            return False
        context = self._user_contexts[user_id]
        return bool(context.tasks and any(not t.done() for t in context.tasks))
    
    # ==================== TEMP FOLDER MANAGEMENT ====================
    
    async def get_user_temp_dir(self, user_id: int) -> str:
        """
        Get or create a unique temporary directory for a user.
        Creates a new unique directory each time to prevent collisions.
        """
        context = await self.get_context(user_id)
        
        # Create a unique directory with user_id and UUID
        unique_id = str(uuid.uuid4())[:8]
        temp_dir = os.path.join(TEMP_BASE_DIR, f"user_{user_id}_{unique_id}")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Store reference for cleanup
        context.temp_dir = temp_dir
        
        return temp_dir
    
    async def cleanup_user_temp(self, user_id: int) -> None:
        """Clean up a user's temporary directory with retry for locked files"""
        context = await self.get_context(user_id)
        
        if context.temp_dir and os.path.exists(context.temp_dir):
            # Retry cleanup with delays (files may still be open)
            for attempt in range(3):
                try:
                    shutil.rmtree(context.temp_dir)
                    logger.info(f"Cleaned up temp dir for user {user_id}: {context.temp_dir}")
                    break
                except PermissionError as e:
                    # File still locked - wait and retry
                    if attempt < 2:
                        await asyncio.sleep(1.0)
                    else:
                        logger.debug(f"Temp dir cleanup deferred (files locked): {context.temp_dir}")
                except Exception as e:
                    logger.warning(f"Error cleaning up temp directory: {e}")
                    break
            context.temp_dir = None
        
        # Also cleanup any orphaned directories for this user
        try:
            for dirname in os.listdir(TEMP_BASE_DIR):
                if dirname.startswith(f"user_{user_id}_"):
                    full_path = os.path.join(TEMP_BASE_DIR, dirname)
                    if os.path.isdir(full_path):
                        try:
                            shutil.rmtree(full_path)
                        except PermissionError:
                            pass  # Skip locked directories
        except Exception as e:
            logger.debug(f"Error cleaning orphaned dirs: {e}")
    
    async def cleanup_user(self, user_id: int) -> None:
        """Clean up all resources for a user"""
        await self.cancel_all_tasks(user_id)
        await self.cleanup_user_temp(user_id)
        lock = self._get_lock()
        async with lock:
            if user_id in self._user_contexts:
                del self._user_contexts[user_id]
    
    # ==================== ALBUM TRACKING (IN-MEMORY) ====================
    
    async def mark_album_sent(self, user_id: int, media_group_id: str) -> None:
        """Mark album as processed for a user (in-memory only, no MongoDB)"""
        context = await self.get_context(user_id)
        context.mark_album_processed(media_group_id)
    
    async def is_album_sent(self, user_id: int, media_group_id: str) -> bool:
        """Check if album was already processed (in-memory only)"""
        context = await self.get_context(user_id)
        return context.is_album_processed(media_group_id)
    
    def get_processed_albums(self, user_id: int) -> Set[str]:
        """Get set of processed album IDs for passing to pipeline"""
        if user_id in self._user_contexts:
            return self._user_contexts[user_id].processed_albums
        return set()


class UserTempDirectory:
    """
    Context manager for per-user temporary directories.
    Guarantees cleanup in ALL cases: success, error, cancellation.
    """
    
    def __init__(self, user_id: int, manager: 'TaskManager'):
        self.user_id = user_id
        self.manager = manager
        self.temp_dir: Optional[str] = None
    
    async def __aenter__(self) -> str:
        """Create and return a temp directory"""
        self.temp_dir = await self.manager.get_user_temp_dir(self.user_id)
        return self.temp_dir
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Cleanup temp directory - called in ALL cases, with retry"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            for attempt in range(3):
                try:
                    shutil.rmtree(self.temp_dir)
                    break
                except PermissionError:
                    if attempt < 2:
                        await asyncio.sleep(0.5)
                except Exception as e:
                    logger.debug(f"Temp dir cleanup: {e}")
                    break
        return False  # Don't suppress exceptions


class StopSafePipeline:
    """
    Context manager for stop-safe async pipelines.
    Ensures proper task registration and cleanup.
    """
    
    def __init__(self, user_id: int, manager: 'TaskManager'):
        self.user_id = user_id
        self.manager = manager
        self.temp_dir: Optional[str] = None
    
    async def __aenter__(self) -> 'StopSafePipeline':
        """Initialize the pipeline"""
        await self.manager.reset_cancel_flag(self.user_id)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Cleanup on exit with retry for locked files"""
        # Cleanup temp directory
        if self.temp_dir and os.path.exists(self.temp_dir):
            for attempt in range(3):
                try:
                    shutil.rmtree(self.temp_dir)
                    break
                except PermissionError:
                    if attempt < 2:
                        await asyncio.sleep(0.5)
                except Exception:
                    break
        return False
    
    async def check_cancelled(self) -> bool:
        """Check if the operation should be cancelled"""
        return await self.manager.should_cancel(self.user_id)
    
    def raise_if_cancelled(self) -> None:
        """Raise CancelledError if operation should be cancelled"""
        if asyncio.current_task() and asyncio.current_task().cancelled():
            raise asyncio.CancelledError()
    
    async def get_temp_dir(self) -> str:
        """Get a temp directory for this pipeline"""
        if not self.temp_dir:
            self.temp_dir = await self.manager.get_user_temp_dir(self.user_id)
        return self.temp_dir


# Global task manager instance
task_manager = TaskManager()
