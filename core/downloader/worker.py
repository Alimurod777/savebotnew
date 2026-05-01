"""
core/downloader/worker.py - Adaptive worker pool for download tasks.

Features:
- Adaptive scaling: 32 default, up to 150 max workers
- Queue-based task distribution
- Clean cancellation
- Per-user task tracking
- Per-user concurrency limits
- Global throttling protection
- Graceful shutdown
"""

import asyncio
import time
import logging
from typing import Optional, Dict, Set, Callable, Any, List
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

# Adaptive worker configuration
# PATCH: reduced from 32/150 to 4/16 — 32 workers opening 32 simultaneous
# MTProto connections per bot triggers Telegram's global rate limiter.
# Real throughput bottleneck is network bandwidth, not worker count.
# 4 workers saturate a 100 Mbit link for 25 MB/s downloads.
DEFAULT_WORKER_COUNT = 4   # was 32
MAX_WORKER_COUNT = 16      # was 150
MIN_WORKER_COUNT = 2       # was 8
SCALE_UP_THRESHOLD = 0.8   # Scale up when queue is 80% of workers
SCALE_DOWN_THRESHOLD = 0.3 # Scale down when queue is 30% of workers
PER_USER_LIMIT = 2         # was 5 — max concurrent downloads per user
QUEUE_SIZE = 50000


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class DownloadTask:
    """A single download task."""
    task_id: str
    user_id: int
    message: Any  # Pyrogram Message
    client: Any  # Pyrogram Client
    progress_callback: Optional[Callable] = None
    
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[str] = None  # File path on success
    error: Optional[str] = None
    
    created_at: float = field(default_factory=time.time)
    started_at: float = 0.0
    completed_at: float = 0.0
    
    priority: int = 10  # Lower = higher priority
    retry_count: int = 0
    max_retries: int = 3
    
    # Real cancellation support
    cancel_event: asyncio.Event = field(default=None)
    asyncio_task: asyncio.Task = field(default=None)
    partial_file: str = field(default=None)
    
    def __post_init__(self):
        if self.cancel_event is None:
            self.cancel_event = asyncio.Event()


# Sentinel for shutdown
SHUTDOWN = object()


class WorkerPool:
    """
    Adaptive worker pool for parallel downloads.
    
    Features:
    - Starts with 32 workers (default)
    - Scales up to 150 workers under load
    - Per-user concurrency limits
    - Clean cancellation support
    
    Usage:
        pool = WorkerPool(download_handler=my_handler)
        await pool.start()
        
        task = DownloadTask(...)
        result = await pool.submit(task)
        
        await pool.shutdown()
    """
    
    def __init__(
        self,
        workers: int = DEFAULT_WORKER_COUNT,
        download_handler: Callable = None,
        min_workers: int = MIN_WORKER_COUNT,
        max_workers: int = MAX_WORKER_COUNT,
        adaptive: bool = True
    ):
        self.worker_count = max(min_workers, min(workers, max_workers))
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.adaptive = adaptive
        self._download_handler = download_handler
        
        self._queue: asyncio.Queue = None
        self._workers: List[asyncio.Task] = []
        self._current_workers = 0
        self._running = False
        
        # Task tracking
        self._pending_tasks: Dict[str, DownloadTask] = {}
        self._user_tasks: Dict[int, Set[str]] = {}
        self._cancel_flags: Dict[str, bool] = {}
        
        # REAL cancellation: track active asyncio tasks
        self._active_downloads: Dict[str, asyncio.Task] = {}
        self._active_tasks_by_user: Dict[int, Set[str]] = {}
        
        # Stats
        self._completed = 0
        self._failed = 0
        self._cancelled = 0
        
        self._lock = asyncio.Lock()
    
    async def start(self) -> None:
        """Start the worker pool with default workers (scales adaptively)."""
        if self._running:
            return
        
        self._queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        self._running = True
        
        # Start with default worker count
        await self._start_workers(self.worker_count)
        
        # Start adaptive scaling task if enabled
        if self.adaptive:
            self._scaler_task = asyncio.create_task(self._adaptive_scaler())
        
        logger.info(f"WorkerPool started with {self._current_workers} workers (adaptive={self.adaptive})")
    
    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the worker pool gracefully."""
        if not self._running:
            return
        
        logger.info("Shutting down WorkerPool...")
        self._running = False
        
        # Cancel scaler task
        if hasattr(self, '_scaler_task') and self._scaler_task:
            self._scaler_task.cancel()
            try:
                await self._scaler_task
            except asyncio.CancelledError:
                pass
        
        # Send shutdown signals
        for _ in range(self._current_workers):
            try:
                self._queue.put_nowait(SHUTDOWN)
            except asyncio.QueueFull:
                break
        
        # Wait for workers
        if self._workers:
            done, pending = await asyncio.wait(
                self._workers,
                timeout=timeout
            )
            for task in pending:
                task.cancel()
        
        self._workers.clear()
        self._current_workers = 0
        logger.info("WorkerPool shutdown complete")
    
    async def _adaptive_scaler(self) -> None:
        """Background task that scales workers based on queue load."""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                if not self._running:
                    break
                
                queue_size = self._queue.qsize()
                current = self._current_workers
                
                # Scale up if queue is backing up
                if queue_size > current * SCALE_UP_THRESHOLD:
                    new_count = min(current + 16, self.max_workers)
                    if new_count > current:
                        logger.info(f"Scaling up workers: {current} -> {new_count} (queue: {queue_size})")
                        await self._start_workers(new_count - current)
                
                # Scale down if queue is mostly empty (but keep minimum)
                elif queue_size < current * SCALE_DOWN_THRESHOLD and current > self.min_workers:
                    # Don't scale down aggressively - just note for gradual reduction
                    pass  # Workers will naturally exit on shutdown
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Scaler error: {e}")
    
    async def submit(self, task: DownloadTask) -> Optional[str]:
        """
        Submit a download task and wait for result.
        
        Enforces per-user concurrency limit to prevent session flooding.
        
        Returns:
            File path on success, None on failure
        """
        if not self._running:
            raise RuntimeError("WorkerPool not running")
        
        # CRITICAL: Enforce per-user concurrency limit
        # Wait if user has too many active tasks
        while True:
            async with self._lock:
                user_active = len(self._user_tasks.get(task.user_id, set()))
                user_running = len(self._active_tasks_by_user.get(task.user_id, set()))
                total_user_tasks = max(user_active, user_running)
                
                if total_user_tasks < PER_USER_LIMIT:
                    # Can proceed - add to tracking
                    self._pending_tasks[task.task_id] = task
                    if task.user_id not in self._user_tasks:
                        self._user_tasks[task.user_id] = set()
                    self._user_tasks[task.user_id].add(task.task_id)
                    break
            
            # User at limit - wait before checking again
            logger.debug(f"User {task.user_id} at concurrency limit ({PER_USER_LIMIT}), waiting...")
            await asyncio.sleep(1.0)
        
        # Create completion event
        completion_event = asyncio.Event()
        task._completion_event = completion_event
        
        # Enqueue
        try:
            await self._queue.put(task)
        except asyncio.QueueFull:
            task.status = TaskStatus.FAILED
            task.error = "Queue full"
            return None
        
        # Wait for completion
        await completion_event.wait()
        
        # Cleanup
        async with self._lock:
            self._pending_tasks.pop(task.task_id, None)
            if task.user_id in self._user_tasks:
                self._user_tasks[task.user_id].discard(task.task_id)
        
        return task.result if task.status == TaskStatus.COMPLETED else None
    
    def submit_nowait(self, task: DownloadTask) -> bool:
        """Submit task without waiting. Returns True if enqueued."""
        if not self._running:
            return False
        
        try:
            self._queue.put_nowait(task)
            return True
        except asyncio.QueueFull:
            return False
    
    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a specific task with REAL cancellation.
        
        This method:
        1. Sets cancel flag
        2. Sets cancel event on task
        3. Cancels the asyncio task
        4. Cleans up partial file
        """
        self._cancel_flags[task_id] = True
        cancelled = False
        
        async with self._lock:
            # Update task status
            if task_id in self._pending_tasks:
                task = self._pending_tasks[task_id]
                task.status = TaskStatus.CANCELLED
                task.cancel_event.set()
                cancelled = True
            
            # Cancel the actual asyncio task
            if task_id in self._active_downloads:
                asyncio_task = self._active_downloads[task_id]
                if asyncio_task and not asyncio_task.done():
                    asyncio_task.cancel()
                    logger.info(f"Cancelled active download: {task_id}")
                self._active_downloads.pop(task_id, None)
                cancelled = True
        
        if cancelled:
            self._cancelled += 1
        
        return cancelled
    
    async def cancel_user_tasks(self, user_id: int) -> int:
        """
        Cancel ALL tasks for a user with REAL cancellation.

        Cancels both pending and actively running downloads,
        then waits for active downloads to actually finish.
        """
        async with self._lock:
            task_ids = list(self._user_tasks.get(user_id, set()))
            active_ids = list(self._active_tasks_by_user.get(user_id, set()))

        # Merge all task IDs
        all_task_ids = set(task_ids) | set(active_ids)

        # Collect asyncio tasks before cancelling (cancel_task pops them)
        asyncio_tasks_to_wait = []
        async with self._lock:
            for task_id in all_task_ids:
                if task_id in self._active_downloads:
                    at = self._active_downloads[task_id]
                    if at and not at.done():
                        asyncio_tasks_to_wait.append(at)

        cancelled = 0
        for task_id in all_task_ids:
            if await self.cancel_task(task_id):
                cancelled += 1

        # Wait for cancelled downloads to actually stop
        if asyncio_tasks_to_wait:
            try:
                await asyncio.wait(asyncio_tasks_to_wait, timeout=5.0)
            except Exception:
                pass

        logger.info(f"Cancelled {cancelled} tasks for user {user_id}")
        return cancelled
    
    async def cancel_all_tasks(self) -> int:
        """Cancel ALL active downloads (for /stop all)."""
        async with self._lock:
            all_task_ids = list(self._active_downloads.keys())
        
        cancelled = 0
        for task_id in all_task_ids:
            if await self.cancel_task(task_id):
                cancelled += 1
        
        logger.info(f"Cancelled ALL {cancelled} active downloads")
        return cancelled
    
    def is_cancelled(self, task_id: str) -> bool:
        """Check if task is cancelled."""
        return self._cancel_flags.get(task_id, False)
    
    def register_active_download(self, task: DownloadTask, asyncio_task: asyncio.Task) -> None:
        """Register an active download task for cancellation tracking."""
        task.asyncio_task = asyncio_task
        self._active_downloads[task.task_id] = asyncio_task
        
        if task.user_id not in self._active_tasks_by_user:
            self._active_tasks_by_user[task.user_id] = set()
        self._active_tasks_by_user[task.user_id].add(task.task_id)
    
    def unregister_active_download(self, task: DownloadTask) -> None:
        """Unregister a completed/cancelled download."""
        self._active_downloads.pop(task.task_id, None)
        if task.user_id in self._active_tasks_by_user:
            self._active_tasks_by_user[task.user_id].discard(task.task_id)
    
    async def _worker_loop(self, worker_id: int) -> None:
        """Worker coroutine that processes tasks from queue."""
        logger.debug(f"Worker {worker_id} started")
        
        while self._running:
            try:
                # Get task with timeout
                try:
                    task = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Check shutdown
                if task is SHUTDOWN:
                    self._queue.task_done()
                    break
                
                # Check cancellation
                if self.is_cancelled(task.task_id):
                    task.status = TaskStatus.CANCELLED
                    self._queue.task_done()
                    if hasattr(task, '_completion_event'):
                        task._completion_event.set()
                    continue
                
                # Process task
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()
                
                try:
                    if self._download_handler:
                        # Wrap download in asyncio task for REAL cancellation
                        download_coro = self._download_handler(task)
                        download_task = asyncio.create_task(download_coro)
                        
                        # Register for cancellation tracking
                        self.register_active_download(task, download_task)
                        
                        try:
                            result = await download_task
                            task.result = result
                            task.status = TaskStatus.COMPLETED
                            self._completed += 1
                        finally:
                            # Always unregister
                            self.unregister_active_download(task)
                    else:
                        task.status = TaskStatus.FAILED
                        task.error = "No download handler"
                        self._failed += 1
                
                except asyncio.CancelledError:
                    task.status = TaskStatus.CANCELLED
                    self._cancelled += 1
                    logger.info(f"Worker {worker_id}: task {task.task_id} cancelled")
                    # Cleanup partial file
                    if task.partial_file:
                        try:
                            import os
                            if os.path.exists(task.partial_file):
                                os.remove(task.partial_file)
                                logger.debug(f"Cleaned up partial file: {task.partial_file}")
                        except:
                            pass
                    raise
                
                except Exception as e:
                    task.status = TaskStatus.FAILED
                    task.error = str(e)
                    self._failed += 1
                    logger.warning(f"Worker {worker_id} task failed: {e}")
                
                finally:
                    task.completed_at = time.time()
                    self._queue.task_done()
                    self._cancel_flags.pop(task.task_id, None)
                    
                    if hasattr(task, '_completion_event'):
                        task._completion_event.set()
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(1)
        
        logger.debug(f"Worker {worker_id} stopped")
    
    async def _start_workers(self, count: int) -> None:
        """Start exactly `count` workers (no scaling)."""
        for i in range(count):
            worker = asyncio.create_task(
                self._worker_loop(i),
                name=f"worker_{i}"
            )
            self._workers.append(worker)
        self._current_workers = count
        logger.debug(f"Started {count} workers")
    
    def get_stats(self) -> Dict:
        """Get pool statistics."""
        return {
            "workers": self._current_workers,
            "min_workers": self.min_workers,
            "max_workers": self.max_workers,
            "adaptive": self.adaptive,
            "queue_size": self._queue.qsize() if self._queue else 0,
            "pending_tasks": len(self._pending_tasks),
            "active_downloads": len(self._active_downloads),
            "completed": self._completed,
            "failed": self._failed,
            "cancelled": self._cancelled
        }
