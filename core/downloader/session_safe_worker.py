"""
core/downloader/session_safe_worker.py - Session-Safe Adaptive Worker Pool

Worker pool designed to NEVER starve Pyrogram's PingTask or destabilize sessions.

Design Principles:
- Fewer stable workers > many aggressive workers
- Worker count adapts to disk latency and network RTT
- Global throttling on session stress signals
- Graceful degradation under pressure

FORBIDDEN:
- Worker oversubscription
- Session starvation
- Aggressive retry loops
- Assuming uninterrupted network
"""

import asyncio
import time
import logging
from typing import Optional, Dict, Set, Callable, Any, List
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

logger = logging.getLogger(__name__)


# Worker configuration
DEFAULT_WORKERS = 4        # Conservative default
MIN_WORKERS = 2            # Absolute minimum
MAX_WORKERS = 16           # Hard cap (never exceed)
SCALE_CHECK_INTERVAL = 5   # Seconds between scaling decisions

# Health thresholds
MAX_DISK_LATENCY_MS = 100       # Scale down if disk > 100ms
MAX_NETWORK_RTT_MS = 500        # Scale down if RTT > 500ms
HEALTHY_QUEUE_RATIO = 0.5       # Scale up if queue > 50% of workers
IDLE_QUEUE_RATIO = 0.2          # Scale down if queue < 20% of workers

# Session protection
MIN_IDLE_TIME_BETWEEN_TASKS = 0.05  # 50ms between tasks per worker
PING_TASK_PRIORITY_WINDOW = 2.0     # Yield every 2 seconds for PingTask


class WorkerState(Enum):
    """Worker lifecycle state."""
    IDLE = "idle"
    BUSY = "busy"
    DRAINING = "draining"
    STOPPED = "stopped"


class HealthStatus(Enum):
    """System health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"


@dataclass
class WorkerMetrics:
    """Metrics for a single worker."""
    worker_id: int
    tasks_completed: int = 0
    tasks_failed: int = 0
    total_bytes: int = 0
    last_task_time: float = 0.0
    avg_task_duration: float = 0.0
    state: WorkerState = WorkerState.IDLE


@dataclass
class PoolMetrics:
    """Aggregate pool metrics."""
    disk_latency_samples: deque = field(default_factory=lambda: deque(maxlen=20))
    rtt_samples: deque = field(default_factory=lambda: deque(maxlen=20))
    throughput_samples: deque = field(default_factory=lambda: deque(maxlen=20))
    
    def add_disk_latency(self, latency_ms: float) -> None:
        self.disk_latency_samples.append(latency_ms)
    
    def add_rtt(self, rtt_ms: float) -> None:
        self.rtt_samples.append(rtt_ms)
    
    def add_throughput(self, bytes_per_sec: float) -> None:
        self.throughput_samples.append(bytes_per_sec)
    
    @property
    def avg_disk_latency(self) -> float:
        if not self.disk_latency_samples:
            return 0.0
        return sum(self.disk_latency_samples) / len(self.disk_latency_samples)
    
    @property
    def avg_rtt(self) -> float:
        if not self.rtt_samples:
            return 0.0
        return sum(self.rtt_samples) / len(self.rtt_samples)
    
    @property
    def health_status(self) -> HealthStatus:
        disk = self.avg_disk_latency
        rtt = self.avg_rtt
        
        if disk > MAX_DISK_LATENCY_MS * 2 or rtt > MAX_NETWORK_RTT_MS * 2:
            return HealthStatus.CRITICAL
        elif disk > MAX_DISK_LATENCY_MS or rtt > MAX_NETWORK_RTT_MS:
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY


@dataclass
class DownloadJob:
    """Job submitted to worker pool."""
    job_id: str
    user_id: int
    handler: Callable
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    priority: int = 10
    created_at: float = field(default_factory=time.time)
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    result: Any = None
    error: Optional[str] = None
    
    def __lt__(self, other):
        return self.priority < other.priority


# Sentinel for shutdown
_SHUTDOWN = object()


class SessionSafeWorkerPool:
    """
    Adaptive worker pool that protects Telegram session stability.
    
    Key Features:
    - Adaptive scaling based on system health
    - Per-user task limits
    - Session-safe task spacing
    - Automatic degradation under stress
    
    CRITICAL: Never starves Pyrogram PingTask!
    
    Usage:
        pool = SessionSafeWorkerPool()
        await pool.start()
        
        result = await pool.submit(
            job_id="dl_123",
            user_id=12345,
            handler=download_func,
            args=(task,),
            kwargs={"progress": callback}
        )
        
        await pool.shutdown()
    """
    
    def __init__(
        self,
        initial_workers: int = DEFAULT_WORKERS,
        min_workers: int = MIN_WORKERS,
        max_workers: int = MAX_WORKERS,
        per_user_limit: int = 3
    ):
        self.initial_workers = max(min_workers, min(initial_workers, max_workers))
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.per_user_limit = per_user_limit
        
        self._running = False
        self._queue: asyncio.PriorityQueue = None
        self._workers: List[asyncio.Task] = []
        self._current_workers = 0
        
        # Tracking
        self._worker_metrics: Dict[int, WorkerMetrics] = {}
        self._pool_metrics = PoolMetrics()
        self._active_jobs: Dict[str, DownloadJob] = {}
        self._user_jobs: Dict[int, Set[str]] = {}
        self._cancel_flags: Dict[str, bool] = {}
        
        # Scaling
        self._scaler_task: Optional[asyncio.Task] = None
        self._last_scale_time = 0.0
        self._scale_cooldown = 10.0  # Seconds between scaling decisions
        
        self._lock = asyncio.Lock()
    
    async def start(self) -> None:
        """Start the worker pool."""
        if self._running:
            return
        
        self._queue = asyncio.PriorityQueue()
        self._running = True
        
        # Start initial workers
        for i in range(self.initial_workers):
            await self._spawn_worker(i)
        
        # Start adaptive scaler
        self._scaler_task = asyncio.create_task(self._adaptive_scaler())
        
        logger.info(
            f"SessionSafeWorkerPool started: {self._current_workers} workers "
            f"(range: {self.min_workers}-{self.max_workers})"
        )
    
    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown gracefully."""
        if not self._running:
            return
        
        logger.info("Shutting down SessionSafeWorkerPool...")
        self._running = False
        
        # Stop scaler
        if self._scaler_task:
            self._scaler_task.cancel()
            try:
                await self._scaler_task
            except asyncio.CancelledError:
                pass
        
        # Send shutdown signals
        for _ in range(self._current_workers):
            try:
                await self._queue.put((0, _SHUTDOWN))
            except Exception:
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
        logger.info("SessionSafeWorkerPool shutdown complete")
    
    async def submit(
        self,
        job_id: str,
        user_id: int,
        handler: Callable,
        args: tuple = (),
        kwargs: dict = None,
        priority: int = 10
    ) -> Any:
        """
        Submit a job and wait for result.
        
        Enforces per-user limits to prevent session flooding.
        """
        if not self._running:
            raise RuntimeError("Pool not running")
        
        # Check per-user limit
        while True:
            async with self._lock:
                user_active = len(self._user_jobs.get(user_id, set()))
                if user_active < self.per_user_limit:
                    # Register job
                    job = DownloadJob(
                        job_id=job_id,
                        user_id=user_id,
                        handler=handler,
                        args=args,
                        kwargs=kwargs or {},
                        priority=priority
                    )
                    self._active_jobs[job_id] = job
                    if user_id not in self._user_jobs:
                        self._user_jobs[user_id] = set()
                    self._user_jobs[user_id].add(job_id)
                    break
            
            # User at limit - wait
            logger.debug(f"User {user_id} at limit ({self.per_user_limit}), waiting...")
            await asyncio.sleep(0.5)
        
        # Enqueue
        await self._queue.put((priority, job))
        
        # Wait for completion
        await job.completion_event.wait()
        
        # Cleanup
        async with self._lock:
            self._active_jobs.pop(job_id, None)
            if user_id in self._user_jobs:
                self._user_jobs[user_id].discard(job_id)
        
        if job.error:
            raise Exception(job.error)
        return job.result
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a specific job."""
        self._cancel_flags[job_id] = True
        
        async with self._lock:
            if job_id in self._active_jobs:
                self._active_jobs[job_id].cancel_event.set()
                return True
        return False
    
    async def cancel_user_jobs(self, user_id: int) -> int:
        """Cancel all jobs for a user."""
        async with self._lock:
            job_ids = list(self._user_jobs.get(user_id, set()))
        
        cancelled = 0
        for job_id in job_ids:
            if await self.cancel_job(job_id):
                cancelled += 1
        
        return cancelled
    
    async def _spawn_worker(self, worker_id: int) -> None:
        """Spawn a new worker."""
        metrics = WorkerMetrics(worker_id=worker_id)
        self._worker_metrics[worker_id] = metrics
        
        worker = asyncio.create_task(
            self._worker_loop(worker_id, metrics),
            name=f"session_safe_worker_{worker_id}"
        )
        self._workers.append(worker)
        self._current_workers += 1
    
    async def _worker_loop(self, worker_id: int, metrics: WorkerMetrics) -> None:
        """Worker loop with session-safe behavior."""
        logger.debug(f"Worker {worker_id} started")
        metrics.state = WorkerState.IDLE
        
        last_yield_time = time.time()
        
        while self._running:
            try:
                # CRITICAL: Yield periodically to allow PingTask
                if time.time() - last_yield_time > PING_TASK_PRIORITY_WINDOW:
                    await asyncio.sleep(0)
                    last_yield_time = time.time()
                
                # Get job with timeout
                try:
                    _, job = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Check shutdown
                if job is _SHUTDOWN:
                    break
                
                # Check cancellation
                if self._cancel_flags.get(job.job_id):
                    job.error = "Cancelled"
                    job.completion_event.set()
                    continue
                
                # Process job
                metrics.state = WorkerState.BUSY
                start_time = time.time()
                
                try:
                    job.result = await job.handler(*job.args, **job.kwargs)
                    metrics.tasks_completed += 1
                except asyncio.CancelledError:
                    job.error = "Cancelled"
                except Exception as e:
                    job.error = str(e)
                    metrics.tasks_failed += 1
                    logger.warning(f"Worker {worker_id} job failed: {e}")
                finally:
                    metrics.last_task_time = time.time()
                    duration = metrics.last_task_time - start_time
                    
                    # Update rolling average
                    if metrics.avg_task_duration == 0:
                        metrics.avg_task_duration = duration
                    else:
                        metrics.avg_task_duration = (metrics.avg_task_duration * 0.8) + (duration * 0.2)
                    
                    job.completion_event.set()
                    metrics.state = WorkerState.IDLE
                    self._cancel_flags.pop(job.job_id, None)
                
                # Session-safe spacing between tasks
                await asyncio.sleep(MIN_IDLE_TIME_BETWEEN_TASKS)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(1)
        
        metrics.state = WorkerState.STOPPED
        logger.debug(f"Worker {worker_id} stopped")
    
    async def _adaptive_scaler(self) -> None:
        """Adaptive scaling based on system health."""
        while self._running:
            try:
                await asyncio.sleep(SCALE_CHECK_INTERVAL)
                
                if not self._running:
                    break
                
                # Check cooldown
                if time.time() - self._last_scale_time < self._scale_cooldown:
                    continue
                
                health = self._pool_metrics.health_status
                queue_size = self._queue.qsize()
                current = self._current_workers
                
                # Determine scaling action
                if health == HealthStatus.CRITICAL:
                    # Emergency scale down
                    if current > self.min_workers:
                        target = max(self.min_workers, current - 2)
                        logger.warning(f"CRITICAL health, scaling down: {current} -> {target}")
                        await self._scale_to(target)
                
                elif health == HealthStatus.DEGRADED:
                    # Moderate scale down
                    if current > self.min_workers + 1:
                        target = max(self.min_workers + 1, current - 1)
                        logger.info(f"Degraded health, scaling down: {current} -> {target}")
                        await self._scale_to(target)
                
                elif health == HealthStatus.HEALTHY:
                    # Consider scaling up if queue backing up
                    if queue_size > current * HEALTHY_QUEUE_RATIO:
                        if current < self.max_workers:
                            target = min(self.max_workers, current + 2)
                            logger.info(f"Scaling up for queue: {current} -> {target}")
                            await self._scale_to(target)
                    
                    # Consider scaling down if mostly idle
                    elif queue_size < current * IDLE_QUEUE_RATIO:
                        if current > self.min_workers + 2:
                            target = max(self.min_workers, current - 1)
                            logger.debug(f"Scaling down (idle): {current} -> {target}")
                            await self._scale_to(target)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scaler error: {e}")
    
    async def _scale_to(self, target: int) -> None:
        """Scale to target worker count."""
        target = max(self.min_workers, min(target, self.max_workers))
        
        if target == self._current_workers:
            return
        
        if target > self._current_workers:
            # Scale up
            for i in range(self._current_workers, target):
                await self._spawn_worker(i)
        else:
            # Scale down by sending shutdown signals
            for _ in range(self._current_workers - target):
                try:
                    await self._queue.put((0, _SHUTDOWN))
                except Exception:
                    break
            self._current_workers = target
        
        self._last_scale_time = time.time()
    
    def record_disk_latency(self, latency_ms: float) -> None:
        """Record disk I/O latency for health monitoring."""
        self._pool_metrics.add_disk_latency(latency_ms)
    
    def record_network_rtt(self, rtt_ms: float) -> None:
        """Record network RTT for health monitoring."""
        self._pool_metrics.add_rtt(rtt_ms)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        active_workers = sum(
            1 for m in self._worker_metrics.values()
            if m.state == WorkerState.BUSY
        )
        
        return {
            "workers_total": self._current_workers,
            "workers_active": active_workers,
            "workers_idle": self._current_workers - active_workers,
            "min_workers": self.min_workers,
            "max_workers": self.max_workers,
            "queue_size": self._queue.qsize() if self._queue else 0,
            "active_jobs": len(self._active_jobs),
            "health": self._pool_metrics.health_status.value,
            "avg_disk_latency_ms": f"{self._pool_metrics.avg_disk_latency:.1f}",
            "avg_rtt_ms": f"{self._pool_metrics.avg_rtt:.1f}",
            "tasks_completed": sum(m.tasks_completed for m in self._worker_metrics.values()),
            "tasks_failed": sum(m.tasks_failed for m in self._worker_metrics.values())
        }
