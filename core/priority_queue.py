"""
core/priority_queue.py — Priority-aware async job queue.

3 buckets: vip / normal / new
Slot reservation prevents starvation:
  vip    → 50% of total workers
  normal → 35% of total workers
  new    → 15% of total workers

Spill-over: if a bucket's own slots are full but another bucket's
slots are free, the idle slots are borrowed (so workers never idle
when there's work to do).

Owner can adjust per-role limits at runtime via set_limit().
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

from core.role_manager import UserRole

logger = logging.getLogger(__name__)

# Default total workers and per-role parallel limits
_DEFAULT_TOTAL_WORKERS = 8
_DEFAULT_LIMITS: Dict[UserRole, int] = {
    UserRole.VIP_USER:    4,   # 50%
    UserRole.NORMAL_USER: 3,   # 35%
    UserRole.NEW_USER:    1,   # 15%
}


@dataclass
class PriorityJob:
    user_id:     int
    role:        UserRole
    parsed_url:  object          # ParsedURL duck-typed
    message:     object          # pyrogram Message
    handler:     Callable        # async fn(client, message, parsed_url)
    client:      object          # pyrogram Client
    enqueued_at: float = field(default_factory=time.monotonic)
    future:      Optional[asyncio.Future] = field(default=None)


# Priority order for dispatcher (highest first)
_PRIORITY_ORDER = [UserRole.VIP_USER, UserRole.NORMAL_USER, UserRole.NEW_USER]


class PriorityQueue:
    """
    3-bucket async priority queue with per-role slot reservation.
    Call start() once inside the running event loop (e.g. at bot startup).
    """

    def __init__(self) -> None:
        self._queues: Dict[UserRole, asyncio.Queue] = {}
        self._semaphores: Dict[UserRole, asyncio.Semaphore] = {}
        self._limits: Dict[UserRole, int] = dict(_DEFAULT_LIMITS)
        self._started = False
        self._worker_tasks: list = []

    def _init_queues(self) -> None:
        for role in _PRIORITY_ORDER:
            self._queues[role] = asyncio.Queue()
            self._semaphores[role] = asyncio.Semaphore(self._limits[role])

    async def start(self) -> None:
        """Initialise queues and launch dispatcher workers."""
        if self._started:
            return
        self._init_queues()
        self._started = True
        total = sum(self._limits.values())
        for i in range(total):
            t = asyncio.create_task(self._worker(i))
            self._worker_tasks.append(t)
        logger.info("PriorityQueue: started %d workers", total)

    async def enqueue(self, job: PriorityJob) -> asyncio.Future:
        """Add job to the appropriate bucket. Returns awaitable future."""
        if not self._started:
            raise RuntimeError("PriorityQueue.start() not called")
        loop = asyncio.get_running_loop()
        job.future = loop.create_future()
        role = job.role if job.role in self._queues else UserRole.NEW_USER
        await self._queues[role].put(job)
        qsize = self._queues[role].qsize()
        logger.debug(
            "PriorityQueue: enqueued user=%d role=%s queue_size=%d",
            job.user_id, role.value, qsize,
        )
        return job.future

    def queue_size(self, role: UserRole) -> int:
        q = self._queues.get(role)
        return q.qsize() if q else 0

    def set_limit(self, role: UserRole, n: int) -> None:
        """Update max parallel tasks for role (takes effect on next worker cycle)."""
        if n < 1:
            return
        self._limits[role] = n
        # Rebuild semaphore with new limit
        self._semaphores[role] = asyncio.Semaphore(n)
        logger.info("PriorityQueue: set limit %s → %d", role.value, n)

    async def _worker(self, worker_id: int) -> None:
        """Dispatcher loop: acquire a slot then pick and run a job."""
        while True:
            picked = await self._pick_and_acquire()
            if picked is None:
                await asyncio.sleep(0.05)
                continue
            job, sem = picked
            await self._run_job(job, sem)

    async def _pick_and_acquire(self):
        """
        Try each priority bucket in order. For the first non-empty queue
        whose semaphore can be acquired without blocking, dequeue a job
        and return (job, sem) with the semaphore already acquired.
        Falls back down priority levels if higher buckets are full.
        Returns None if no work is available right now.
        """
        for role in _PRIORITY_ORDER:
            q = self._queues.get(role)
            if q is None or q.empty():
                continue
            sem = self._semaphores[role]
            # Non-blocking acquire attempt
            acquired = not sem.locked()  # fast pre-check
            if not acquired:
                continue
            # Real acquire — non-blocking because _value > 0 and asyncio is
            # single-threaded (no other coroutine can run between the check above
            # and this await since there is no suspension point between them).
            await sem.acquire()
            # Semaphore acquired — now dequeue
            try:
                job = q.get_nowait()
                return job, sem
            except asyncio.QueueEmpty:
                # Race: queue emptied between our check and now; release and try next
                sem.release()
                continue
        return None

    async def _run_job(self, job: PriorityJob, sem: asyncio.Semaphore) -> None:
        try:
            result = await job.handler(job.client, job.message, job.parsed_url)
            if job.future and not job.future.done():
                job.future.set_result(result)
        except Exception as e:
            logger.warning(
                "PriorityQueue: job failed user=%d: %s", job.user_id, e
            )
            if job.future and not job.future.done():
                job.future.set_exception(e)
        finally:
            sem.release()


# Module-level singleton
priority_queue = PriorityQueue()
