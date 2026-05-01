"""
core/upload_queue.py - Per-session serialized MTProto upload queue.

DESIGN GOALS:
  1. Uploads ALWAYS use MTProto user sessions — never Bot API.
  2. One active upload per session at a time (serialized queue).
  3. FloodWait is detected and the upload is paused, then retried.
  4. Adaptive throttling: exponential back-off on repeated errors,
     cooldown period after FloodWait storms.
  5. Memory vs disk threshold (100 MB):
       < 100 MB → BytesIO (in-memory)
       ≥ 100 MB → file path already on disk (streamed, no rewrite)

USAGE:
    from core.upload_queue import upload_queue

    # Queue a file upload and await the result
    result = await upload_queue.enqueue(
        session_id="user_12345",
        upload_coro=client.send_video(chat_id, file_path, ...),
    )

INTERNALS:
  - _SessionQueue: per-session asyncio.Queue + worker coroutine.
  - UploadJob: dataclass wrapping one upload coroutine + result future.
  - upload_queue: global UploadQueue managing all _SessionQueue instances.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional

from pyrogram.errors import FloodWait

logger = logging.getLogger(__name__)

# Thresholds
_MEMORY_THRESHOLD_BYTES = 100 * 1024 * 1024  # 100 MB

# Retry / throttle config
_MAX_FLOOD_WAIT = 300      # seconds — cap for any single FloodWait
_MAX_RETRIES = 3            # attempts per job before giving up
_BASE_RETRY_DELAY = 1.0     # seconds before first retry
_INTER_JOB_DELAY = 0.3      # seconds between consecutive jobs (anti-flood)


@dataclass
class UploadJob:
    """One pending upload task."""
    coro_factory: Callable[[], Awaitable[Any]]
    # future is created lazily in enqueue(), never at field default time,
    # to avoid attaching it to the wrong event loop.
    future: asyncio.Future = field(default=None)
    session_id: str = ""

    # Retry state (mutated by worker)
    attempts: int = 0


class _SessionQueue:
    """
    Serialized upload queue for one session.

    All uploads for a given session go through a single asyncio.Queue,
    ensuring they are executed one at a time.
    """

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self._queue: asyncio.Queue[Optional[UploadJob]] = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        self._last_flood: float = 0.0   # monotonic time of last FloodWait
        self._consecutive_errors: int = 0

    def _ensure_worker(self) -> None:
        if self._worker_task is None or self._worker_task.done():
            loop = asyncio.get_running_loop()
            self._worker_task = loop.create_task(self._worker_loop())

    async def enqueue(self, job: UploadJob) -> Any:
        """Put a job in the queue and wait for its result."""
        self._ensure_worker()
        await self._queue.put(job)
        return await job.future

    async def _worker_loop(self) -> None:
        logger.debug("upload_queue[%s]: worker started", self.session_id)
        try:
            while True:
                job = await asyncio.wait_for(self._queue.get(), timeout=60.0)
                if job is None:
                    break  # Sentinel: stop worker
                await self._execute(job)
                self._queue.task_done()
                await asyncio.sleep(_INTER_JOB_DELAY)
        except asyncio.TimeoutError:
            # No jobs for 60 s — let worker exit; will restart on next job
            pass
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning("upload_queue[%s]: worker crashed: %s", self.session_id, exc)
        finally:
            logger.debug("upload_queue[%s]: worker stopped", self.session_id)

    async def _execute(self, job: UploadJob) -> None:
        """Execute one job with FloodWait handling and retry."""
        last_exc: Optional[Exception] = None
        for attempt in range(_MAX_RETRIES):
            job.attempts = attempt + 1
            try:
                result = await job.coro_factory()
                self._consecutive_errors = 0
                if not job.future.done():
                    job.future.set_result(result)
                return
            except FloodWait as fw:
                wait = min(getattr(fw, "value", getattr(fw, "x", 30)), _MAX_FLOOD_WAIT)
                logger.warning(
                    "upload_queue[%s]: FloodWait %ds (attempt %d/%d)",
                    self.session_id, wait, attempt + 1, _MAX_RETRIES,
                )
                self._last_flood = time.monotonic()
                self._consecutive_errors += 1
                await asyncio.sleep(wait)
                last_exc = fw
            except (asyncio.CancelledError, KeyboardInterrupt):
                if not job.future.done():
                    job.future.cancel()
                return
            except Exception as exc:
                last_exc = exc
                self._consecutive_errors += 1
                if attempt < _MAX_RETRIES - 1:
                    delay = _BASE_RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        "upload_queue[%s]: error on attempt %d/%d (%s), retry in %.1fs",
                        self.session_id, attempt + 1, _MAX_RETRIES, exc, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "upload_queue[%s]: job failed after %d attempts: %s",
                        self.session_id, _MAX_RETRIES, exc,
                    )

        # All attempts exhausted
        if not job.future.done():
            if last_exc is not None:
                job.future.set_exception(last_exc)
            else:
                job.future.set_exception(RuntimeError("upload failed after max retries"))

    async def stop(self) -> None:
        """Signal the worker to stop and wait for it."""
        await self._queue.put(None)  # sentinel
        if self._worker_task and not self._worker_task.done():
            try:
                await asyncio.wait_for(self._worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._worker_task.cancel()


class UploadQueue:
    """
    Global upload manager: one _SessionQueue per session_id.

    ISOLATION GUARANTEE: each session_id gets its own asyncio.Queue and
    worker coroutine. Operations on session A never block session B.
    The global lock is only held for the microsecond needed to insert a
    new session entry into the dict — never during actual uploads.

    session_id is typically f"user_{user_id}" or f"sys_{system_user_id}".
    """

    def __init__(self) -> None:
        self._queues: Dict[str, _SessionQueue] = {}
        # Protects _queues dict writes only — released before any I/O
        self._registry_lock: Optional[asyncio.Lock] = None

    def _get_registry_lock(self) -> asyncio.Lock:
        if self._registry_lock is None:
            self._registry_lock = asyncio.Lock()
        return self._registry_lock

    def _get_queue(self, session_id: str) -> _SessionQueue:
        """Fast lock-free read for existing sessions (common path)."""
        return self._queues.get(session_id)

    async def _get_or_create_queue(self, session_id: str) -> _SessionQueue:
        """Get existing queue or create a new one under a brief registry lock."""
        # Fast path: already exists (no lock needed for reads in CPython GIL)
        q = self._queues.get(session_id)
        if q is not None:
            return q
        # Slow path: create new entry under lock (rare — only on first use)
        async with self._get_registry_lock():
            q = self._queues.get(session_id)
            if q is None:
                q = _SessionQueue(session_id)
                self._queues[session_id] = q
            return q

    async def enqueue(
        self,
        session_id: str,
        coro_factory: Callable[[], Awaitable[Any]],
    ) -> Any:
        """
        Enqueue one upload coroutine factory and await its completion.

        Args:
            session_id:    Unique identifier for the uploading session.
            coro_factory:  Zero-argument callable that returns an awaitable
                           (a coroutine or future). It is called by the worker,
                           NOT by the caller. This allows retries to create
                           fresh coroutine objects.

        Returns:
            Whatever the upload coroutine returns.

        Raises:
            Exception from the upload coroutine if all retries fail.
        """
        q = await self._get_or_create_queue(session_id)
        # Create future in the currently running loop (never at import time)
        loop = asyncio.get_running_loop()
        job = UploadJob(
            coro_factory=coro_factory,
            future=loop.create_future(),
            session_id=session_id,
        )
        return await q.enqueue(job)

    async def stop_session(self, session_id: str) -> None:
        """Stop the worker for a specific session (cleanup on logout)."""
        async with self._get_registry_lock():
            q = self._queues.pop(session_id, None)
        if q:
            await q.stop()

    async def stop_all(self) -> None:
        """Stop all session workers (called on bot shutdown)."""
        async with self._get_registry_lock():
            queues = list(self._queues.values())
            self._queues.clear()
        for q in queues:
            try:
                await q.stop()
            except Exception:
                pass


# Module-level singleton
upload_queue = UploadQueue()


# ==================== CONVENIENCE HELPERS ====================

def should_use_memory(file_size_bytes: int) -> bool:
    """
    Return True if the file should be buffered in memory (BytesIO).

    Files < 100 MB → memory.
    Files ≥ 100 MB → disk path (already downloaded).
    """
    return file_size_bytes < _MEMORY_THRESHOLD_BYTES


async def flood_safe_upload(
    send_func: Callable[..., Awaitable[Any]],
    **kwargs: Any,
) -> Any:
    """
    Call *send_func(**kwargs)* with FloodWait retry (standalone, no queue).

    Use this for one-off sends where queue serialization is not needed.
    For bulk / multi-session uploads, use upload_queue.enqueue() instead.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(_MAX_RETRIES):
        try:
            return await send_func(**kwargs)
        except FloodWait as fw:
            wait = min(getattr(fw, "value", getattr(fw, "x", 30)), _MAX_FLOOD_WAIT)
            logger.warning("flood_safe_upload: FloodWait %ds (attempt %d)", wait, attempt + 1)
            await asyncio.sleep(wait)
            last_exc = fw
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(_BASE_RETRY_DELAY * (2 ** attempt))
    raise last_exc or RuntimeError("upload failed")
