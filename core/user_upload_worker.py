"""
core/user_upload_worker.py — Per-user isolated MTProto upload worker.

Architecture:
  Each session gets its own:
    - asyncio.Queue
    - worker coroutine
    - Pyrogram Client instance (workers=1, no_updates=True)

  The worker sends messages DIRECTLY via the session to the bot chat
  (target_chat_id = bot's Telegram user ID as seen by the session).

  No relay through Saved Messages, no Bot API, no intermediate chat.

Flow:
  source_chat
    → download_media (user session, acc)
    → enqueue UploadTask
    → worker picks task
    → session.send_*(bot_chat_id, file_path, caption=..., entities=...)
    → media appears in user's bot chat

Bot chat preparation (runs once per new worker):
  1. Try unblock_user(bot) — silently skip if not blocked
  2. Send /start to bot — if this fails (e.g. bot permanently blocked by user),
     worker.start() raises WorkerBotBlockedError so caller can skip this session.
  3. Archive the bot chat (best-effort, non-fatal)
  /start is only sent once per worker lifetime (worker is reused between tasks).

Rate limiting (human-like pattern):
  Text   : min 0.7 s between sends
  Media  : min 1.3 s between sends

Batching:
  After every 8 messages → sleep 4 s (burst protection)

FloodWait:
  < 60s  → sleep and retry once
  ≥ 60s  → raise FloodWait so caller can try another session
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait, UserIsBlocked, PeerIdInvalid
from pyrogram.enums import ParseMode

from config import API_ID, API_HASH, get_client_params

logger = logging.getLogger(__name__)

# ── Timing constants ─────────────────────────────────────────────────────────
TEXT_MIN_GAP   = 0.3   # seconds between text sends
MEDIA_MIN_GAP  = 0.5   # seconds between media sends (reduced for speed)
BATCH_SIZE     = 15    # messages per batch before pause
BATCH_PAUSE    = 3.0   # seconds between batches
FLOOD_BUFFER   = 1.0   # extra seconds after FloodWait
FLOOD_LONG_SEC = 60    # FloodWait >= this → surface to caller
IDLE_TIMEOUT   = 600   # seconds of inactivity before worker is reaped (10 min)
REAP_INTERVAL  = 120   # seconds between reaper sweeps (2 min)


class WorkerBotBlockedError(Exception):
    """Raised by UserUploadWorker.start() when the session has the bot blocked
    and unblocking failed — caller should skip this session."""


# ── Task ─────────────────────────────────────────────────────────────────────
@dataclass
class UploadTask:
    """
    One send operation to be executed by the per-session worker.

    send_fn:  async callable(client) → None
              Must call client.send_*(target_chat_id, ...) inside.
    is_media: True → use MEDIA_MIN_GAP, False → TEXT_MIN_GAP
    future:   set by the worker on completion / exception
    """
    send_fn:  Callable[[Client], Any]
    is_media: bool = True
    future:   asyncio.Future = field(default=None)


# ── Per-session worker ────────────────────────────────────────────────────────
class UserUploadWorker:
    """
    Manages one Pyrogram Client + one worker loop for a single session.

    Lifecycle:
      worker = UserUploadWorker(session_string, user_id, bot_id)
      await worker.start()   # raises WorkerBotBlockedError if bot is blocked
      await worker.enqueue(task)
      await worker.stop()
    """

    def __init__(
        self,
        session_string: str,
        user_id: int,
        bot_id: int = None,
        bot_username: str = None,
    ):
        self._session_string = session_string
        self._user_id = user_id
        self._bot_id = bot_id
        self._bot_username = bot_username
        self._queue: asyncio.Queue[Optional[UploadTask]] = asyncio.Queue()
        self._client: Optional[Client] = None
        self._task:   Optional[asyncio.Task] = None
        self._running = False
        self._last_send_time = 0.0
        self._last_activity = time.monotonic()  # for idle reaper
        self._batch_count = 0

    # ── Public API ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Connect the MTProto client and launch the worker loop.
        Raises WorkerBotBlockedError if bot is blocked and can't be unblocked."""
        if self._running:
            return
        await self._connect()   # may raise WorkerBotBlockedError
        self._running = True
        self._task = asyncio.ensure_future(self._loop())
        logger.info("UserUploadWorker[%d] started", self._user_id)

    async def stop(self) -> None:
        """Drain the queue and stop the worker."""
        self._running = False
        await self._queue.put(None)          # sentinel
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        await self._disconnect()
        logger.info("UserUploadWorker[%d] stopped", self._user_id)

    async def enqueue(self, task: UploadTask) -> Any:
        """
        Put a task on the queue and wait for it to complete.
        Returns whatever send_fn returns.
        Raises the exception if send_fn fails (including FloodWait ≥ 60s).
        """
        self._last_activity = time.monotonic()
        task.future = asyncio.get_running_loop().create_future()
        await self._queue.put(task)
        return await task.future

    # ── Internals ─────────────────────────────────────────────────────────────

    async def _connect(self) -> None:
        fp = get_client_params(self._user_id)
        self._client = Client(
            f"user_worker_{self._user_id}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=self._session_string,
            in_memory=True,
            no_updates=True,
            workers=1,
            sleep_threshold=0,          # disable auto-sleep; we handle FloodWait manually
            max_concurrent_transmissions=1,
            device_model=fp["device_model"],
            system_version=fp["system_version"],
            app_version=fp["app_version"],
            lang_code=fp["lang_code"],
        )
        await self._client.start()

        # Pre-resolve bot peer (PEER_ID_INVALID prevention for fresh in_memory sessions)
        resolved = False
        if self._bot_username:
            try:
                uname = self._bot_username.lstrip("@")
                await self._client.get_chat(uname)
                resolved = True
                logger.debug(
                    "UserUploadWorker[%d] bot peer resolved via username @%s",
                    self._user_id, uname,
                )
            except Exception as e:
                logger.warning(
                    "UserUploadWorker[%d] username resolve failed (@%s): %s",
                    self._user_id, self._bot_username, e,
                )

        if not resolved and self._bot_id:
            try:
                # Use resolve_peer which properly looks up access_hash
                # from internal cache. NEVER use access_hash=0 directly.
                peer = await self._client.resolve_peer(self._bot_id)
                resolved = True
                logger.debug(
                    "UserUploadWorker[%d] bot peer resolved via resolve_peer id=%d",
                    self._user_id, self._bot_id,
                )
            except Exception as e:
                logger.warning(
                    "UserUploadWorker[%d] resolve_peer failed (id=%d): %s",
                    self._user_id, self._bot_id, e,
                )

        if not resolved:
            logger.error(
                "UserUploadWorker[%d] could not resolve bot peer — "
                "uploads will fail with PEER_ID_INVALID",
                self._user_id,
            )

        # Bot chat preparation — runs once per worker lifecycle.
        # Raises WorkerBotBlockedError if bot is blocked and cannot be unblocked.
        if self._bot_username:
            await self._prepare_bot_chat(self._bot_username)

    async def _prepare_bot_chat(self, bot_username: str) -> None:
        """
        Ensure the session has an active chat with the bot.

        Steps:
          1. Try unblock_user — silently skip if bot is not blocked.
          2. Send /start — if this fails because the bot is still blocked,
             raise WorkerBotBlockedError so caller skips this session.
          3. Archive the chat (best-effort).

        /start is sent only once per worker lifetime (worker is reused between tasks).
        """
        uname = bot_username.lstrip("@")

        # Step 1: try to unblock the bot
        try:
            await self._client.unblock_user(uname)
            logger.debug("UserUploadWorker[%d] unblocked bot @%s", self._user_id, uname)
        except Exception:
            pass  # not blocked, or unblock not needed

        # Step 2: send /start
        try:
            await self._client.send_message(uname, "/start")
            logger.debug("UserUploadWorker[%d] sent /start to @%s", self._user_id, uname)
        except (UserIsBlocked, PeerIdInvalid) as e:
            # Bot is still blocked — this session is unusable for uploads
            logger.warning(
                "UserUploadWorker[%d] bot @%s is blocked and cannot be unblocked: %s — "
                "skipping this session",
                self._user_id, uname, e,
            )
            raise WorkerBotBlockedError(
                f"Bot @{uname} is blocked in session for user {self._user_id}"
            ) from e
        except Exception as e:
            # Other errors (network, etc.) — log and continue, don't block worker
            logger.warning(
                "UserUploadWorker[%d] /start to @%s failed (non-fatal): %s",
                self._user_id, uname, e,
            )

        # Step 3: archive chat (best-effort)
        try:
            chat = await self._client.get_chat(uname)
            await self._client.archive_chats([chat.id])
        except Exception:
            pass

    async def _disconnect(self) -> None:
        if self._client:
            try:
                if self._client.is_connected:
                    await asyncio.wait_for(self._client.stop(), timeout=5)
            except Exception:
                pass
            self._client = None

    async def _loop(self) -> None:
        while True:
            task = await self._queue.get()
            if task is None:                    # sentinel → exit
                break
            await self._run_task(task)
            self._queue.task_done()

    async def _run_task(self, task: UploadTask) -> None:
        """Execute one upload task with rate-limiting and FloodWait handling."""
        # Adaptive rate limit
        gap = MEDIA_MIN_GAP if task.is_media else TEXT_MIN_GAP
        elapsed = time.monotonic() - self._last_send_time
        if elapsed < gap:
            await asyncio.sleep(gap - elapsed)

        # Batch pause
        self._batch_count += 1
        if self._batch_count >= BATCH_SIZE:
            self._batch_count = 0
            logger.debug("UserUploadWorker[%d] batch pause %.1fs", self._user_id, BATCH_PAUSE)
            await asyncio.sleep(BATCH_PAUSE)

        try:
            result = await task.send_fn(self._client)
            self._last_send_time = time.monotonic()
            self._last_activity = self._last_send_time
            if not task.future.done():
                task.future.set_result(result)

        except FloodWait as e:
            wait = getattr(e, "value", getattr(e, "x", 30)) + FLOOD_BUFFER
            if wait >= FLOOD_LONG_SEC:
                # Long FloodWait — surface to caller; caller should try another session
                logger.warning(
                    "UserUploadWorker[%d] FloodWait %ds (≥%ds) — surfacing to caller",
                    self._user_id, wait, FLOOD_LONG_SEC,
                )
                if not task.future.done():
                    task.future.set_exception(e)
                return
            logger.warning(
                "UserUploadWorker[%d] FloodWait %ds — sleeping then retry",
                self._user_id, wait,
            )
            await asyncio.sleep(wait)
            # Retry once after short flood wait
            try:
                result = await task.send_fn(self._client)
                self._last_send_time = time.monotonic()
                if not task.future.done():
                    task.future.set_result(result)
            except Exception as retry_err:
                if not task.future.done():
                    task.future.set_exception(retry_err)

        except Exception as e:
            logger.warning(
                "UserUploadWorker[%d] send error: %s: %s",
                self._user_id, type(e).__name__, e,
            )
            if not task.future.done():
                task.future.set_exception(e)


# ── Global registry ───────────────────────────────────────────────────────────
class UserWorkerRegistry:
    """
    Registry: session_key → UserUploadWorker.

    session_key = (user_id, session_string[:16]) so that each unique session
    gets its own worker even if multiple pool sessions serve the same user.

    Workers are kept alive between tasks (same session stays connected → faster).
    An idle reaper runs every REAP_INTERVAL seconds and stops workers that have
    been inactive for IDLE_TIMEOUT seconds, freeing MTProto connections.
    Call remove(user_id) when a user logs out.
    """

    def __init__(self):
        self._workers: Dict[tuple, UserUploadWorker] = {}
        self._lock = asyncio.Lock()
        self._reaper_task: Optional[asyncio.Task] = None

    def _ensure_reaper(self) -> None:
        """Start the idle reaper if it's not already running."""
        if self._reaper_task is None or self._reaper_task.done():
            self._reaper_task = asyncio.ensure_future(self._reap_idle_workers())

    async def _reap_idle_workers(self) -> None:
        """Background loop: stop workers idle for > IDLE_TIMEOUT seconds."""
        while True:
            try:
                await asyncio.sleep(REAP_INTERVAL)
                now = time.monotonic()
                to_reap: list = []
                async with self._lock:
                    for key, worker in list(self._workers.items()):
                        idle = now - worker._last_activity
                        if idle >= IDLE_TIMEOUT and worker._queue.empty():
                            to_reap.append((key, worker))
                    for key, _ in to_reap:
                        del self._workers[key]
                for key, worker in to_reap:
                    logger.info(
                        "UserWorkerRegistry: reaping idle worker user=%d (idle %.0fs)",
                        key[0], now - worker._last_activity,
                    )
                    try:
                        await worker.stop()
                    except Exception as e:
                        logger.debug("Reaper: stop error for user=%d: %s", key[0], e)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug("Reaper loop error: %s", e)

    async def get_or_create(
        self,
        user_id: int,
        session_string: str,
        bot_id: int = None,
        bot_username: str = None,
    ) -> UserUploadWorker:
        self._ensure_reaper()
        key = (user_id, session_string[:16])
        async with self._lock:
            worker = self._workers.get(key)
            if worker is None or not worker._running:
                worker = UserUploadWorker(
                    session_string, user_id,
                    bot_id=bot_id,
                    bot_username=bot_username,
                )
                await worker.start()   # may raise WorkerBotBlockedError
                self._workers[key] = worker
            else:
                worker._last_activity = time.monotonic()
            return worker

    async def remove(self, user_id: int) -> None:
        """Remove all workers for a user (e.g. on logout)."""
        async with self._lock:
            keys = [k for k in self._workers if k[0] == user_id]
            workers = [self._workers.pop(k) for k in keys]
        for w in workers:
            await w.stop()

    async def shutdown_all(self) -> None:
        if self._reaper_task and not self._reaper_task.done():
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except asyncio.CancelledError:
                pass
        async with self._lock:
            workers = list(self._workers.values())
            self._workers.clear()
        for w in workers:
            await w.stop()


# Module-level singleton
worker_registry = UserWorkerRegistry()


# ── Convenience: build send_fn for each media type ───────────────────────────

def make_send_fn(
    target_chat_id: int,
    msg_type: str,
    file_path: str,
    send_kwargs: dict,
    video_meta: Optional[dict] = None,
    thumb_path: Optional[str] = None,
    progress_cb=None,
    doc_file_name: Optional[str] = None,
) -> Callable[[Client], Any]:
    """
    Return an async callable (client) → sent_message
    that uploads file_path directly to target_chat_id
    using the MTProto session.

    No relay. No Saved Messages. Direct send.
    """

    async def _send(client: Client):
        kw = dict(send_kwargs)          # copy to avoid mutation

        if msg_type == "Photo":
            return await client.send_photo(target_chat_id, file_path, **kw)

        elif msg_type == "Video":
            if video_meta:
                kw.update(video_meta)
            if thumb_path:
                kw["thumb"] = thumb_path
            kw["supports_streaming"] = True
            if progress_cb:
                kw["progress"] = progress_cb
            return await client.send_video(target_chat_id, file_path, **kw)

        elif msg_type == "Audio":
            if progress_cb:
                kw["progress"] = progress_cb
            return await client.send_audio(target_chat_id, file_path, **kw)

        elif msg_type == "Voice":
            return await client.send_voice(target_chat_id, file_path, **kw)

        elif msg_type == "VideoNote":
            kw.pop("caption", None)
            kw.pop("caption_entities", None)
            return await client.send_video_note(target_chat_id, file_path, **kw)

        elif msg_type == "Animation":
            return await client.send_animation(target_chat_id, file_path, **kw)

        elif msg_type == "Sticker":
            kw.pop("caption", None)
            kw.pop("caption_entities", None)
            return await client.send_sticker(target_chat_id, file_path, **kw)

        else:   # Document or unknown
            if doc_file_name:
                kw["file_name"] = doc_file_name
            kw["force_document"] = True  # Prevent Telegram reclassification
            if thumb_path:
                kw["thumb"] = thumb_path
            if progress_cb:
                kw["progress"] = progress_cb
            return await client.send_document(target_chat_id, file_path, **kw)

    return _send
