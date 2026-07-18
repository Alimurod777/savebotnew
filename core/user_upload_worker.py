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
import hashlib
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait, UserIsBlocked, PeerIdInvalid
from pyrogram.enums import ParseMode

from config import API_ID, API_HASH, get_client_params
from core.retry_utils import get_floodwait_seconds, is_floodwait_error

logger = logging.getLogger(__name__)


def _session_fingerprint(session_string: str) -> str:
    """Safe short identifier for logs; never log raw session strings."""
    return hashlib.sha256((session_string or "").encode("utf-8")).hexdigest()[:10]

# ── Timing constants (base values — adaptive throttle scales these) ───────────
TEXT_MIN_GAP   = 0.3   # seconds between text sends
MEDIA_MIN_GAP  = 0.5   # seconds between media sends (reduced for speed)
BATCH_SIZE     = 15    # messages per batch before pause
BATCH_PAUSE    = 3.0   # seconds between batches
FLOOD_BUFFER   = 1.0   # extra seconds after FloodWait
FLOOD_LONG_SEC = 60    # FloodWait >= this → surface to caller
IDLE_TIMEOUT   = 600   # seconds of inactivity before worker is reaped (10 min)
REAP_INTERVAL  = 120   # seconds between reaper sweeps (2 min)
JITTER_MAX     = 0.3   # max random jitter added to each send (prevents burst sync)
RECOVERY_BATCHES = 3   # consecutive clean batches to halve flood_scale
UPLOAD_PART_WORKERS = 2
UPLOAD_PART_FLOOD_RETRIES = 5
UPLOAD_PART_MAX_TOTAL_WAIT = 180

# Global upload concurrency cap — prevents N workers from uploading simultaneously
# Adaptive: starts at default, shrinks under flood pressure, grows back when clear
_DEFAULT_UPLOAD_CONCURRENCY = 4
_MIN_UPLOAD_CONCURRENCY = 2
_upload_semaphore: Optional[asyncio.Semaphore] = None
_upload_semaphore_size: int = _DEFAULT_UPLOAD_CONCURRENCY


def _get_upload_semaphore() -> asyncio.Semaphore:
    """Lazy-init global upload semaphore (must be called inside event loop)."""
    global _upload_semaphore
    if _upload_semaphore is None:
        _upload_semaphore = asyncio.Semaphore(_DEFAULT_UPLOAD_CONCURRENCY)
    return _upload_semaphore


def _resize_upload_semaphore(new_size: int) -> None:
    """Resize the global semaphore. Only shrinks capacity via tracking."""
    global _upload_semaphore_size
    _upload_semaphore_size = max(_MIN_UPLOAD_CONCURRENCY, new_size)


# ── Adaptive throttle (per-worker) ────────────────────────────────────────────

class _AdaptiveThrottle:
    """Per-worker adaptive rate limiter that responds to FloodWait signals.

    - Base gaps start at TEXT_MIN_GAP / MEDIA_MIN_GAP
    - After FloodWait: scale doubles (gaps widen, batch shrinks)
    - After RECOVERY_BATCHES clean batches: scale halves back toward 1.0
    - Scale is clamped to [1.0, 8.0]
    """

    __slots__ = ('_flood_scale', '_batches_clean', '_last_send', '_batch_count')

    def __init__(self) -> None:
        self._flood_scale: float = 1.0
        self._batches_clean: int = 0
        self._last_send: float = 0.0
        self._batch_count: int = 0

    @property
    def flood_pressure(self) -> float:
        """0.0 = no pressure, 1.0 = max pressure (scale=8.0)."""
        return min(1.0, (self._flood_scale - 1.0) / 7.0)

    def effective_gap(self, is_media: bool) -> float:
        base = MEDIA_MIN_GAP if is_media else TEXT_MIN_GAP
        return base * self._flood_scale

    def effective_batch_size(self) -> int:
        return max(4, int(BATCH_SIZE / self._flood_scale))

    def effective_batch_pause(self) -> float:
        return min(15.0, BATCH_PAUSE * self._flood_scale)

    async def wait_before_send(self, is_media: bool) -> None:
        """Adaptive inter-send delay + random jitter."""
        gap = self.effective_gap(is_media)
        elapsed = time.monotonic() - self._last_send
        if elapsed < gap:
            await asyncio.sleep(gap - elapsed)
        # Random jitter to desynchronize concurrent workers
        jitter = random.uniform(0, JITTER_MAX * self._flood_scale)
        if jitter > 0.01:
            await asyncio.sleep(jitter)

    async def check_batch_pause(self) -> None:
        """Pause at batch boundaries."""
        self._batch_count += 1
        bs = self.effective_batch_size()
        if self._batch_count >= bs:
            self._batch_count = 0
            self._batches_clean += 1
            pause = self.effective_batch_pause()
            logger.debug(
                "AdaptiveThrottle: batch pause %.1fs (scale=%.1f, clean=%d)",
                pause, self._flood_scale, self._batches_clean,
            )
            await asyncio.sleep(pause)
            # Recovery check
            if self._batches_clean >= RECOVERY_BATCHES and self._flood_scale > 1.0:
                self._flood_scale = max(1.0, self._flood_scale / 2.0)
                logger.info(
                    "AdaptiveThrottle: scale recovered to %.1f after %d clean batches",
                    self._flood_scale, self._batches_clean,
                )
                self._batches_clean = 0

    def record_send(self) -> None:
        self._last_send = time.monotonic()

    def record_flood(self, wait_seconds: float) -> None:
        """Escalate throttle after FloodWait."""
        self._flood_scale = min(8.0, self._flood_scale * 2.0)
        self._batches_clean = 0
        logger.warning(
            "AdaptiveThrottle: FloodWait %ds → scale escalated to %.1f",
            int(wait_seconds), self._flood_scale,
        )
        # Signal global concurrency reduction
        if self._flood_scale >= 4.0:
            _resize_upload_semaphore(_MIN_UPLOAD_CONCURRENCY)
        elif self._flood_scale >= 2.0:
            _resize_upload_semaphore(3)
        else:
            _resize_upload_semaphore(_DEFAULT_UPLOAD_CONCURRENCY)


class WorkerBotBlockedError(Exception):
    """Raised by UserUploadWorker.start() when the session has the bot blocked
    and unblocking failed — caller should skip this session."""


# ── Task ─────────────────────────────────────────────────────────────────────
@dataclass
class UploadTask:
    """
    One send operation to be executed by the per-session worker.

    send_fn:       async callable(client) → None
                   Must call client.send_*(target_chat_id, ...) inside.
    is_media:      True → use MEDIA_MIN_GAP, False → TEXT_MIN_GAP
    future:        set by the worker on completion / exception
    owner_user_id: The user who requested this upload — used for sender
                   isolation validation. Workers verify this matches before
                   executing to prevent cross-user contamination.
    request_id:    Request-scoped ID for structured logging correlation.
    """
    send_fn:       Callable[[Client], Any]
    is_media:      bool = True
    future:        asyncio.Future = field(default=None)
    owner_user_id: Optional[int] = None
    request_id:    Optional[str] = None


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
        self._session_fp = _session_fingerprint(session_string)
        self._user_id = user_id
        self._bot_id = bot_id
        self._bot_username = bot_username
        self._queue: asyncio.Queue[Optional[UploadTask]] = asyncio.Queue()
        self._client: Optional[Client] = None
        self._task:   Optional[asyncio.Task] = None
        self._session_user_id: Optional[int] = None
        self._running = False
        self._last_send_time = 0.0
        self._last_activity = time.monotonic()  # for idle reaper
        self._batch_count = 0
        self._bot_prepared = False
        self._busy = False
        self._throttle = _AdaptiveThrottle()

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

    @property
    def session_user_id(self) -> Optional[int]:
        return self._session_user_id

    @property
    def session_fingerprint(self) -> str:
        return self._session_fp

    @staticmethod
    def _message_text(message: Any) -> str:
        text = (
            getattr(message, "text", None)
            or getattr(message, "caption", None)
            or getattr(message, "message", None)
            or ""
        )
        return str(text).strip()

    @staticmethod
    def _message_timestamp(message: Any) -> Optional[float]:
        date_value = getattr(message, "date", None)
        if date_value is None:
            return None
        try:
            return float(date_value.timestamp())
        except Exception:
            return None

    async def resolve_bot_reply_message_id(
        self,
        reply_message: Any,
        *,
        history_limit: int = 50,
        max_age_seconds: int = 900,
    ) -> Optional[int]:
        """
        Resolve a bot-authored reply anchor as seen by this user session.

        Bot API message IDs and MTProto user-session message IDs can drift in
        bot private chats. Sending media with an unchecked reply_to_message_id
        can therefore reply to an unrelated message. We first verify the ID by
        text, then search recent bot-chat history for the exact source header.
        """
        if self._client is None:
            return None

        expected_id = getattr(reply_message, "id", None) or getattr(reply_message, "message_id", None)
        expected_text = self._message_text(reply_message)
        expected_ts = self._message_timestamp(reply_message)
        if not expected_text:
            logger.warning(
                "UserUploadWorker[%d] reply anchor has no text; refusing unchecked id-only reply bot_msg_id=%s session_fp=%s",
                self._user_id,
                expected_id,
                self._session_fp,
            )
            return None

        peer = self._bot_username.lstrip("@") if self._bot_username else self._bot_id
        if peer is None:
            return None

        def _matches(candidate: Any) -> bool:
            if getattr(candidate, "empty", False):
                return False
            if expected_text and self._message_text(candidate) != expected_text:
                return False
            if expected_ts is not None:
                candidate_ts = self._message_timestamp(candidate)
                if candidate_ts is not None and abs(candidate_ts - expected_ts) > max_age_seconds:
                    return False
            return True

        if expected_id is not None:
            try:
                candidate = await self._client.get_messages(peer, int(expected_id))
                if candidate and _matches(candidate):
                    logger.debug(
                        "UserUploadWorker[%d] reply anchor verified by id: bot_msg_id=%s session_msg_id=%s session_fp=%s",
                        self._user_id,
                        expected_id,
                        getattr(candidate, "id", None),
                        self._session_fp,
                    )
                    return int(getattr(candidate, "id"))
            except Exception as err:
                logger.debug(
                    "UserUploadWorker[%d] reply anchor id lookup failed: bot_msg_id=%s session_fp=%s error=%s",
                    self._user_id,
                    expected_id,
                    self._session_fp,
                    err,
                )

        if not expected_text:
            return None

        best = None
        best_score = None
        try:
            async for candidate in self._client.get_chat_history(peer, limit=history_limit):
                if not _matches(candidate):
                    continue
                candidate_ts = self._message_timestamp(candidate)
                time_score = abs(candidate_ts - expected_ts) if candidate_ts is not None and expected_ts is not None else 0
                id_score = abs(int(getattr(candidate, "id", 0)) - int(expected_id or 0)) if expected_id is not None else 0
                score = (time_score, id_score)
                if best is None or score < best_score:
                    best = candidate
                    best_score = score
            if best is not None:
                logger.info(
                    "UserUploadWorker[%d] reply anchor resolved: bot_msg_id=%s session_msg_id=%s session_fp=%s",
                    self._user_id,
                    expected_id,
                    getattr(best, "id", None),
                    self._session_fp,
                )
                return int(getattr(best, "id"))
        except Exception as err:
            logger.warning(
                "UserUploadWorker[%d] reply anchor history lookup failed: bot_msg_id=%s session_fp=%s error=%s",
                self._user_id,
                expected_id,
                self._session_fp,
                err,
            )

        logger.warning(
            "UserUploadWorker[%d] reply anchor not resolved: bot_msg_id=%s session_fp=%s",
            self._user_id,
            expected_id,
            self._session_fp,
        )
        return None

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
        # Pyrofork 2.3.69 swallows SaveBigFilePart errors in save_file().
        # Opt this client into the compatibility uploader: two parts run in
        # parallel until the first flood signal, then the file is serialized.
        self._client._techvj_flood_safe_upload = True
        self._client._techvj_upload_part_workers = UPLOAD_PART_WORKERS
        self._client._techvj_upload_part_flood_retries = UPLOAD_PART_FLOOD_RETRIES
        self._client._techvj_upload_part_max_total_wait = UPLOAD_PART_MAX_TOTAL_WAIT
        self._client._techvj_upload_part_long_wait = FLOOD_LONG_SEC
        self._client._techvj_upload_flood_callback = self._throttle.record_flood
        await self._client.start()
        try:
            me = await self._client.get_me()
            me_id = getattr(me, "id", None)
            if me_id is not None:
                self._session_user_id = int(me_id)
                logger.info(
                    "UserUploadWorker[%d] connected: session_fp=%s session_user_id=%s bot_id=%s",
                    self._user_id,
                    self._session_fp,
                    self._session_user_id,
                    self._bot_id,
                )
        except Exception as err:
            logger.warning(
                "UserUploadWorker[%d] get_me failed: session_fp=%s error=%s",
                self._user_id,
                self._session_fp,
                err,
            )

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

    async def _prepare_bot_chat(self, bot_username: str, force: bool = False) -> None:
        """
        Ensure the session has an active chat with the bot.

        Steps:
          1. Try unblock_user — silently skip if bot is not blocked.
          2. Send /start — if this fails because the bot is still blocked,
             raise WorkerBotBlockedError so caller skips this session.
          3. Archive the chat (best-effort).

        /start is sent only once per worker lifetime (worker is reused between tasks).
        """
        if self._bot_prepared and not force:
            return

        uname = bot_username.lstrip("@")
        blocked = False

        # Step 0: check dialog
        try:
            await self._client.get_chat(uname)
            self._bot_prepared = True
            return
        except UserIsBlocked:
            blocked = True
        except PeerIdInvalid:
            blocked = False
        except Exception as e:
            err_upper = str(e).upper()
            if "USER_IS_BLOCKED" in err_upper or "BOT WAS BLOCKED" in err_upper:
                blocked = True

        # Step 1: unblock only when blocked
        if blocked:
            try:
                await self._client.unblock_user(uname)
                logger.debug("UserUploadWorker[%d] unblocked bot @%s", self._user_id, uname)
            except Exception:
                pass

        # Step 2: send /start ONCE (only if dialog missing or after unblock)
        try:
            await self._client.send_message(uname, "/start")
            self._bot_prepared = True
            logger.debug("UserUploadWorker[%d] sent /start to @%s", self._user_id, uname)
        except (UserIsBlocked, PeerIdInvalid) as e:
            logger.warning(
                "UserUploadWorker[%d] bot @%s is blocked and cannot be unblocked: %s — "
                "skipping this session",
                self._user_id, uname, e,
            )
            raise WorkerBotBlockedError(
                f"Bot @{uname} is blocked in session for user {self._user_id}"
            ) from e
        except Exception as e:
            logger.warning(
                "UserUploadWorker[%d] /start to @%s failed (non-fatal): %s",
                self._user_id, uname, e,
            )
            return

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
                self._queue.task_done()
                break
            self._busy = True
            try:
                await self._run_task(task)
            except asyncio.CancelledError:
                if not task.future.done():
                    task.future.cancel()
                raise
            except Exception as exc:
                logger.exception(
                    "UserUploadWorker[%d] task loop failed unexpectedly",
                    self._user_id,
                )
                if not task.future.done():
                    task.future.set_exception(exc)
            finally:
                self._busy = False
                self._last_activity = time.monotonic()
                self._queue.task_done()

    async def _run_task(self, task: UploadTask) -> None:
        """Execute one upload task with adaptive rate-limiting and FloodWait handling."""
        # Sender isolation: verify task belongs to this worker's user
        if task.owner_user_id is not None and task.owner_user_id != self._user_id:
            logger.error(
                "UserUploadWorker[%d] SENDER ISOLATION VIOLATION: task owner=%d != worker user=%d — rejecting",
                self._user_id, task.owner_user_id, self._user_id,
            )
            if not task.future.done():
                task.future.set_exception(
                    RuntimeError(f"Sender isolation: task for user {task.owner_user_id} routed to worker for user {self._user_id}")
                )
            return

        # Adaptive rate limit (replaces static gap)
        await self._throttle.wait_before_send(task.is_media)

        # Adaptive batch pause
        await self._throttle.check_batch_pause()

        # Acquire global upload semaphore to prevent congestion
        sem = _get_upload_semaphore()
        try:
            # Non-blocking check: if semaphore would block and we're under
            # flood pressure, add extra stagger
            if sem.locked() and self._throttle.flood_pressure > 0.3:
                stagger = random.uniform(0.5, 2.0) * self._throttle.flood_pressure
                logger.debug(
                    "UserUploadWorker[%d] congestion stagger %.1fs (pressure=%.2f)",
                    self._user_id, stagger, self._throttle.flood_pressure,
                )
                await asyncio.sleep(stagger)

            async with sem:
                result = await task.send_fn(self._client)
            self._throttle.record_send()
            self._last_send_time = time.monotonic()
            self._last_activity = self._last_send_time
            if not task.future.done():
                task.future.set_result(result)

        except (UserIsBlocked, PeerIdInvalid) as e:
            recovered = False
            if self._bot_username:
                try:
                    await self._prepare_bot_chat(self._bot_username, force=True)
                    recovered = True
                except WorkerBotBlockedError:
                    recovered = False
                except Exception:
                    recovered = False
            if recovered:
                try:
                    result = await task.send_fn(self._client)
                    self._last_send_time = time.monotonic()
                    self._last_activity = self._last_send_time
                    if not task.future.done():
                        task.future.set_result(result)
                    return
                except Exception as retry_err:
                    if not task.future.done():
                        task.future.set_exception(retry_err)
                    return
            if not task.future.done():
                task.future.set_exception(e)
            return

        except Exception as e:
            if not is_floodwait_error(e):
                err_upper = str(e).upper()
                if "USER_IS_BLOCKED" in err_upper or "BOT WAS BLOCKED" in err_upper:
                    recovered = False
                    if self._bot_username:
                        try:
                            await self._prepare_bot_chat(self._bot_username, force=True)
                            recovered = True
                        except Exception:
                            recovered = False
                    if recovered:
                        try:
                            result = await task.send_fn(self._client)
                            self._last_send_time = time.monotonic()
                            self._last_activity = self._last_send_time
                            if not task.future.done():
                                task.future.set_result(result)
                            return
                        except Exception as retry_err:
                            if not task.future.done():
                                task.future.set_exception(retry_err)
                            return
                logger.warning(
                    "UserUploadWorker[%d] send error: session_fp=%s session_user_id=%s error=%s: %s",
                    self._user_id,
                    self._session_fp,
                    self._session_user_id,
                    type(e).__name__,
                    e,
                )
                if not task.future.done():
                    task.future.set_exception(e)
                return

            wait = get_floodwait_seconds(e) + FLOOD_BUFFER
            self._throttle.record_flood(wait)
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
                "UserUploadWorker[%d] FloodWait %ds — sleeping then retry (scale=%.1f)",
                self._user_id, wait, self._throttle._flood_scale,
            )
            await asyncio.sleep(wait)
            # Retry once after short flood wait (with adaptive stagger)
            await self._throttle.wait_before_send(task.is_media)
            try:
                async with _get_upload_semaphore():
                    result = await task.send_fn(self._client)
                self._throttle.record_send()
                self._last_send_time = time.monotonic()
                if not task.future.done():
                    task.future.set_result(result)
            except Exception as retry_err:
                if not task.future.done():
                    task.future.set_exception(retry_err)


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

    @staticmethod
    def _should_reap(worker: UserUploadWorker, now: float) -> bool:
        idle = now - worker._last_activity
        return idle >= IDLE_TIMEOUT and not worker._busy and worker._queue.empty()

    async def _reap_idle_workers(self) -> None:
        """Background loop: stop workers idle for > IDLE_TIMEOUT seconds."""
        while True:
            try:
                await asyncio.sleep(REAP_INTERVAL)
                now = time.monotonic()
                to_reap: list = []
                async with self._lock:
                    for key, worker in list(self._workers.items()):
                        if self._should_reap(worker, now):
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
        session_fp = _session_fingerprint(session_string)
        async with self._lock:
            worker = self._workers.get(key)
            if worker is None or not worker._running:
                logger.info(
                    "UserWorkerRegistry: creating worker request_user=%d session_fp=%s bot_id=%s",
                    user_id,
                    session_fp,
                    bot_id,
                )
                worker = UserUploadWorker(
                    session_string, user_id,
                    bot_id=bot_id,
                    bot_username=bot_username,
                )
                await worker.start()   # may raise WorkerBotBlockedError
                self._workers[key] = worker
            else:
                worker._last_activity = time.monotonic()
                logger.debug(
                    "UserWorkerRegistry: reusing worker request_user=%d session_fp=%s session_user_id=%s",
                    user_id,
                    session_fp,
                    worker.session_user_id,
                )
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
