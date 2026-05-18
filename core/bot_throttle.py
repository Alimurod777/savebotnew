"""
core/bot_throttle.py — Markaziy Bot API throttle, StatusTracker, BatchController.

FloodWait'dan proaktiv himoya:
  1. BotThrottle  — per-chat token bucket (2 req/s), FloodWait adaptive backoff
  2. StatusTracker — batch status edit'larni kamaytirish (har 2 postda, min 5s)
  3. BatchController — batch chunking + adaptive pauza + aktiv userlar tracking

Ishlatish:
    from core.bot_throttle import bot_throttle

    await bot_throttle.send_message(client, chat_id, text)
    await bot_throttle.edit_or_skip(client, chat_id, msg_id, text)

    status = StatusTracker(bot_throttle, client, chat_id, msg_id)
    batch = BatchController(user_id)
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, Dict, Set, List, Any, ClassVar

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# Token Bucket — per-chat rate limiter
# ═══════════════════════════════════════════════════════════════════

class _TokenBucket:
    """Leaky token bucket — refills at `rate` tokens/second, max `burst`."""

    __slots__ = ('rate', 'burst', '_tokens', '_last_refill')

    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self._tokens: float = float(burst)
        self._last_refill: float = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_refill = now

    async def acquire(self) -> None:
        """Wait until a token is available, then consume one."""
        while True:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            # How long until 1 token is available?
            deficit = 1.0 - self._tokens
            wait = deficit / self.rate
            await asyncio.sleep(wait)


# ═══════════════════════════════════════════════════════════════════
# BotThrottle — singleton, per-chat rate limiting + FloodWait tracking
# ═══════════════════════════════════════════════════════════════════

class ThrottleStats:
    """Monitoring counters."""
    __slots__ = ('total_calls', 'flood_waits', 'skipped_edits', 'throttled_waits')

    def __init__(self):
        self.total_calls: int = 0
        self.flood_waits: int = 0
        self.skipped_edits: int = 0
        self.throttled_waits: int = 0

    def __repr__(self) -> str:
        return (f"ThrottleStats(calls={self.total_calls}, floods={self.flood_waits}, "
                f"skipped={self.skipped_edits}, waits={self.throttled_waits})")


class BotThrottle:
    """
    Per-chat token bucket rate limiter for Bot API calls.

    - 2 req/s per chat (burst 3)
    - FloodWait → shu chat uchun flood_until belgilanadi
    - Boshqa chatlar ta'sirlanmaydi
    """

    PER_CHAT_RATE: float = 2.0      # tokens per second
    PER_CHAT_BURST: int = 3          # max burst
    BUCKET_IDLE_TTL: float = 120.0   # bucket 2 min faoliyatsizlikdan keyin tozalanadi
    FLOOD_BUFFER: float = 2.0        # FloodWait + buffer
    FLOOD_SLOWDOWN_DURATION: float = 30.0  # FloodWait keyin 30s davomida rate 1 req/s

    def __init__(self):
        self._buckets: Dict[int, _TokenBucket] = {}
        self._bucket_last_use: Dict[int, float] = {}
        self._flood_until: Dict[int, float] = {}
        self._flood_slowdown_until: Dict[int, float] = {}
        self._lock = asyncio.Lock()
        self.stats = ThrottleStats()
        self._cleanup_counter: int = 0
        # Global rate tracking — prevents exceeding Telegram's ~30 req/s bot limit
        self._global_sends: list = []           # timestamps of recent sends
        self._global_flood_until: float = 0.0   # brief all-chat pause after any flood
        self._GLOBAL_RATE_LIMIT: float = 25.0   # proactive slowdown threshold (req/s)
        self._GLOBAL_RATE_WINDOW: float = 1.0   # sliding window for rate calc
        self._GLOBAL_FLOOD_PAUSE: float = 2.0   # all-chat pause on any flood

    # ── acquire ──────────────────────────────────────────────────

    async def acquire(self, chat_id: int) -> None:
        """Wait until it's safe to make a Bot API call to this chat."""
        self.stats.total_calls += 1

        # Global flood pause: if any chat recently triggered FloodWait,
        # briefly delay ALL chats to prevent cascade amplification
        now = time.monotonic()
        if self._global_flood_until > now:
            gw = self._global_flood_until - now
            logger.debug("Throttle: global flood pause %.1fs", gw)
            self.stats.throttled_waits += 1
            await asyncio.sleep(gw)

        # Per-chat FloodWait waiting
        flood_end = self._flood_until.get(chat_id, 0.0)
        now = time.monotonic()
        if flood_end > now:
            wait = flood_end - now
            logger.debug("Throttle: chat %s flood-waiting %.1fs", chat_id, wait)
            self.stats.throttled_waits += 1
            await asyncio.sleep(wait)

        # Global rate check: proactive slowdown if approaching Telegram's limit
        await self._check_global_rate()

        # Per-chat token bucket
        bucket = await self._get_bucket(chat_id)
        await bucket.acquire()

        # Record this send for global rate tracking
        self._global_sends.append(time.monotonic())

        # Periodic cleanup (every 100 calls)
        self._cleanup_counter += 1
        if self._cleanup_counter >= 100:
            self._cleanup_counter = 0
            await self._cleanup_idle_buckets()
            self._trim_global_sends()

    async def _get_bucket(self, chat_id: int) -> _TokenBucket:
        """Get or create token bucket for chat."""
        now = time.monotonic()
        self._bucket_last_use[chat_id] = now

        if chat_id in self._buckets:
            # FloodWait slowdown — vaqtincha rate kamaytirish
            slowdown_end = self._flood_slowdown_until.get(chat_id, 0.0)
            if slowdown_end > now:
                bucket = self._buckets[chat_id]
                bucket.rate = 1.0  # sekinlashtirilgan rate
                return bucket
            else:
                bucket = self._buckets[chat_id]
                bucket.rate = self.PER_CHAT_RATE  # normal rate'ga qaytarish
                return bucket

        bucket = _TokenBucket(self.PER_CHAT_RATE, self.PER_CHAT_BURST)
        self._buckets[chat_id] = bucket
        return bucket

    async def _cleanup_idle_buckets(self) -> None:
        """Remove buckets idle for more than BUCKET_IDLE_TTL."""
        now = time.monotonic()
        expired = [
            cid for cid, last in self._bucket_last_use.items()
            if now - last > self.BUCKET_IDLE_TTL
        ]
        for cid in expired:
            self._buckets.pop(cid, None)
            self._bucket_last_use.pop(cid, None)
            self._flood_until.pop(cid, None)
            self._flood_slowdown_until.pop(cid, None)
        if expired:
            logger.debug("Throttle: cleaned %d idle buckets", len(expired))

    # ── FloodWait handling ───────────────────────────────────────

    def record_flood(self, chat_id: int, wait_seconds: float) -> None:
        """Record a FloodWait for this chat — block future calls until expiry."""
        now = time.monotonic()
        self._flood_until[chat_id] = now + wait_seconds + self.FLOOD_BUFFER
        self._flood_slowdown_until[chat_id] = now + wait_seconds + self.FLOOD_SLOWDOWN_DURATION
        self.stats.flood_waits += 1
        logger.warning("Throttle: chat %s FloodWait %ds recorded", chat_id, int(wait_seconds))
        # FloodWait propagation guard: briefly slow ALL chats to prevent
        # other concurrent requests from immediately hitting the same limit
        self._global_flood_until = max(
            self._global_flood_until,
            now + self._GLOBAL_FLOOD_PAUSE,
        )

    async def _check_global_rate(self) -> None:
        """Proactive slowdown if global send rate approaches Telegram's limit."""
        now = time.monotonic()
        cutoff = now - self._GLOBAL_RATE_WINDOW
        # Count sends in the last window
        recent = sum(1 for t in self._global_sends if t > cutoff)
        if recent >= self._GLOBAL_RATE_LIMIT:
            # We're close to Telegram's global limit — add delay
            delay = 0.5 + (recent - self._GLOBAL_RATE_LIMIT) * 0.1
            delay = min(delay, 3.0)
            logger.debug(
                "Throttle: global rate %.0f req/s >= %.0f — proactive delay %.1fs",
                recent / self._GLOBAL_RATE_WINDOW, self._GLOBAL_RATE_LIMIT, delay,
            )
            self.stats.throttled_waits += 1
            await asyncio.sleep(delay)

    def _trim_global_sends(self) -> None:
        """Remove old entries from global send timestamp list."""
        cutoff = time.monotonic() - 10.0  # keep 10s of history
        self._global_sends = [t for t in self._global_sends if t > cutoff]

    @property
    def active_floods(self) -> Dict[int, float]:
        """Return chat_id → seconds_remaining for active floods."""
        now = time.monotonic()
        return {
            cid: round(end - now, 1)
            for cid, end in self._flood_until.items()
            if end > now
        }

    # ── Wrapper methods ──────────────────────────────────────────

    async def send_message(self, client, chat_id: int, text: str, **kwargs) -> Optional[Any]:
        """Throttled send_message — waits for token, handles FloodWait."""
        await self.acquire(chat_id)
        try:
            return await client.send_message(chat_id, text, **kwargs)
        except Exception as e:
            fw_val = _extract_flood_wait(e)
            if fw_val is not None:
                self.record_flood(chat_id, fw_val)
                raise
            raise

    async def edit_message(self, client, chat_id: int, message_id: int,
                           text: str, **kwargs) -> Optional[Any]:
        """Throttled edit_message_text — waits for token, handles FloodWait."""
        await self.acquire(chat_id)
        try:
            return await client.edit_message_text(chat_id, message_id, text, **kwargs)
        except Exception as e:
            fw_val = _extract_flood_wait(e)
            if fw_val is not None:
                self.record_flood(chat_id, fw_val)
                raise
            raise

    async def edit_or_skip(self, client, chat_id: int, message_id: int,
                           text: str, **kwargs) -> Optional[Any]:
        """Edit message — FloodWait bo'lsa SKIP (None qaytaradi, kutmaydi)."""
        # FloodWait hali davom etayotgan bo'lsa — darhol skip
        flood_end = self._flood_until.get(chat_id, 0.0)
        if flood_end > time.monotonic():
            self.stats.skipped_edits += 1
            return None

        await self.acquire(chat_id)
        try:
            return await client.edit_message_text(chat_id, message_id, text, **kwargs)
        except Exception as e:
            fw_val = _extract_flood_wait(e)
            if fw_val is not None:
                self.record_flood(chat_id, fw_val)
                self.stats.skipped_edits += 1
                return None  # skip, don't raise
            # MESSAGE_NOT_MODIFIED — skip silently
            err_str = str(e).upper()
            if "MESSAGE_NOT_MODIFIED" in err_str:
                return None
            raise

    async def delete_messages(self, client, chat_id: int,
                              message_ids: list, **kwargs) -> None:
        """Throttled delete_messages."""
        await self.acquire(chat_id)
        try:
            await client.delete_messages(chat_id, message_ids, **kwargs)
        except Exception as e:
            fw_val = _extract_flood_wait(e)
            if fw_val is not None:
                self.record_flood(chat_id, fw_val)
                raise
            raise

    async def copy_message(self, client, chat_id: int, from_chat_id: int,
                           message_id: int, **kwargs) -> Optional[Any]:
        """Throttled copy_message."""
        await self.acquire(chat_id)
        try:
            return await client.copy_message(chat_id, from_chat_id, message_id, **kwargs)
        except Exception as e:
            fw_val = _extract_flood_wait(e)
            if fw_val is not None:
                self.record_flood(chat_id, fw_val)
                raise
            raise


# ═══════════════════════════════════════════════════════════════════
# StatusTracker — batch status edit optimallashtirish
# ═══════════════════════════════════════════════════════════════════

class StatusTracker:
    """
    Batch jarayoni uchun status xabarni boshqaradi.

    - Har 2 postda 1 marta edit (post_interval=2)
    - Kamida 5s orasida (min_interval=5.0)
    - Duplikat text yuborilmaydi
    - FloodWait bo'lsa skip qiladi

    Ishlatish:
        status = StatusTracker(throttle, client, chat_id, msg_id)
        await status.update("⏳ 5/100...")
        await status.finish("✅ Tugadi")
    """

    def __init__(self, throttle: BotThrottle, client, chat_id: int,
                 message_id: int, min_interval: float = 5.0,
                 post_interval: int = 2):
        self.throttle = throttle
        self.client = client
        self.chat_id = chat_id
        self.message_id = message_id
        self.min_interval = min_interval
        self.post_interval = post_interval
        self._last_edit: float = 0.0
        self._post_count: int = 0
        self._last_text: str = ""

    async def update(self, text: str, force: bool = False) -> Optional[Any]:
        """
        Status yangilash — qoidalar bo'yicha skip yoki edit.

        force=True: har doim yuboriladi (pauza xabari, muhim status)
        """
        self._post_count += 1

        if not force:
            # Post interval tekshiruvi
            if self._post_count % self.post_interval != 0:
                return None

            # Vaqt interval tekshiruvi
            now = time.monotonic()
            if now - self._last_edit < self.min_interval:
                return None

            # Duplikat tekshiruvi
            if text == self._last_text:
                return None

        result = await self.throttle.edit_or_skip(
            self.client, self.chat_id, self.message_id, text
        )

        if result is not None:
            self._last_edit = time.monotonic()
            self._last_text = text

        return result

    async def finish(self, text: str) -> Optional[Any]:
        """
        Oxirgi status — har doim yuboriladi (FloodWait bo'lsa 1 marta retry).

        "Tugadi", "Xato", "Bekor qilindi" xabarlari uchun.
        """
        try:
            result = await self.throttle.edit_message(
                self.client, self.chat_id, self.message_id, text
            )
            self._last_text = text
            return result
        except Exception as e:
            fw_val = _extract_flood_wait(e)
            if fw_val is not None:
                # FloodWait — 1 marta retry qisqa kutish bilan
                wait = min(fw_val, 10)
                await asyncio.sleep(wait)
                try:
                    return await self.client.edit_message_text(
                        self.chat_id, self.message_id, text
                    )
                except Exception:
                    pass
                return None
            # MESSAGE_NOT_MODIFIED — OK
            if "MESSAGE_NOT_MODIFIED" in str(e).upper():
                return None
            logger.debug("StatusTracker.finish error: %s", e)
            return None


# ═══════════════════════════════════════════════════════════════════
# BatchController — batch chunking + adaptive pauza
# ═══════════════════════════════════════════════════════════════════

class BatchController:
    """
    Batch processing uchun chunk pauza va adaptive backoff.

    - Aktiv userlar soniga qarab chunk/pause moslashadi
    - FloodWait bo'lganda chunk kichrayadi, pauza oshadi
    - 5 ta ketma-ket xato → batch to'xtatiladi

    Ishlatish:
        batch = BatchController(user_id)
        try:
            for idx, post_id in enumerate(post_ids):
                pause = batch.check_pause(idx)
                if pause > 0:
                    await asyncio.sleep(pause)
                ...
                batch.record_success()
                if batch.should_stop():
                    break
        finally:
            batch.finish()
    """

    # Class-level: aktiv userlarni kuzatish
    _active_users: ClassVar[Set[int]] = set()
    _active_lock: ClassVar[Optional[asyncio.Lock]] = None

    # Aktiv userlar soniga qarab default qiymatlari
    _PROFILES = {
        1: (50, 5.0),     # 1 user: chunk=50, pause=5s
        3: (30, 8.0),     # 2-3 user: chunk=30, pause=8s
        99: (25, 12.0),   # 4+ user: chunk=25, pause=12s
    }

    MAX_CONSECUTIVE_ERRORS: int = 5
    MIN_CHUNK_SIZE: int = 8
    MAX_PAUSE: float = 60.0
    RECOVERY_CHUNKS: int = 3  # bu qadar chunk FloodWait'siz → reset

    def __init__(self, user_id: int):
        self.user_id = user_id
        self._consecutive_errors: int = 0
        self._chunks_without_flood: int = 0
        self._flood_scale: float = 1.0  # 1.0 = normal, 2.0 = doubled pause

        # Aktiv userlar ro'yxatiga qo'shish
        self._active_users.add(user_id)

        # Profile bo'yicha default qiymatlarni olish
        base_chunk, base_pause = self._get_profile()
        self._base_chunk: int = base_chunk
        self._base_pause: float = base_pause
        self._current_chunk: int = base_chunk
        self._current_pause: float = base_pause

        logger.debug("BatchController: user=%s chunk=%d pause=%.1fs active_users=%d",
                      user_id, base_chunk, base_pause, len(self._active_users))

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._active_lock is None:
            cls._active_lock = asyncio.Lock()
        return cls._active_lock

    def _get_profile(self) -> tuple:
        """Aktiv userlar soniga qarab chunk/pause qaytaradi."""
        n = len(self._active_users)
        for threshold, values in sorted(self._PROFILES.items()):
            if n <= threshold:
                return values
        return (25, 12.0)  # fallback

    def _recalculate(self) -> None:
        """Aktiv userlar soni o'zgarganda chunk/pause ni qayta hisoblash."""
        base_chunk, base_pause = self._get_profile()
        self._base_chunk = base_chunk
        self._base_pause = base_pause
        # Flood scale saqlangan holda yangilash
        self._current_chunk = max(self.MIN_CHUNK_SIZE,
                                   int(base_chunk / self._flood_scale))
        self._current_pause = min(self.MAX_PAUSE,
                                   base_pause * self._flood_scale)

    def check_pause(self, post_index: int) -> float:
        """
        Chunk pauzasi kerakmi?

        Returns: 0 → pauza kerak emas, >0 → shu qadar soniya kutish kerak
        """
        if post_index == 0:
            return 0.0

        if post_index % self._current_chunk == 0:
            # Chunk chegarasi — pauza
            self._recalculate()  # aktiv userlar soni o'zgargan bo'lishi mumkin
            self._chunks_without_flood += 1

            # Recovery: 3 chunk FloodWait'siz → scale reset
            if self._chunks_without_flood >= self.RECOVERY_CHUNKS and self._flood_scale > 1.0:
                self._flood_scale = 1.0
                self._recalculate()
                logger.info("BatchController: user=%s flood scale reset to 1.0", self.user_id)

            return self._current_pause

        return 0.0

    def record_success(self) -> None:
        """Muvaffaqiyatli post — ketma-ket xato hisoblagichni nolga qaytarish."""
        self._consecutive_errors = 0

    def record_error(self) -> None:
        """Post muvaffaqiyatsiz — ketma-ket xato hisoblagichni oshirish."""
        self._consecutive_errors += 1

    def record_flood(self, wait_seconds: float) -> None:
        """FloodWait olinsa — adaptive backoff."""
        self._consecutive_errors += 1
        self._chunks_without_flood = 0

        # Scale oshirish: chunk yarmilanadi, pause ikki baravar
        self._flood_scale = min(4.0, self._flood_scale * 2.0)
        self._current_chunk = max(self.MIN_CHUNK_SIZE,
                                   int(self._base_chunk / self._flood_scale))
        self._current_pause = min(self.MAX_PAUSE,
                                   self._base_pause * self._flood_scale)

        logger.warning("BatchController: user=%s FloodWait %ds, scale=%.1f, "
                        "chunk=%d, pause=%.1fs",
                        self.user_id, int(wait_seconds), self._flood_scale,
                        self._current_chunk, self._current_pause)

    def should_stop(self) -> bool:
        """Ketma-ket xato limiti oshganmi?"""
        return self._consecutive_errors >= self.MAX_CONSECUTIVE_ERRORS

    def finish(self) -> None:
        """Batch tugadi — aktiv userlar ro'yxatidan chiqarish."""
        self._active_users.discard(self.user_id)
        logger.debug("BatchController: user=%s finished, active_users=%d",
                      self.user_id, len(self._active_users))


# ═══════════════════════════════════════════════════════════════════
# Progress filtering — milestone-based, 100MB+ faqat
# ═══════════════════════════════════════════════════════════════════

FILE_SIZE_PROGRESS_THRESHOLD: int = 100 * 1024 * 1024  # 100MB

PROGRESS_MILESTONES: List[int] = [10, 25, 50, 75, 100]


def should_show_progress(file_size: int) -> bool:
    """100MB dan kichik fayllar uchun progress ko'rsatilmaydi."""
    return file_size >= FILE_SIZE_PROGRESS_THRESHOLD


def should_update_progress(current_percent: float,
                           last_reported_percent: float) -> bool:
    """Faqat milestone'ga yetganda edit qiladi."""
    for milestone in PROGRESS_MILESTONES:
        if last_reported_percent < milestone <= current_percent:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════
# Utility
# ═══════════════════════════════════════════════════════════════════

def _extract_flood_wait(exc: Exception) -> Optional[float]:
    """
    FloodWait exception'dan wait soniyalarini olish.

    Pyrogram FloodWait → .value yoki .x
    Boshqa exception → None
    """
    # pyrogram.errors.FloodWait check
    cls_name = type(exc).__name__
    if cls_name == "FloodWait" or "FLOOD_WAIT" in str(exc).upper():
        val = getattr(exc, 'value', None)
        if val is not None:
            return float(val)
        val = getattr(exc, 'x', None)
        if val is not None:
            return float(val)
        # Fallback — parse from string
        import re
        m = re.search(r'(\d+)\s*seconds', str(exc))
        if m:
            return float(m.group(1))
        return 30.0  # conservative default
    return None


# ═══════════════════════════════════════════════════════════════════
# Singleton
# ═══════════════════════════════════════════════════════════════════

bot_throttle = BotThrottle()
