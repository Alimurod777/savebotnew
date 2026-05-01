"""
core/premium_relay.py — Premium relay upload system.

Upload strategy (two methods, automatic fallback):

  Usul A — Relay channel (asosiy):
    1. Premium session faylni relay kanaliga yuklaydi.
    2. Bot copy_message() bilan foydalanuvchiga ko'chiradi (qayta yuklash yo'q).
    3. Relay kanaldagi xabar o'chiriladi.
    Shart: relay_channel_id konfiguratsiyalangan va bot admin bo'lishi kerak.

  Usul B — Saved Messages (fallback):
    1. Premium session faylni o'z "Saved Messages"iga yuklaydi.
    2. file_id olinadi.
    3. Bot shu file_id bilan foydalanuvchiga yuboradi.
    4. Saved Messages dan o'chiriladi.
    Shart: faqat premium sessiya bo'lsa yetarli (relay kanal kerak emas).

Fallback tartibi:
  Usul A → muvaffaqiyatsiz → Usul B → muvaffaqiyatsiz → split fallback (False)

When to use premium relay vs. user worker:
  - User is NOT Telegram Premium AND file requires premium upload (>2GB) → relay
  - User IS Telegram Premium → acc session uploads directly (no relay needed)
  - No premium sessions → split fallback

Ban / FloodWait protection:
  - FloodWait  → session paused for (wait + buffer) seconds; next session tried.
  - Fatal error → session permanently removed from runtime pool + owner notified.
  - If all sessions paused → caller receives False → split fallback.

Session rotation:
  - Round-robin index incremented after each upload (success or flood).
  - Each session tracks its own "paused_until" timestamp.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    AuthKeyUnregistered,
    AuthKeyInvalid,
    AuthKeyDuplicated,
    SessionExpired,
    SessionRevoked,
    UserDeactivated,
    UserDeactivatedBan,
    ChatAdminRequired,
    ChatWriteForbidden,
    RPCError,
)

from config import API_ID, API_HASH, get_client_params, OWNER_ID

logger = logging.getLogger(__name__)

# Extra seconds added after FloodWait
_FLOOD_BUFFER = 2.0
# Hard cap: if FloodWait > this, skip session instead of waiting
_FLOOD_SKIP_THRESHOLD = 120

FATAL_ERRORS = (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    AuthKeyDuplicated,
    SessionExpired,
    SessionRevoked,
    UserDeactivated,
    UserDeactivatedBan,
)

RELAY_ERRORS = (
    ChatAdminRequired,
    ChatWriteForbidden,
)


# ── Session pool ──────────────────────────────────────────────────────────────

class PremiumSessionPool:
    """
    Thread-safe pool of premium session strings with round-robin rotation.

    Mutations (add/remove/flood/fatal) are protected by asyncio.Lock so that
    concurrent upload tasks don't race each other.
    """

    def __init__(self) -> None:
        self._sessions: List[str] = []       # session strings
        self._paused_until: Dict[int, float] = {}  # idx → monotonic timestamp
        self._fatal: set = set()              # permanently removed indices (for logging)
        self._index: int = 0                  # next candidate
        self._lock = asyncio.Lock()

    # ── Mutation ──────────────────────────────────────────────────────────────

    async def set_sessions(self, session_strings: List[str]) -> None:
        """Replace entire pool (called at startup and after /premium add/remove)."""
        async with self._lock:
            self._sessions = list(session_strings)
            self._paused_until = {}
            self._fatal = set()
            self._index = 0
        logger.info("PremiumPool: loaded %d session(s)", len(session_strings))

    async def add_session(self, session_string: str) -> None:
        """Add a single session at runtime (no restart needed)."""
        async with self._lock:
            if session_string not in self._sessions:
                self._sessions.append(session_string)
        logger.info("PremiumPool: session added (total=%d)", len(self._sessions))

    async def remove_session(self, one_based_index: int) -> bool:
        """Remove by 1-based index. Returns True if removed."""
        async with self._lock:
            idx = one_based_index - 1
            if idx < 0 or idx >= len(self._sessions):
                return False
            self._sessions.pop(idx)
            # Re-key paused_until (indices shifted)
            new_paused = {}
            for k, v in self._paused_until.items():
                if k < idx:
                    new_paused[k] = v
                elif k > idx:
                    new_paused[k - 1] = v
            self._paused_until = new_paused
            self._index = self._index % max(len(self._sessions), 1)
        return True

    async def mark_flood(self, idx: int, wait_seconds: float) -> None:
        """Pause a session for (wait + buffer) seconds."""
        async with self._lock:
            resume_at = time.monotonic() + wait_seconds + _FLOOD_BUFFER
            self._paused_until[idx] = resume_at
        logger.warning(
            "PremiumPool: session #%d paused %.0fs (FloodWait)",
            idx + 1, wait_seconds + _FLOOD_BUFFER,
        )

    async def mark_fatal(self, idx: int) -> None:
        """Permanently disable a session for this runtime (not removed from disk)."""
        async with self._lock:
            self._fatal.add(idx)
        logger.error("PremiumPool: session #%d FATAL — disabled for this session", idx + 1)

    # ── Selection ─────────────────────────────────────────────────────────────

    async def get_available(self) -> Optional[tuple[int, str]]:
        """
        Return (index, session_string) of next available session.
        Returns None if all sessions are paused or pool is empty.
        """
        async with self._lock:
            n = len(self._sessions)
            if n == 0:
                return None
            now = time.monotonic()
            for _ in range(n):
                idx = self._index % n
                self._index = (self._index + 1) % n
                if idx in self._fatal:
                    continue
                paused = self._paused_until.get(idx, 0.0)
                if paused > now:
                    continue
                return idx, self._sessions[idx]
            return None

    @property
    def count(self) -> int:
        return len(self._sessions)

    def available_count(self) -> int:
        now = time.monotonic()
        return sum(
            1 for i in range(len(self._sessions))
            if i not in self._fatal and self._paused_until.get(i, 0.0) <= now
        )

    def next_available_in(self) -> Optional[float]:
        """
        Return seconds until the soonest paused (non-fatal) session becomes free.
        Returns 0.0 if a session is already available.
        Returns None if all sessions are fatal (no recovery possible).
        """
        now = time.monotonic()
        soonest: Optional[float] = None
        for i in range(len(self._sessions)):
            if i in self._fatal:
                continue
            resume = self._paused_until.get(i, 0.0)
            wait = max(0.0, resume - now)
            if soonest is None or wait < soonest:
                soonest = wait
        return soonest


# ── Relay state ───────────────────────────────────────────────────────────────

@dataclass
class _RelayState:
    channel_id: int = 0
    enabled: bool = True
    # Set to True after startup check confirms bot is admin in channel
    bot_is_admin: bool = False
    # True when sessions exist but relay channel is missing/not-admin → Usul B active
    has_saved_messages_fallback: bool = False
    # Notification sent flag (don't spam owner)
    _notified_no_admin: bool = field(default=False, repr=False)


# ── Main relay uploader ───────────────────────────────────────────────────────

class RelayUploader:
    """
    Singleton: orchestrates premium relay uploads.

    Usage:
        await relay_uploader.init(session_strings, relay_channel_id, bot_client)
        success = await relay_uploader.upload_via_relay(bot_client, user_id, ...)
    """

    def __init__(self) -> None:
        self.pool = PremiumSessionPool()
        self._state = _RelayState()
        self._ready = False           # True after init() confirms everything OK

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def init(
        self,
        session_strings: List[str],
        relay_channel_id: int,
        bot_client: Client,
        notify_owner: bool = True,
    ) -> bool:
        """
        Load sessions and verify relay channel.
        Returns True if relay is ready to use (either Usul A or Usul B).
        Sends owner notification on any problem (non-fatal — bot keeps running).
        """
        self._state.channel_id = relay_channel_id
        self._state.has_saved_messages_fallback = False
        self._ready = False

        # 1. No sessions — neither method can work
        if not session_strings:
            logger.info("RelayUploader: no premium sessions configured — relay disabled")
            return False

        # 2. No relay channel → Usul B (Saved Messages) only
        if not relay_channel_id:
            await self.pool.set_sessions(session_strings)
            self._state.has_saved_messages_fallback = True
            self._ready = True
            msg = (
                "⚠️ Premium sessions mavjud, lekin relay kanal belgilanmagan.\n"
                "Saved Messages fallback (Usul B) yoqildi.\n"
                "Relay kanal qo'shish uchun: /premium relay <channel_id>"
            )
            logger.warning("RelayUploader: no relay channel — Saved Messages fallback active")
            if notify_owner:
                await self._notify_owner(bot_client, msg)
            return True

        # 3. Check bot is admin in relay channel
        try:
            member = await bot_client.get_chat_member(relay_channel_id, "me")
            can_post = getattr(member.privileges, "can_post_messages", False) or \
                       getattr(member, "status", None) and str(member.status) in ("administrator", "creator")
            if not can_post:
                raise PermissionError("bot is not admin with post rights")
            self._state.bot_is_admin = True
        except Exception as e:
            # Bot not admin → Usul B fallback
            await self.pool.set_sessions(session_strings)
            self._state.has_saved_messages_fallback = True
            self._ready = True
            msg = (
                f"⚠️ Bot relay kanalda admin emas — Saved Messages fallback (Usul B) yoqildi.\n"
                f"Kanal: `{relay_channel_id}`\nXato: {e}\n\n"
                f"Bot adminlikka qo'shilsa, /premium relay buyrug'i bilan qayta sozlang."
            )
            logger.warning(
                "RelayUploader: bot not admin in relay channel %d — fallback active: %s",
                relay_channel_id, e,
            )
            if notify_owner and not self._state._notified_no_admin:
                self._state._notified_no_admin = True
                await self._notify_owner(bot_client, msg)
            return True

        # 4. Full relay mode (Usul A) — both channel and sessions ready
        await self.pool.set_sessions(session_strings)
        self._ready = True
        self._state.enabled = True
        self._state.has_saved_messages_fallback = False
        logger.info(
            "RelayUploader: ready (Usul A) — %d session(s), relay_channel=%d",
            len(session_strings), relay_channel_id,
        )
        return True

    async def reload_sessions(self, session_strings: List[str]) -> None:
        """Hot-reload sessions without restarting (after /premium add/remove)."""
        await self.pool.set_sessions(session_strings)
        if not session_strings:
            self._ready = False
            self._state.has_saved_messages_fallback = False
        elif self._state.channel_id and self._state.bot_is_admin:
            # Full relay mode
            self._ready = True
            self._state.has_saved_messages_fallback = False
        else:
            # Sessions exist but no valid relay channel → Usul B
            self._ready = True
            self._state.has_saved_messages_fallback = True
        logger.info("RelayUploader: sessions reloaded (%d total)", len(session_strings))

    def set_enabled(self, enabled: bool) -> None:
        self._state.enabled = enabled
        logger.info("RelayUploader: %s", "enabled" if enabled else "disabled")

    @property
    def is_ready(self) -> bool:
        """True if at least one upload method (Usul A or Usul B) is available."""
        if not self._ready or not self._state.enabled:
            return False
        # Usul A: relay channel configured and bot is admin
        if self._state.channel_id != 0 and self._state.bot_is_admin:
            return True
        # Usul B: sessions exist even without relay channel
        return self._state.has_saved_messages_fallback

    # ── Upload ────────────────────────────────────────────────────────────────

    async def upload_via_relay(
        self,
        bot_client: Client,
        user_id: int,
        file_path: str,
        msg_type: str,
        send_kwargs: dict,
        video_meta: Optional[dict] = None,
        thumb_path: Optional[str] = None,
        progress_cb: Optional[Callable] = None,
    ) -> bool:
        """
        Upload file_path to user_id using available premium sessions.

        Tries Usul A (relay channel) first, then Usul B (Saved Messages) as fallback.
        Returns True on success, False if all methods failed (caller should split).
        """
        if not self.is_ready:
            return False

        relay_id = self._state.channel_id
        use_relay_channel = bool(relay_id and self._state.bot_is_admin)
        max_attempts = max(1, self.pool.count)

        # ── Usul A: relay channel ──────────────────────────────────────────
        if use_relay_channel:
            for attempt in range(max_attempts):
                slot = await self.pool.get_available()
                if slot is None:
                    logger.warning(
                        "RelayUploader (Usul A): no available sessions for user %d", user_id
                    )
                    break  # fall through to Usul B

                idx, session_string = slot

                try:
                    sent_id = await self._upload_to_relay(
                        session_string=session_string,
                        relay_channel_id=relay_id,
                        file_path=file_path,
                        msg_type=msg_type,
                        send_kwargs=send_kwargs,
                        video_meta=video_meta,
                        thumb_path=thumb_path,
                        progress_cb=progress_cb,
                    )

                    await bot_client.copy_message(
                        chat_id=user_id,
                        from_chat_id=relay_id,
                        message_id=sent_id,
                    )

                    try:
                        await bot_client.delete_messages(relay_id, [sent_id])
                    except Exception:
                        pass

                    logger.info(
                        "RelayUploader (Usul A): success user=%d session=#%d type=%s",
                        user_id, idx + 1, msg_type,
                    )
                    return True

                except FloodWait as e:
                    wait = e.value
                    if wait > _FLOOD_SKIP_THRESHOLD:
                        logger.warning(
                            "RelayUploader (Usul A): session #%d FloodWait %ds > threshold — skipping",
                            idx + 1, wait,
                        )
                        await self.pool.mark_flood(idx, _FLOOD_SKIP_THRESHOLD)
                    else:
                        await self.pool.mark_flood(idx, wait)
                    continue

                except FATAL_ERRORS as e:
                    logger.error(
                        "RelayUploader (Usul A): session #%d FATAL %s — disabling",
                        idx + 1, type(e).__name__,
                    )
                    await self.pool.mark_fatal(idx)
                    await self._notify_owner(
                        bot_client,
                        f"🚨 Premium sessiya #{idx + 1} ishdan chiqdi!\n"
                        f"Xato: `{type(e).__name__}`\n"
                        f"Sessiya vaqtincha o'chirildi. /premium remove {idx + 1} bilan olib tashlang.",
                    )
                    continue

                except RELAY_ERRORS as e:
                    logger.error(
                        "RelayUploader (Usul A): relay channel %d — bot not admin: %s",
                        relay_id, e,
                    )
                    # Bot admin emas — Usul B ga o'tamiz
                    self._state.bot_is_admin = False
                    self._state.has_saved_messages_fallback = True
                    if not self._state._notified_no_admin:
                        self._state._notified_no_admin = True
                        await self._notify_owner(
                            bot_client,
                            f"🚨 Relay kanalda bot admin emas! Saved Messages fallbackka o'tildi.\n"
                            f"Kanal: `{relay_id}`",
                        )
                    break  # fall through to Usul B

                except Exception as e:
                    logger.warning(
                        "RelayUploader (Usul A): error (session #%d, attempt %d): %s: %s",
                        idx + 1, attempt + 1, type(e).__name__, e,
                    )
                    continue

            logger.warning(
                "RelayUploader (Usul A): exhausted — trying Saved Messages fallback for user %d",
                user_id,
            )

        # ── Usul B: Saved Messages fallback ───────────────────────────────
        logger.info(
            "RelayUploader (Usul B): Saved Messages fallback user=%d type=%s",
            user_id, msg_type,
        )
        for attempt in range(max_attempts):
            slot = await self.pool.get_available()
            if slot is None:
                logger.warning(
                    "RelayUploader (Usul B): no available sessions for user %d", user_id
                )
                return False

            idx, session_string = slot

            try:
                file_id = await self._upload_via_saved_messages(
                    session_string=session_string,
                    file_path=file_path,
                    msg_type=msg_type,
                    send_kwargs=send_kwargs,
                    video_meta=video_meta,
                    thumb_path=thumb_path,
                    progress_cb=progress_cb,
                )

                if not file_id:
                    logger.warning(
                        "RelayUploader (Usul B): no file_id from session #%d — trying next",
                        idx + 1,
                    )
                    continue

                ok = await self._send_by_file_id(
                    bot_client=bot_client,
                    user_id=user_id,
                    msg_type=msg_type,
                    file_id=file_id,
                    send_kwargs=send_kwargs,
                )

                if ok:
                    logger.info(
                        "RelayUploader (Usul B): success user=%d session=#%d type=%s",
                        user_id, idx + 1, msg_type,
                    )
                    return True

                logger.warning(
                    "RelayUploader (Usul B): _send_by_file_id failed user=%d — trying next",
                    user_id,
                )

            except FloodWait as e:
                wait = e.value
                if wait > _FLOOD_SKIP_THRESHOLD:
                    await self.pool.mark_flood(idx, _FLOOD_SKIP_THRESHOLD)
                else:
                    await self.pool.mark_flood(idx, wait)
                continue

            except FATAL_ERRORS as e:
                logger.error(
                    "RelayUploader (Usul B): session #%d FATAL %s — disabling",
                    idx + 1, type(e).__name__,
                )
                await self.pool.mark_fatal(idx)
                await self._notify_owner(
                    bot_client,
                    f"🚨 Premium sessiya #{idx + 1} ishdan chiqdi (Saved Messages)!\n"
                    f"Xato: `{type(e).__name__}`\n"
                    f"/premium remove {idx + 1} bilan olib tashlang.",
                )
                continue

            except Exception as e:
                logger.warning(
                    "RelayUploader (Usul B): error (session #%d, attempt %d): %s: %s",
                    idx + 1, attempt + 1, type(e).__name__, e,
                )
                continue

        logger.error(
            "RelayUploader: all methods exhausted for user %d — split fallback", user_id
        )
        return False

    # ── Internal: upload to relay channel ────────────────────────────────────

    async def _upload_to_relay(
        self,
        session_string: str,
        relay_channel_id: int,
        file_path: str,
        msg_type: str,
        send_kwargs: dict,
        video_meta: Optional[dict] = None,
        thumb_path: Optional[str] = None,
        progress_cb: Optional[Callable] = None,
    ) -> int:
        """Upload file to relay channel. Returns sent message_id."""
        client_name = f"relay_{uuid.uuid4().hex[:8]}"
        fp = get_client_params()

        # Caption is not needed on relay — will be transferred via copy_message
        # (copy_message preserves caption from the relay message if we set it there,
        #  but we include it so the relay message has proper caption for copy)
        kw = dict(send_kwargs)

        # Remove reply kwargs — relay channel has no reply context
        kw.pop("reply_to_message_id", None)
        kw.pop("reply_parameters", None)
        kw.pop("reply_markup", None)   # inline keyboards not needed on relay

        if msg_type == "Video" and video_meta:
            kw.update(video_meta)
        if msg_type == "Video" and thumb_path and os.path.exists(thumb_path):
            kw["thumb"] = thumb_path
        if msg_type == "Video":
            kw["supports_streaming"] = True
        if progress_cb:
            kw["progress"] = progress_cb

        # VideoNote and Sticker don't support caption
        if msg_type in ("VideoNote", "Sticker"):
            kw.pop("caption", None)
            kw.pop("caption_entities", None)

        client = Client(
            client_name,
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True,
            no_updates=True,
            workers=1,
            sleep_threshold=60,
            max_concurrent_transmissions=1,
            device_model=fp["device_model"],
            system_version=fp["system_version"],
            app_version=fp["app_version"],
            lang_code=fp["lang_code"],
        )

        try:
            await asyncio.wait_for(client.start(), timeout=30)

            # Pre-resolve relay channel peer (NEVER use access_hash=0)
            try:
                await client.resolve_peer(relay_channel_id)
            except Exception:
                # resolve_peer may fail for fresh in_memory sessions;
                # get_chat triggers username/invite-link resolution
                try:
                    await client.get_chat(relay_channel_id)
                except Exception:
                    pass

            send_map = {
                "Photo":     client.send_photo,
                "Video":     client.send_video,
                "Audio":     client.send_audio,
                "Voice":     client.send_voice,
                "VideoNote": client.send_video_note,
                "Animation": client.send_animation,
                "Sticker":   client.send_sticker,
                "Document":  client.send_document,
            }
            send_fn = send_map.get(msg_type, client.send_document)
            sent = await asyncio.wait_for(
                send_fn(relay_channel_id, file_path, **kw),
                timeout=600,
            )
            return sent.id

        finally:
            try:
                if client.is_connected:
                    await asyncio.wait_for(client.stop(), timeout=5)
            except Exception:
                pass

    # ── Owner notification ────────────────────────────────────────────────────

    async def _upload_via_saved_messages(
        self,
        session_string: str,
        file_path: str,
        msg_type: str,
        send_kwargs: dict,
        video_meta: Optional[dict] = None,
        thumb_path: Optional[str] = None,
        progress_cb: Optional[Callable] = None,
    ) -> Optional[str]:
        """
        Upload file to premium session's Saved Messages ("me").
        Returns file_id string on success, None on failure.
        The file_id is then used by bot to send media without re-uploading.
        """
        client_name = f"saved_{uuid.uuid4().hex[:8]}"
        fp = get_client_params()

        kw = dict(send_kwargs)
        kw.pop("reply_to_message_id", None)
        kw.pop("reply_parameters", None)
        kw.pop("reply_markup", None)

        if msg_type in ("VideoNote", "Sticker"):
            kw.pop("caption", None)
            kw.pop("caption_entities", None)

        if msg_type == "Video" and video_meta:
            kw.update(video_meta)
        if msg_type == "Video" and thumb_path and os.path.exists(thumb_path):
            kw["thumb"] = thumb_path
        if msg_type == "Video":
            kw["supports_streaming"] = True
        if progress_cb:
            kw["progress"] = progress_cb

        client = Client(
            client_name,
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True,
            no_updates=True,
            workers=1,
            sleep_threshold=60,
            max_concurrent_transmissions=1,
            device_model=fp["device_model"],
            system_version=fp["system_version"],
            app_version=fp["app_version"],
            lang_code=fp["lang_code"],
        )

        sent_msg = None
        try:
            await asyncio.wait_for(client.start(), timeout=30)

            send_map = {
                "Photo":     client.send_photo,
                "Video":     client.send_video,
                "Audio":     client.send_audio,
                "Voice":     client.send_voice,
                "VideoNote": client.send_video_note,
                "Animation": client.send_animation,
                "Sticker":   client.send_sticker,
                "Document":  client.send_document,
            }
            send_fn = send_map.get(msg_type, client.send_document)

            sent_msg = await asyncio.wait_for(
                send_fn("me", file_path, **kw),
                timeout=600,
            )

            # Extract file_id based on msg_type
            attr_map = {
                "Photo":     lambda m: m.photo.file_id if m.photo else None,
                "Video":     lambda m: m.video.file_id if m.video else None,
                "Audio":     lambda m: m.audio.file_id if m.audio else None,
                "Voice":     lambda m: m.voice.file_id if m.voice else None,
                "VideoNote": lambda m: m.video_note.file_id if m.video_note else None,
                "Animation": lambda m: m.animation.file_id if m.animation else None,
                "Sticker":   lambda m: m.sticker.file_id if m.sticker else None,
                "Document":  lambda m: m.document.file_id if m.document else None,
            }
            extractor = attr_map.get(msg_type, lambda m: m.document.file_id if m.document else None)
            file_id = extractor(sent_msg)

            # Cleanup Saved Messages
            try:
                await client.delete_messages("me", [sent_msg.id])
            except Exception:
                pass

            return file_id

        except Exception as e:
            logger.warning(
                "RelayUploader._upload_via_saved_messages: error (type=%s): %s: %s",
                msg_type, type(e).__name__, e,
            )
            # Cleanup on failure
            if sent_msg:
                try:
                    await client.delete_messages("me", [sent_msg.id])
                except Exception:
                    pass
            return None

        finally:
            try:
                if client.is_connected:
                    await asyncio.wait_for(client.stop(), timeout=5)
            except Exception:
                pass

    async def _send_by_file_id(
        self,
        bot_client: Client,
        user_id: int,
        msg_type: str,
        file_id: str,
        send_kwargs: dict,
    ) -> bool:
        """
        Bot sends media to user using a cached file_id (no upload).
        Caption and caption_entities are taken from send_kwargs.
        Returns True on success.
        """
        kw: dict = {}

        if msg_type not in ("VideoNote", "Sticker"):
            if "caption" in send_kwargs:
                kw["caption"] = send_kwargs["caption"]
            if "caption_entities" in send_kwargs:
                kw["caption_entities"] = send_kwargs["caption_entities"]

        if msg_type == "Video":
            kw["supports_streaming"] = True

        send_map = {
            "Photo":     bot_client.send_photo,
            "Video":     bot_client.send_video,
            "Audio":     bot_client.send_audio,
            "Voice":     bot_client.send_voice,
            "VideoNote": bot_client.send_video_note,
            "Animation": bot_client.send_animation,
            "Sticker":   bot_client.send_sticker,
            "Document":  bot_client.send_document,
        }
        send_fn = send_map.get(msg_type, bot_client.send_document)

        try:
            await asyncio.wait_for(
                send_fn(user_id, file_id, **kw),
                timeout=60,
            )
            return True
        except Exception as e:
            logger.warning(
                "RelayUploader._send_by_file_id: failed (user=%d type=%s): %s: %s",
                user_id, msg_type, type(e).__name__, e,
            )
            return False

    async def _notify_owner(self, bot_client: Client, text: str) -> None:
        if not OWNER_ID:
            return
        try:
            await bot_client.send_message(OWNER_ID, text)
        except Exception as e:
            logger.warning("RelayUploader: could not notify owner: %s", e)

    # ── Status ────────────────────────────────────────────────────────────────

    def status_text(self) -> str:
        holat = "🟢 Yoqilgan" if self._state.enabled else "🔴 O'chirilgan"

        if self._state.channel_id and self._state.bot_is_admin:
            usul = "A (Relay kanal) 📡"
        elif self._state.has_saved_messages_fallback:
            usul = "B (Saved Messages) 💾"
        else:
            usul = "Mavjud emas ❌"

        lines = [
            "**Premium Relay holati**",
            f"• Sessiyalar: {self.pool.count} ta (mavjud: {self.pool.available_count()})",
            f"• Relay kanal: `{self._state.channel_id or 'belgilanmagan'}`",
            f"• Bot admin: {'✅' if self._state.bot_is_admin else '❌'}",
            f"• Saved Messages fallback: {'✅' if self._state.has_saved_messages_fallback else '❌'}",
            f"• Faol usul: {usul}",
            f"• Holat: {holat}",
            f"• Tayyor: {'✅' if self.is_ready else '❌'}",
        ]
        return "\n".join(lines)


# ── Module-level singleton ────────────────────────────────────────────────────

relay_uploader = RelayUploader()
