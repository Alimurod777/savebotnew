"""
core/session_manager/session_manager.py — Main orchestrator (singleton).

Provides:
  - Priority-based session selection per user
  - Concurrency-safe upload with borrow/release
  - Rotation on FloodWait (up to max_attempts sessions tried)
  - Owner controls: disable/enable/tune sessions
  - Status reporting

Priority for user A:
  1. USER_OWNED  with owner_user_id == A
  2. DEDICATED   with owner_user_id == A
  3. BORROWABLE  (allow_borrow=True, not owner A)
  4. GLOBAL      (enabled, allow_system_use=True)
  5. None → caller uses split/acc fallback
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, List, Optional, Set, Tuple

from pyrogram import Client

from core.retry_utils import get_floodwait_seconds, is_floodwait_error
from core.copy_utils import (
    BotMessageResolutionError,
    get_bot_copy_source_chat_id,
    get_bot_latest_message_id,
    get_bot_real_message_id,
)

from .borrow_manager import borrow_manager
from .flood_controller import flood_controller
from .models import SessionRecord, SessionType
from .premium_worker import SessionWorkerBridge
from .session_registry import SessionRegistry

logger = logging.getLogger(__name__)


class SessionManager:
    """
    Singleton orchestrator.  Call ``await session_manager.init(...)`` once
    at startup before any upload calls.
    """

    def __init__(self) -> None:
        self._registry = SessionRegistry()
        self._bridge: Optional[SessionWorkerBridge] = None
        self._bot_id: Optional[int] = None
        self._bot_username: Optional[str] = None
        self._initialized = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def init(self, bot_id: int, bot_username: Optional[str] = None) -> None:
        """
        Load persisted sessions and set bot identity.
        Non-fatal: caller logs warning if this raises.
        """
        self._bot_id = bot_id
        self._bot_username = bot_username
        await self._registry.load()
        self._bridge = SessionWorkerBridge(bot_id=bot_id, bot_username=bot_username)
        self._initialized = True
        logger.info(
            "SessionManager: initialized with %d session(s)",
            len(self._registry.get_all()),
        )

    # ── Session selection (synchronous priority scan) ─────────────────────────

    def get_session_for_user(self, user_id: int) -> Optional[SessionRecord]:
        """
        Return the highest-priority available session for *user_id*, or None.
        Synchronous (safe in tight loops).
        """
        # 1. USER_OWNED
        rec = self._registry.get_user_owned(user_id)
        if rec and borrow_manager.is_session_available(rec):
            return rec

        # 2. DEDICATED
        rec = self._registry.get_dedicated(user_id)
        if rec and borrow_manager.is_session_available(rec):
            return rec

        # 3. BORROWABLE (any not owned by user_id)
        for rec in self._registry.get_borrowable(exclude_owner=user_id):
            if borrow_manager.is_session_available(rec):
                return rec

        # 4. GLOBAL
        for rec in self._registry.get_global():
            if borrow_manager.is_session_available(rec):
                return rec

        return None

    def get_system_session_for_use(self) -> Optional[SessionRecord]:
        """
        Return any available system-style session for owner/background tasks.

        Channel monitor jobs are not initiated by the target recipient, so they
        need a neutral uploader: GLOBAL first, then BORROWABLE sessions.
        """
        for rec in self._registry.get_global():
            if borrow_manager.is_session_available(rec):
                return rec

        for rec in self._registry.get_borrowable(exclude_owner=0):
            if borrow_manager.is_session_available(rec):
                return rec

        return None

    def _select_excluding(
        self, user_id: int, tried_ids: Set[str]
    ) -> Optional[SessionRecord]:
        """
        Same priority as get_session_for_user but skips sessions in *tried_ids*.
        """
        # 1. USER_OWNED
        rec = self._registry.get_user_owned(user_id)
        if rec and rec.session_id not in tried_ids and borrow_manager.is_session_available(rec):
            return rec

        # 2. DEDICATED
        rec = self._registry.get_dedicated(user_id)
        if rec and rec.session_id not in tried_ids and borrow_manager.is_session_available(rec):
            return rec

        # 3. BORROWABLE
        for rec in self._registry.get_borrowable(exclude_owner=user_id):
            if rec.session_id not in tried_ids and borrow_manager.is_session_available(rec):
                return rec

        # 4. GLOBAL
        for rec in self._registry.get_global():
            if rec.session_id not in tried_ids and borrow_manager.is_session_available(rec):
                return rec

        return None

    def _select_system_excluding(self, tried_ids: Set[str]) -> Optional[SessionRecord]:
        """System-session selector for background jobs, skipping tried IDs."""
        for rec in self._registry.get_global():
            if rec.session_id not in tried_ids and borrow_manager.is_session_available(rec):
                return rec

        for rec in self._registry.get_borrowable(exclude_owner=0):
            if rec.session_id not in tried_ids and borrow_manager.is_session_available(rec):
                return rec

        return None

    def _is_pool_session(self, record: SessionRecord, user_id: int) -> bool:
        """
        True when the media is uploaded to the bot's DM by a session not
        owned by the user — caller must copy_message to deliver to the user.
        """
        return (
            record.type in (SessionType.BORROWABLE, SessionType.GLOBAL)
            and record.owner_user_id != user_id
        )

    # ── Single-session upload ─────────────────────────────────────────────────

    async def upload_with_session(
        self,
        record: SessionRecord,
        user_id: int,
        target_chat_id: int,
        msg_type: str,
        file_path: str,
        send_kwargs: dict,
        bot_client: Client,
        video_meta: Optional[dict] = None,
        thumb_path: Optional[str] = None,
        progress_cb=None,
        bot_user_id: Optional[int] = None,
        doc_file_name: Optional[str] = None,
        force_copy: bool = False,
    ) -> Optional[Any]:
        """
        Attempt to upload via *record*.

        Returns the sent Message on success, None on failure (flood, blocked,
        full).  Caller should rotate to the next session on None.

        If the session is a "pool" session (BORROWABLE/GLOBAL not owned by
        user), the uploaded message is copy_message'd to the user after upload.

        For non-pool sessions (USER_OWNED/DEDICATED), the session uploads
        directly to the bot's user ID so the message lands in the user's
        private chat with the bot (NOT Saved Messages).
        """
        if not await borrow_manager.try_acquire(record, user_id):
            return None

        try:
            bridge = self._bridge
            if bridge is None:
                return None

            # CRITICAL: ALL sessions upload to bot_user_id (bot's Telegram user ID).
            # - Pool session → pool_user↔bot chat → copy_message to user
            # - Non-pool session → user↔bot chat (directly visible to user)
            # Using target_chat_id (= user_id) would send to Saved Messages!
            _bot_uid = bot_user_id or bot_client.me.id
            upload_chat_id = _bot_uid

            # For pool sessions, extract reply_to_message_id from send_kwargs
            # and pass it to copy_message instead — the pool session's bot chat
            # does NOT contain the user's original message, so reply_to_message_id
            # would be silently ignored by Telegram if included in the upload call.
            _upload_kwargs = dict(send_kwargs)
            _reply_to_id = _upload_kwargs.pop("reply_to_message_id", None)
            _upload_kwargs.pop("reply_parameters", None)

            send_fn = bridge.build_send_fn(
                target_chat_id=upload_chat_id,
                msg_type=msg_type,
                file_path=file_path,
                send_kwargs=_upload_kwargs,
                video_meta=video_meta,
                thumb_path=thumb_path,
                progress_cb=progress_cb,
                doc_file_name=doc_file_name,
            )

            worker = await bridge.get_worker(record, user_id)
            if worker is None:
                return None

            expected_source_chat_id = getattr(worker, "session_user_id", None)
            copy_source_chat_id = int(expected_source_chat_id) if expected_source_chat_id is not None else None
            pre_upload_latest_id = None
            if copy_source_chat_id is None:
                logger.error(
                    "SessionManager: uploader account unavailable for session %s",
                    record.session_id[:8],
                )
                return None

            pre_upload_latest_id = await get_bot_latest_message_id(bot_client, copy_source_chat_id)
            if pre_upload_latest_id is None:
                logger.error(
                    "SessionManager: could not read bot-side watermark for session %s chat=%s",
                    record.session_id[:8],
                    copy_source_chat_id,
                )
                return None

            sent_msg = await bridge.enqueue_task(
                worker,
                send_fn,
                is_media=True,
                owner_user_id=user_id,
            )

            # Copy from uploader-bot DM to user chat so reply context is applied.
            copied_msg = None
            if sent_msg is not None:
                detected_source_chat_id = get_bot_copy_source_chat_id(sent_msg, upload_chat_id)
                if copy_source_chat_id is None:
                    copy_source_chat_id = detected_source_chat_id
                elif detected_source_chat_id is not None and int(detected_source_chat_id) != int(copy_source_chat_id):
                    logger.error(
                        "SessionManager: copy source mismatch expected=%s detected=%s session=%s",
                        copy_source_chat_id,
                        detected_source_chat_id,
                        record.session_id[:8],
                    )
                    return None
                if copy_source_chat_id is None:
                    logger.error(
                        "SessionManager: cannot determine copy source for session %s",
                        record.session_id[:8],
                    )
                    return None

                try:
                    real_msg_id = await get_bot_real_message_id(
                        bot_client,
                        copy_source_chat_id,
                        sent_msg,
                        min_message_id=pre_upload_latest_id,
                    )
                except BotMessageResolutionError as resolve_err:
                    logger.error(
                        "SessionManager: blocked unsafe copy for session %s chat=%s sent_id=%s: %s",
                        record.session_id[:8],
                        copy_source_chat_id,
                        getattr(sent_msg, "id", None),
                        resolve_err,
                    )
                    return None

                _copy_ok = False
                for _copy_try in range(3):
                    try:
                        _copy_kwargs = {}
                        if _reply_to_id is not None:
                            _copy_kwargs["reply_to_message_id"] = _reply_to_id
                        copied_msg = await bot_client.copy_message(
                            chat_id=target_chat_id,
                            from_chat_id=copy_source_chat_id,
                            message_id=real_msg_id,
                            **_copy_kwargs,
                        )
                        _copy_ok = True
                        logger.debug(
                            "SessionManager: copied uploaded message %d to user %d",
                            real_msg_id, user_id,
                        )
                        break
                    except Exception as e:
                        if is_floodwait_error(e):
                            _cfw_wait = get_floodwait_seconds(e, default=10)
                            logger.warning(
                                "SessionManager: copy_message FloodWait %ds — retrying",
                                _cfw_wait,
                            )
                            await asyncio.sleep(min(_cfw_wait, 30))
                            continue
                        logger.warning(
                            "SessionManager: copy_message failed for user %d (attempt %d): %s",
                            user_id, _copy_try + 1, e,
                        )
                        if _copy_try < 2:
                            await asyncio.sleep(1.0 * (_copy_try + 1))

                # ONLY delete intermediate message if copy succeeded
                if _copy_ok:
                    try:
                        await bot_client.delete_messages(copy_source_chat_id, [real_msg_id])
                    except Exception:
                        pass  # best-effort cleanup
                else:
                    logger.error(
                        "SessionManager: copy_message failed after 3 attempts — "
                        "keeping intermediate msg %d for user %d",
                        real_msg_id, user_id,
                    )
                    return None

            return copied_msg or sent_msg

        except Exception as e:
            if is_floodwait_error(e):
                wait = get_floodwait_seconds(e)
                logger.warning(
                    "SessionManager: FloodWait %ds on session %s — marking flooded",
                    wait, record.session_id[:8],
                )
                await borrow_manager.mark_flood(record, float(wait))
                return None
            logger.warning(
                "SessionManager: upload error on session %s: %s: %s",
                record.session_id[:8], type(e).__name__, e,
            )
            return None

        finally:
            await borrow_manager.release(record, user_id)

    # ── Multi-session upload with rotation ────────────────────────────────────

    async def upload_for_user(
        self,
        user_id: int,
        target_chat_id: int,
        msg_type: str,
        file_path: str,
        send_kwargs: dict,
        bot_client: Client,
        video_meta: Optional[dict] = None,
        thumb_path: Optional[str] = None,
        progress_cb=None,
        max_attempts: int = 8,
        doc_file_name: Optional[str] = None,
        bot_user_id: Optional[int] = None,
    ) -> bool:
        """
        Try to upload for *user_id*, rotating sessions on failure.

        Returns True if the upload succeeded (caller does NOT need to send
        anything else — media is already in the user's chat).
        Returns False if all sessions failed or none are available
        (caller should fall back to split/acc path).
        """
        if not self._initialized:
            return False

        tried: Set[str] = set()

        for attempt in range(max_attempts):
            record = self._select_excluding(user_id, tried)
            if record is None:
                logger.info(
                    "SessionManager: no available session for user %d after %d attempt(s)",
                    user_id, attempt,
                )
                return False

            tried.add(record.session_id)
            result = await self.upload_with_session(
                record=record,
                user_id=user_id,
                target_chat_id=target_chat_id,
                msg_type=msg_type,
                file_path=file_path,
                send_kwargs=send_kwargs,
                bot_client=bot_client,
                video_meta=video_meta,
                thumb_path=thumb_path,
                progress_cb=progress_cb,
                bot_user_id=bot_user_id,
                doc_file_name=doc_file_name,
            )
            if result is not None:
                logger.info(
                    "SessionManager: upload succeeded via session %s (attempt %d)",
                    record.session_id[:8], attempt + 1,
                )
                return True
            # else: session failed, try next

        logger.warning(
            "SessionManager: all %d attempt(s) exhausted for user %d",
            max_attempts, user_id,
        )
        return False

    async def upload_with_system_session(
        self,
        target_chat_id: int,
        msg_type: str,
        file_path: str,
        send_kwargs: dict,
        bot_client: Client,
        video_meta: Optional[dict] = None,
        thumb_path: Optional[str] = None,
        progress_cb=None,
        max_attempts: int = 8,
        doc_file_name: Optional[str] = None,
        bot_user_id: Optional[int] = None,
    ) -> bool:
        """
        Upload a background/system job through GLOBAL or BORROWABLE sessions.

        The upload always copies from the uploader account's bot DM into
        target_chat_id, so monitored channel jobs can deliver to any configured
        target instead of the uploader's own bot chat.
        """
        if not self._initialized:
            return False

        tried: Set[str] = set()
        owner_for_borrow = int(target_chat_id)

        for attempt in range(max_attempts):
            record = self._select_system_excluding(tried)
            if record is None:
                logger.info(
                    "SessionManager: no available system session for target %d after %d attempt(s)",
                    target_chat_id, attempt,
                )
                return False

            tried.add(record.session_id)
            result = await self.upload_with_session(
                record=record,
                user_id=owner_for_borrow,
                target_chat_id=target_chat_id,
                msg_type=msg_type,
                file_path=file_path,
                send_kwargs=send_kwargs,
                bot_client=bot_client,
                video_meta=video_meta,
                thumb_path=thumb_path,
                progress_cb=progress_cb,
                bot_user_id=bot_user_id,
                doc_file_name=doc_file_name,
                force_copy=True,
            )
            if result is not None:
                logger.info(
                    "SessionManager: system upload succeeded via session %s (attempt %d)",
                    record.session_id[:8], attempt + 1,
                )
                return True

        logger.warning(
            "SessionManager: all %d system upload attempt(s) exhausted for target %d",
            max_attempts, target_chat_id,
        )
        return False

    # ── next_available_in ────────────────────────────────────────────────────

    def next_available_in_for_user(self, user_id: int) -> Optional[float]:
        """
        Return seconds until any session becomes available for *user_id*.
          0.0   → at least one is available right now
          N     → all flooded; earliest frees in N seconds
          None  → no sessions registered
        """
        candidates = (
            self._registry.get_by_owner(user_id)
            + self._registry.get_borrowable(exclude_owner=user_id)
            + self._registry.get_global()
        )
        if not candidates:
            return None
        ids = [r.session_id for r in candidates]
        return flood_controller.next_available_in(ids)

    # ── Owner controls ────────────────────────────────────────────────────────

    async def register_session(
        self,
        session_string: str,
        session_type: SessionType,
        owner_user_id: Optional[int] = None,
        phone: str = "",
    ) -> Tuple[bool, str, Optional[SessionRecord]]:
        """
        Validate and register a new session.

        Returns (success, message, record_or_None).
        Validates that the session is a working Telegram Premium account
        before storing it.
        """
        # Validate via MTProto
        try:
            from core.premium_logic import check_user_premium_via_session
            is_premium, info = await check_user_premium_via_session(session_string)
            if not is_premium:
                return False, "Session is not Premium", None
            # Extract phone/username from info dict
            if not phone and info:
                phone = info.get("username", "") or info.get("first_name", "") or ""
        except Exception as e:
            logger.warning("SessionManager.register_session: validation error: %s", e)
            return False, f"Validation error: {e}", None

        sid = self._registry.generate_session_id()
        record = SessionRecord(
            session_id=sid,
            session_string=session_string,
            phone=phone,
            type=session_type,
            owner_user_id=owner_user_id,
            enabled=True,
            allow_system_use=(session_type == SessionType.GLOBAL),
            allow_borrow=(session_type == SessionType.BORROWABLE),
            max_parallel_tasks=3,
        )
        await self._registry.add(record)
        logger.info(
            "SessionManager: registered %s session %s phone=%s",
            session_type.value, sid[:8], phone,
        )
        return True, f"Registered {session_type.value} session {sid[:8]}", record

    async def disable_session(self, session_id: str) -> Tuple[bool, str]:
        return await self._registry.disable_session(session_id)

    async def enable_session(self, session_id: str) -> bool:
        return await self._registry.enable_session(session_id)

    async def remove_session(self, session_id: str) -> bool:
        return await self._registry.remove(session_id)

    async def set_max_parallel_tasks(self, session_id: str, n: int) -> bool:
        if n < 1:
            return False
        return await self._registry.update_field(session_id, max_parallel_tasks=n)

    async def set_allow_borrow(self, session_id: str, allow: bool) -> bool:
        return await self._registry.update_field(session_id, allow_borrow=allow)

    async def disable_all_global(self) -> int:
        return await self._registry.disable_all_global()

    def find_by_prefix(self, prefix: str) -> Optional[SessionRecord]:
        return self._registry.find_by_prefix(prefix)

    # ── Status report ────────────────────────────────────────────────────────

    def status_text(self) -> str:
        """Human-readable status for /session status command."""
        records = self._registry.get_all()
        if not records:
            return "SessionManager: no sessions registered."

        lines = [f"SessionManager: {len(records)} session(s)\n"]
        for i, rec in enumerate(records, 1):
            flooded = flood_controller.is_flooded(rec.session_id)
            flood_str = ""
            if flooded:
                secs = flood_controller.seconds_until_free(rec.session_id)
                flood_str = f" [FLOOD {secs:.0f}s]"
            enabled_str = "✓" if rec.enabled else "✗"
            lines.append(
                f"{i}. [{enabled_str}] {rec.type.value[:3].upper()} "
                f"sid={rec.session_id[:8]} "
                f"phone={rec.phone or '?'} "
                f"tasks={rec.current_tasks}/{rec.max_parallel_tasks} "
                f"borrow={'Y' if rec.allow_borrow else 'N'}"
                f"{flood_str}"
            )
        return "\n".join(lines)

    # ── Registry accessor (for command handler) ───────────────────────────────

    @property
    def registry(self) -> SessionRegistry:
        return self._registry


# Module-level singleton
session_manager = SessionManager()
