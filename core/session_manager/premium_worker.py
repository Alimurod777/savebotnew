"""
core/session_manager/premium_worker.py — Thin bridge to UserUploadWorker.

Wraps the existing worker_registry singleton so SessionManager does not
duplicate any worker-lifecycle code.  The bridge's only added value is:
  - Translating WorkerBotBlockedError into a 24-hour flood mark
    (effectively fatal for the day).
  - Providing a single enqueue entry-point that propagates FloodWait
    upward for the caller to handle rotation.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait

from core.user_upload_worker import (
    UserUploadWorker,
    UserWorkerRegistry,
    WorkerBotBlockedError,
    UploadTask,
    make_send_fn,
    worker_registry,
)
from .borrow_manager import borrow_manager
from .models import SessionRecord

logger = logging.getLogger(__name__)

# 24 hours — effectively fatal for the day
_BOT_BLOCKED_FLOOD_SECONDS = 86400.0


class SessionWorkerBridge:
    """
    Bridge between SessionManager and UserWorkerRegistry.

    One instance is shared across all sessions; it simply delegates to
    the global worker_registry with the right parameters.
    """

    def __init__(self, bot_id: int, bot_username: Optional[str]) -> None:
        self._bot_id = bot_id
        self._bot_username = bot_username
        self._registry: UserWorkerRegistry = worker_registry

    # ── Worker acquisition ────────────────────────────────────────────────────

    async def get_worker(
        self,
        record: SessionRecord,
        user_id: int,
    ) -> Optional[UserUploadWorker]:
        """
        Return (or create) the worker for *record*'s session.
        On WorkerBotBlockedError: marks the session flooded for 24h and
        returns None so the caller can skip to the next session.
        """
        try:
            worker = await self._registry.get_or_create(
                user_id=user_id,
                session_string=record.session_string,
                bot_id=self._bot_id,
                bot_username=self._bot_username,
            )
            return worker
        except WorkerBotBlockedError:
            logger.warning(
                "SessionWorkerBridge: bot blocked in session %s for user %d — "
                "marking session flooded 24h",
                record.session_id[:8], user_id,
            )
            await borrow_manager.mark_flood(record, _BOT_BLOCKED_FLOOD_SECONDS)
            return None
        except Exception as e:
            logger.warning(
                "SessionWorkerBridge: failed to get worker for session %s: %s",
                record.session_id[:8], e,
            )
            return None

    # ── Task execution ────────────────────────────────────────────────────────

    async def enqueue_task(
        self,
        worker: UserUploadWorker,
        send_fn: Callable[[Client], Any],
        is_media: bool = True,
        owner_user_id: Optional[int] = None,
    ) -> Any:
        """
        Enqueue *send_fn* on *worker* and return the result.
        FloodWait is re-raised so SessionManager can handle rotation.
        """
        task = UploadTask(
            send_fn=send_fn,
            is_media=is_media,
            owner_user_id=owner_user_id,
        )
        return await worker.enqueue(task)

    # ── Convenience ───────────────────────────────────────────────────────────

    def build_send_fn(
        self,
        target_chat_id: int,
        msg_type: str,
        file_path: str,
        send_kwargs: dict,
        video_meta: Optional[dict] = None,
        thumb_path: Optional[str] = None,
        progress_cb=None,
        doc_file_name: Optional[str] = None,
    ) -> Callable[[Client], Any]:
        """Delegate directly to the existing make_send_fn helper."""
        return make_send_fn(
            target_chat_id=target_chat_id,
            msg_type=msg_type,
            file_path=file_path,
            send_kwargs=send_kwargs,
            video_meta=video_meta,
            thumb_path=thumb_path,
            progress_cb=progress_cb,
            doc_file_name=doc_file_name,
        )
