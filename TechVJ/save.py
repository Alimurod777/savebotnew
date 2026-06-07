
# Don't Remove Credit Tg - @VJ_Botz
# Subscribe YouTube Channel For Amazing Bot https://youtube.com/@Tech_VJ
# Ask Doubt on telegram @KingVJ01

import asyncio 
import random
import uuid
import pyrogram
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, UserAlreadyParticipant, InviteHashExpired, UsernameNotOccupied
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, Message,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio,
)
from core.reply_compat import build_reply_kwargs_from_message, build_link_preview_kwargs
from core.copy_utils import (
    BotMessageResolutionError,
    get_bot_copy_source_chat_id,
    get_bot_latest_message_id,
    get_bot_real_message_id,
)
from core.restricted_channel_guard import validate_restricted_channel_id
from core.safe_send import (
    safe_send_message as core_safe_send,
    safe_reply as core_safe_reply,
    safe_edit_message,
)
from core.message_utils import (
    split_caption as split_caption_tuple,
    split_message as split_message_chunks,
    normalize_poll_to_text,
    MAX_MESSAGE_LENGTH,
    MAX_CAPTION_LENGTH,
)
from core.failure_classifier import (
    FailureCategory,
    classify_failure,
    describe as describe_failure_category,
    is_reportable_to_owner,
    should_notify_user,
    stage_indicates_processing,
)
from pyrogram.enums import ChatType, ParseMode, UserStatus, PollType, MessageMediaType, MessageEntityType
from pyrogram.raw import functions, types
import time
import os
import shutil
import re
import logging
from typing import List, Tuple, Optional, Any
from dataclasses import dataclass, replace

logger = logging.getLogger(__name__)
from config import API_ID, API_HASH, OWNER_ID, OWNER_USERNAME, BANNED_MESSAGE, TEMP_DOWNLOAD_DIR
from database.async_db import async_db
from TechVJ.strings import strings, HELP_TXT

from TechVJ.file_splitter import (
    needs_splitting, split_file, cleanup_chunks, 
    get_chunk_info, format_size, MAX_TELEGRAM_FILE_SIZE,
    split_large_video, is_video_file
)
from core.structured_log import op_log
from TechVJ.task_manager import task_manager, StopSafePipeline, sanitize_filename

try:
    from TechVJ.activity_tracker import ping_activity, track_activity
except ImportError:
    def track_activity(func):
        return func
    async def ping_activity(user_id):
        return None

# Request-scoped structured logging (contextvars-based)
try:
    from core.request_context import (
        RequestContext as _ReqCtx,
        set_request_context as _set_req_ctx,
        clear_request_context as _clear_req_ctx,
    )
    _REQUEST_CONTEXT_AVAILABLE = True
except ImportError:
    _REQUEST_CONTEXT_AVAILABLE = False

# User download queue (one active per user)
from TechVJ.user_queue import user_queue, check_and_queue, format_queue_status

# Governance layer (role + rate limit + permission)
try:
    from core.role_manager import role_manager as _role_manager, UserRole as _UserRole
    from core.rate_limiter import rate_limiter as _rate_limiter
    from core.permission_guard import permission_guard as _permission_guard
    from TechVJ.owner_commands import is_maintenance as _is_maintenance
    _GOVERNANCE_AVAILABLE = True
except ImportError as _gov_err:
    _GOVERNANCE_AVAILABLE = False
    import logging as _logging
    _logging.getLogger(__name__).warning("Governance layer not available: %s", _gov_err)

# Import refactored modules (v2)
from TechVJ.session_handler import (
    create_user_session,
    SessionInvalidError,
    SessionConnectionError
)

# TopicExtractor for bulk topic fetch
try:
    from core.topic_extractor import TopicExtractor, TopicExtractorConfig, TopicExtractionError
    _TOPIC_EXTRACTOR_AVAILABLE = True
except ImportError:
    _TOPIC_EXTRACTOR_AVAILABLE = False

from TechVJ.album_collector_v2 import (
    AlbumAwareIterator,
    detect_album_boundary,
    process_album_with_session,
)

# Import download engine from core
from core.downloader import DownloadEngine, get_engine, progress_tracker

# Bot API throttle — FloodWait prevention
try:
    from core.bot_throttle import bot_throttle as _throttle, StatusTracker, BatchController
    _THROTTLE_AVAILABLE = True
except ImportError as _thr_err:
    _THROTTLE_AVAILABLE = False
    logging.getLogger(__name__).warning("Bot throttle not available: %s", _thr_err)

# Maximum retries for connection issues
MAX_RETRIES = 3
RETRY_DELAY = 2


# ── Task context (per request) ────────────────────────────────────────────────

@dataclass(frozen=True)
class TaskContext:
    task_id: str
    user_id: int
    source_chat_id: Optional[int]
    message_id: Optional[int]
    target_chat_id: int
    client_type: str  # "bot" | "user_session"

    def with_client_type(self, client_type: str) -> "TaskContext":
        return replace(self, client_type=client_type)


def _new_task_context(message: Optional[Message], parsed=None, client_type: str = "bot") -> TaskContext:
    user_id = None
    if message and getattr(message, "from_user", None) and getattr(message.from_user, "id", None):
        user_id = int(message.from_user.id)
    elif message and getattr(message, "chat", None) and getattr(message.chat, "id", None):
        user_id = int(message.chat.id)
    else:
        user_id = 0

    target_chat_id = user_id
    if message and getattr(message, "chat", None) and getattr(message.chat, "id", None):
        target_chat_id = int(message.chat.id)

    source_chat_id = getattr(parsed, "channel_id", None) if parsed is not None else None
    message_id = getattr(message, "id", None) or getattr(message, "message_id", None)

    return TaskContext(
        task_id=uuid.uuid4().hex,
        user_id=user_id,
        source_chat_id=source_chat_id,
        message_id=message_id,
        target_chat_id=target_chat_id,
        client_type=client_type,
    )


def _task_context_from_queue(item, parsed=None, client_type: str = "bot") -> TaskContext:
    return TaskContext(
        task_id=uuid.uuid4().hex,
        user_id=int(item.chat_id),
        source_chat_id=getattr(parsed, "channel_id", None) if parsed is not None else None,
        message_id=getattr(item, "message_id", None),
        target_chat_id=int(item.chat_id),
        client_type=client_type,
    )


def _task_context_for_channel_monitor(
    *,
    source_chat_id: int,
    source_message_id: int,
    target_chat_id: int,
    client_type: str = "user_session",
) -> TaskContext:
    return TaskContext(
        task_id=uuid.uuid4().hex,
        user_id=int(target_chat_id),
        source_chat_id=int(source_chat_id),
        message_id=int(source_message_id),
        target_chat_id=int(target_chat_id),
        client_type=client_type,
    )


async def _resolve_peer_safe(acc, peer_id: int, context: Optional[TaskContext] = None) -> None:
    """Resolve peer per request to avoid stale access_hash caches."""
    try:
        await acc.resolve_peer(peer_id)
    except Exception as e:
        logger.debug(
            "Peer resolve failed (task=%s peer=%s): %s",
            getattr(context, "task_id", "?"),
            peer_id,
            e,
        )


async def _validate_restricted_channel_request(
    client: Client,
    parsed,
    user_id: int,
) -> Tuple[bool, str]:
    """Block non-owner requests for enabled /addchannel sources before queueing."""
    source_ref = getattr(parsed, "channel_id", None)
    source_id: Optional[int] = source_ref if isinstance(source_ref, int) else None

    if source_id is None and isinstance(source_ref, str):
        url_type = getattr(parsed, "url_type", "")
        if url_type in {"public", "topic"}:
            try:
                chat = await client.get_chat(source_ref)
                resolved_id = getattr(chat, "id", None)
                if isinstance(resolved_id, int):
                    source_id = resolved_id
            except Exception as err:
                logger.debug(
                    "Restricted channel public resolve skipped source=%s: %s",
                    source_ref,
                    err,
                )

    return validate_restricted_channel_id(
        source_id,
        user_id=user_id,
        owner_id=OWNER_ID,
    )


def _is_photo_only_group(messages: list) -> bool:
    if not messages or len(messages) < 2:
        return False
    for m in messages:
        if not getattr(m, "photo", None):
            return False
    return True


async def _process_media_group_sequential(
    client: Client,
    acc,
    user_id: int,
    album_msgs: list,
    temp_dir: str,
    pipeline: StopSafePipeline,
    request_message: Optional[Message] = None,
    session_string: Optional[str] = None,
    context: Optional[TaskContext] = None,
) -> bool:
    processed_count = 0
    for msg in sorted(album_msgs, key=lambda m: m.id):
        if await pipeline.check_cancelled():
            return False
        msg_type = get_message_type(msg)
        if not msg_type or msg_type in ("Text", "Unknown"):
            continue
        try:
            success = await download_and_send_media(
                client,
                acc,
                _make_reply_target_message(user_id, getattr(request_message, "id", None)),
                msg,
                msg_type,
                temp_dir,
                pipeline,
                session_string=session_string,
                context=context,
            )
            if success:
                processed_count += 1
            await asyncio.sleep(1.5)
        except Exception as e:
            logger.warning("Sequential media group error: %s", e)
            continue
    return processed_count > 0


# ── Session invalidation helper ──────────────────────────────────────────────

async def _handle_session_invalid(client, user_id: int, error) -> None:
    """
    Session yaroqsiz bo'lganda:
    1. DB dan sessionni tozalash (logged_in=False, session=None)
    2. Foydalanuvchiga aniq xabar yuborish
    Hech qachon exception raise qilmaydi.
    """
    _fast_relogin_enabled = False

    # DB tozalash
    try:
        await async_db.update_user(user_id, {'session': None, 'logged_in': False})
        logger.info("Session auto-cleared for user %d (reason: %s)", user_id, getattr(error, 'error_type', 'unknown'))
        try:
            from TechVJ.login_rate_limiter import login_limiter
            _fast_relogin_enabled = await login_limiter.allow_fast_relogin(
                user_id,
                wait_seconds=60,
            )
        except Exception as _limit_err:
            logger.debug("Fast re-login update failed for user %d: %s", user_id, _limit_err)
    except Exception as _db_err:
        logger.warning("Failed to clear session for user %d: %s", user_id, _db_err)

    # Foydalanuvchiga xabar
    _error_type = getattr(error, 'error_type', 'unknown')
    _relogin_text = "/login bilan qayta kiring."
    if _fast_relogin_enabled:
        _relogin_text = "Tizim band bo'lmasa 1 daqiqadan keyin /login bilan qayta kiring."
    try:
        await core_safe_send(
            client, user_id,
            f"**Sessiyangiz muddati tugagan**\n"
            f"Sabab: `{_error_type}`\n\n"
            f"{_relogin_text}"
        )
    except Exception:
        pass


class PoolDeliveryError(RuntimeError):
    """Raised when a pool upload reaches the intermediate chat but not the user."""


def _prepare_upload_send_kwargs(send_kwargs: dict, is_pool_session: bool) -> Tuple[dict, dict]:
    """
    Return (upload_send_kwargs, direct_send_kwargs) for the current upload path.

    reply_to_message_id is ALWAYS stripped from upload kwargs because the
    user session uploads to bot_user_id where the user's message ID doesn't
    exist. The reply context is applied later via copy_message.
    """
    direct_send_kwargs = dict(send_kwargs)
    upload_send_kwargs = dict(send_kwargs)

    # Always strip reply context — user session uploads to bot_user_id
    # where the original user message doesn't exist (BUG-018 fix)
    upload_send_kwargs.pop("reply_to_message_id", None)
    upload_send_kwargs.pop("reply_parameters", None)

    return upload_send_kwargs, direct_send_kwargs


async def _enqueue_media_delivery(
    *,
    worker,
    send_fn,
    task_factory,
    is_pool_session: bool,
    bot_client: Client,
    target_user_id: int,
    request_message,
    reply_kwargs_builder=build_reply_kwargs_from_message,
):
    """
    Enqueue upload task and copy the result to the user with reply context.

    Pool sessions: upload → bot DM of pool account → resolve real message ID
    → copy_message to user → delete intermediate.

    Non-pool sessions: upload → user's own bot DM → message is directly
    visible to the user, no copy needed.
    """
    # ── Pre-upload watermark (pool only) ──────────────────────────────────
    copy_source_chat_id = None
    pre_upload_latest_id = None

    if is_pool_session:
        expected_source_chat_id = getattr(worker, "session_user_id", None)
        if expected_source_chat_id is None:
            raise PoolDeliveryError("Could not determine uploader account before upload")
        copy_source_chat_id = int(expected_source_chat_id)
        pre_upload_latest_id = await get_bot_latest_message_id(bot_client, copy_source_chat_id)
        if pre_upload_latest_id is None:
            raise PoolDeliveryError("Could not read bot-side upload watermark")

    # ── Enqueue upload ────────────────────────────────────────────────────
    upload_task = task_factory(send_fn=send_fn, is_media=True, owner_user_id=int(target_user_id))
    sent = await worker.enqueue(upload_task)
    if sent is None:
        return sent

    # ── Non-pool: message already visible in user's bot DM ────────────────
    if not is_pool_session:
        return sent

    # ── Pool: copy from pool↔bot DM to user chat ─────────────────────────
    detected_source_chat_id = get_bot_copy_source_chat_id(
        sent, getattr(getattr(sent, "chat", None), "id", None)
    )
    if detected_source_chat_id is not None and int(detected_source_chat_id) != int(copy_source_chat_id):
        logger.error(
            "copy source mismatch: expected=%s detected=%s sent_id=%s",
            copy_source_chat_id,
            detected_source_chat_id,
            getattr(sent, "id", None),
        )
        raise PoolDeliveryError("Bot copy source mismatch")

    try:
        real_msg_id = await get_bot_real_message_id(
            bot_client,
            copy_source_chat_id,
            sent,
            min_message_id=pre_upload_latest_id,
        )
    except BotMessageResolutionError as resolve_err:
        logger.error(
            "copy_message blocked: could not resolve bot-side message id chat=%s sent_id=%s: %s",
            copy_source_chat_id,
            getattr(sent, "id", None),
            resolve_err,
        )
        raise PoolDeliveryError(str(resolve_err)) from resolve_err

    copy_ok = False
    for copy_try in range(3):
        try:
            await bot_client.copy_message(
                chat_id=target_user_id,
                from_chat_id=copy_source_chat_id,
                message_id=real_msg_id,
                **reply_kwargs_builder(request_message),
            )
            copy_ok = True
            break
        except FloodWait as copy_wait:
            wait_seconds = getattr(copy_wait, "value", getattr(copy_wait, "x", 10))
            logger.warning("copy_message FloodWait %ds - retrying", wait_seconds)
            await asyncio.sleep(min(wait_seconds, 30))
        except Exception as copy_err:
            logger.warning(
                "copy_message to user failed (attempt %d): %s",
                copy_try + 1,
                copy_err,
            )
            if copy_try < 2:
                await asyncio.sleep(1.0 * (copy_try + 1))

    if copy_ok:
        try:
            await bot_client.delete_messages(copy_source_chat_id, [real_msg_id])
        except Exception:
            pass
        return sent

    # copy failed — keep intermediate msg for recovery, raise error
    logger.error(
        "copy_message failed after 3 attempts - keeping intermediate msg %d in chat %d",
        real_msg_id, copy_source_chat_id,
    )
    raise PoolDeliveryError(
        f"Pool upload reached intermediate chat but delivery to user {target_user_id} failed"
    )


def _get_user_session_target_chat_id(bot_client: Client, fallback_chat_id: int) -> int:
    """
    Resolve the chat that a user session must send to.

    Sending to the user's own numeric ID from MTProto can land in Saved Messages.
    To deliver into the bot PM, the user session must send to the bot's user ID.
    """
    try:
        bot_me = getattr(bot_client, "me", None)
        bot_id = getattr(bot_me, "id", None)
        if bot_id:
            return int(bot_id)
    except Exception:
        pass
    return int(fallback_chat_id)


def _positive_int(value) -> Optional[int]:
    """Return a positive int or None for empty/zero values."""
    try:
        number = int(round(float(value)))
    except Exception:
        return None
    return number if number > 0 else None


async def _get_local_video_artifacts(video_path: str, source_msg=None) -> Tuple[dict, Optional[str]]:
    """
    Extract local video metadata and thumbnail after download/split.

    Telegram often shows 0:00 or no thumb when we forward split chunks without
    explicit metadata, so we probe the actual file whenever possible.
    """
    meta = {}
    thumb_path = None

    try:
        from TechVJ.ffmpeg_utils import get_video_info, extract_thumbnail

        info = await asyncio.to_thread(get_video_info, video_path)
        if info:
            duration = _positive_int(info.get("duration"))
            width = _positive_int(info.get("width"))
            height = _positive_int(info.get("height"))
            if duration is not None:
                meta["duration"] = duration
            if width is not None:
                meta["width"] = width
            if height is not None:
                meta["height"] = height

        if source_msg and getattr(source_msg, "video", None):
            meta.setdefault("width", _positive_int(getattr(source_msg.video, "width", None)))
            meta.setdefault("height", _positive_int(getattr(source_msg.video, "height", None)))

        thumb_path = await asyncio.to_thread(extract_thumbnail, video_path)
    except Exception as e:
        logger.debug("Local video artifacts unavailable for %s: %s", video_path, e)

    return meta, thumb_path


def _build_split_part_caption(
    part_num: int,
    total_parts: int,
    chunk_size: int,
    caption: Optional[str] = None,
    limit: int = 1024,
) -> str:
    """Build a compact per-part caption like: Part 1/3 • 1.78 GB."""
    header = f"**Part {part_num}/{total_parts} • {format_size(chunk_size)}**"
    if part_num == 1 and caption:
        text = f"{header}\n\n{caption}"
    else:
        text = header

    if len(text) > limit:
        text = text[: limit - 3] + "..."
    return text


def _make_reply_target_message(user_id: int, reply_to_message_id: Optional[int] = None):
    """Create a minimal message-like object that preserves reply context."""
    return type(
        "ReplyTargetMessage",
        (),
        {
            "chat": type("Chat", (), {"id": user_id})(),
            "id": reply_to_message_id,
            "message_id": reply_to_message_id,
        },
    )()


async def _safe_status_edit_message(client: Client, chat_id: int, message_id: int, text: str) -> bool:
    """
    Best-effort status edit that never aborts the media pipeline on FloodWait.

    Progress/status messages are useful UI only. If Telegram throttles edits,
    the download/upload should continue instead of failing the whole request.
    """
    try:
        await client.edit_message_text(chat_id, message_id, text)
        return True
    except FloodWait as wait_err:
        wait_seconds = getattr(wait_err, "value", getattr(wait_err, "x", 0))
        logger.warning(
            "Status message edit FloodWait %ss for chat %s message %s - skipping status update",
            wait_seconds,
            chat_id,
            message_id,
        )
        return False
    except Exception as edit_err:
        logger.debug(
            "Status message edit skipped for chat %s message %s: %s",
            chat_id,
            message_id,
            edit_err,
        )
        return False


# ==================== URL PARSING (SECTION A) ====================

class ParsedURL:
    """Parsed URL result containing channel_id, post_ids, and optional topic/thread info"""
    def __init__(
        self, 
        channel_id: int, 
        post_ids: List[int], 
        url_type: str = "private",
        topic_id: int = None,
        thread_id: int = None,
        topic_range_anchor: Optional[Tuple[int, int]] = None,
    ):
        self.channel_id = channel_id
        self.post_ids = post_ids
        self.url_type = url_type  # "private", "public", "bot", "topic", "thread"
        self.topic_id = topic_id  # For topic links: https://t.me/c/CHAT/TOPIC/MSG
        self.thread_id = thread_id  # For thread links: ?thread=ID
        self.topic_range_anchor = topic_range_anchor
    
    @property
    def is_topic(self) -> bool:
        return self.topic_id is not None
    
    @property
    def is_thread(self) -> bool:
        return self.thread_id is not None
    
    def __repr__(self):
        extra = ""
        if self.topic_id:
            extra = f", topic_id={self.topic_id}"
        if self.thread_id:
            extra = f", thread_id={self.thread_id}"
        if self.topic_range_anchor:
            extra += f", topic_range_anchor={self.topic_range_anchor}"
        return f"ParsedURL(channel_id={self.channel_id}, post_ids={self.post_ids}, type={self.url_type}{extra})"


def parse_topic_url(url: str) -> Optional[ParsedURL]:
    """
    Parse topic URL format.

    Supported formats (private /c/ and public username):
        https://t.me/c/<channel_id>/<topic_id>/<post_id>
        https://t.me/c/<channel_id>/<topic_id>/<from_id>-<to_id>
        https://t.me/c/<channel_id>/<topic_id>/<id1>,<id2>,<id3>
        https://t.me/<username>/<topic_id>/<post_id>
        https://t.me/<username>/<topic_id>/<from_id>-<to_id>
        https://t.me/<username>/<topic_id>/<id1>,<id2>,<id3>

    Returns:
        ParsedURL with topic_id set, or None if not a topic URL
    """
    url = url.strip()

    # ── Private channel (/c/) formats ──────────────────────────────────────────

    # Topic with range: /c/CHAT/TOPIC/FROM-TO
    topic_range = re.match(r'^https?://t\.me/c/(\d+)/(\d+)/(\d+)\s*-\s*(\d+)$', url)
    if topic_range:
        channel_id = int("-100" + topic_range.group(1))
        topic_id = int(topic_range.group(2))
        from_id = int(topic_range.group(3))
        to_id = int(topic_range.group(4))

        return ParsedURL(
            channel_id,
            [from_id, to_id],
            "topic",
            topic_id=topic_id,
            topic_range_anchor=(from_id, to_id),
        )

    # Topic with comma-separated: /c/CHAT/TOPIC/ID1,ID2,ID3
    topic_multi = re.match(r'^https?://t\.me/c/(\d+)/(\d+)/(\d+(?:,\d+)+)$', url)
    if topic_multi:
        channel_id = int("-100" + topic_multi.group(1))
        topic_id = int(topic_multi.group(2))
        post_ids_str = topic_multi.group(3)
        post_ids = [int(pid.strip()) for pid in post_ids_str.split(',')]
        return ParsedURL(channel_id, post_ids, "topic", topic_id=topic_id)

    # Topic single: /c/CHAT/TOPIC/MSG
    topic_single = re.match(r'^https?://t\.me/c/(\d+)/(\d+)/(\d+)$', url)
    if topic_single:
        channel_id = int("-100" + topic_single.group(1))
        topic_id = int(topic_single.group(2))
        post_id = int(topic_single.group(3))
        return ParsedURL(channel_id, [post_id], "topic", topic_id=topic_id)

    # ── Public channel (username) formats ──────────────────────────────────────

    # Public topic with range: /username/TOPIC/FROM-TO
    pub_range = re.match(r'^https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)/(\d+)\s*-\s*(\d+)$', url)
    if pub_range:
        username = pub_range.group(1)
        topic_id = int(pub_range.group(2))
        from_id = int(pub_range.group(3))
        to_id = int(pub_range.group(4))
        return ParsedURL(
            username,
            [from_id, to_id],
            "topic",
            topic_id=topic_id,
            topic_range_anchor=(from_id, to_id),
        )

    # Public topic with comma-separated: /username/TOPIC/ID1,ID2,ID3
    pub_multi = re.match(r'^https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)/(\d+(?:,\d+)+)$', url)
    if pub_multi:
        username = pub_multi.group(1)
        topic_id = int(pub_multi.group(2))
        post_ids = [int(pid.strip()) for pid in pub_multi.group(3).split(',')]
        return ParsedURL(username, post_ids, "topic", topic_id=topic_id)

    # Public topic single: /username/TOPIC/MSG
    pub_single = re.match(r'^https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)/(\d+)$', url)
    if pub_single:
        username = pub_single.group(1)
        topic_id = int(pub_single.group(2))
        post_id = int(pub_single.group(3))
        return ParsedURL(username, [post_id], "topic", topic_id=topic_id)

    return None


def parse_thread_url(url: str) -> Optional[ParsedURL]:
    """
    Parse thread URL format (comment thread).
    
    Supported formats:
        https://t.me/c/<channel_id>/<post_id>?thread=<thread_id>
        https://t.me/c/<channel_id>/<post_id>?thread=<thread_id>&range<start>-<end>
        https://t.me/c/<channel_id>/<post_id>?thread=<thread_id>&range=<start>-<end>
    
    Returns:
        ParsedURL with thread_id set, or None if not a thread URL
    """
    url = url.strip()
    
    # Thread link with range (both &range= and &rangeSTART-END formats)
    # Format: /c/CHAT/MSG?thread=ID&range=START-END or /c/CHAT/MSG?thread=ID&rangeSTART-END
    thread_range_match = re.match(
        r'^https?://t\.me/c/(\d+)/(\d+)\?thread=(\d+)&range=?(\d+)-(\d+)$', url
    )
    if thread_range_match:
        channel_id = int("-100" + thread_range_match.group(1))
        post_id = int(thread_range_match.group(2))
        thread_id = int(thread_range_match.group(3))
        range_start = int(thread_range_match.group(4))
        range_end = int(thread_range_match.group(5))
        
        # Generate post IDs in range
        if range_start <= range_end:
            post_ids = list(range(range_start, range_end + 1))
        else:
            post_ids = list(range(range_start, range_end - 1, -1))
        
        return ParsedURL(channel_id, post_ids, "thread", thread_id=thread_id)
    
    # Thread link single: /c/CHAT/MSG?thread=ID
    thread_match = re.match(r'^https?://t\.me/c/(\d+)/(\d+)\?thread=(\d+)$', url)
    if thread_match:
        channel_id = int("-100" + thread_match.group(1))
        post_id = int(thread_match.group(2))
        thread_id = int(thread_match.group(3))
        return ParsedURL(channel_id, [post_id], "thread", thread_id=thread_id)
    
    return None


def parse_multi_post_url(url: str) -> Optional[ParsedURL]:
    """
    Parse URL with comma-separated post IDs.

    Supported formats:
        https://t.me/c/<channel_id>/<post_id1>,<post_id2>,<post_id3>
        https://t.me/<username>/<post_id1>,<post_id2>,<post_id3>

    Returns:
        ParsedURL object with channel_id and list of post_ids, or None if invalid
    """
    url = url.strip()

    # Private channel with comma-separated post IDs
    private_match = re.match(r'^https?://t\.me/c/(\d+)/(\d+(?:,\d+)*)$', url)
    if private_match:
        channel_id = int("-100" + private_match.group(1))
        try:
            post_ids = [int(pid.strip()) for pid in private_match.group(2).split(',')]
        except ValueError:
            return None
        if not post_ids:
            return None
        seen = set()
        unique_post_ids = []
        for pid in post_ids:
            if pid not in seen:
                seen.add(pid)
                unique_post_ids.append(pid)
        return ParsedURL(channel_id, unique_post_ids, "private")

    # Public channel with comma-separated post IDs
    public_match = re.match(r'^https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+(?:,\d+)+)$', url)
    if public_match:
        username = public_match.group(1)
        try:
            post_ids = [int(pid.strip()) for pid in public_match.group(2).split(',')]
        except ValueError:
            return None
        if not post_ids:
            return None
        seen = set()
        unique_post_ids = []
        for pid in post_ids:
            if pid not in seen:
                seen.add(pid)
                unique_post_ids.append(pid)
        return ParsedURL(username, unique_post_ids, "public")

    return None


def parse_range_url(url: str) -> Optional[ParsedURL]:
    """
    Parse URL with range format (from-to).

    Supported formats:
        https://t.me/c/<channel_id>/<from_id>-<to_id>
        https://t.me/<username>/<from_id>-<to_id>

    Returns:
        ParsedURL object with channel_id and list of post_ids in range
    """
    url = url.strip()

    # Private channel range
    private_match = re.match(r'^https?://t\.me/c/(\d+)/(\d+)\s*-\s*(\d+)$', url)
    if private_match:
        channel_id = int("-100" + private_match.group(1))
        from_id = int(private_match.group(2))
        to_id = int(private_match.group(3))
        if from_id <= to_id:
            post_ids = list(range(from_id, to_id + 1))
        else:
            post_ids = list(range(from_id, to_id - 1, -1))
        return ParsedURL(channel_id, post_ids, "private")

    # Public channel range
    public_match = re.match(r'^https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)\s*-\s*(\d+)$', url)
    if public_match:
        username = public_match.group(1)
        from_id = int(public_match.group(2))
        to_id = int(public_match.group(3))
        if from_id <= to_id:
            post_ids = list(range(from_id, to_id + 1))
        else:
            post_ids = list(range(from_id, to_id - 1, -1))
        return ParsedURL(username, post_ids, "public")

    return None


def parse_single_post_url(url: str) -> Optional[ParsedURL]:
    """
    Parse URL with single post ID.
    
    Supported formats:
        https://t.me/c/<channel_id>/<post_id>
        https://t.me/<username>/<post_id>
        https://t.me/b/<bot_username>/<post_id>
    """
    url = url.strip()
    
    # Private channel single post
    private_single = r'^https?://t\.me/c/(\d+)/(\d+)(?:\?.*)?$'
    match = re.match(private_single, url)
    if match:
        channel_id = int("-100" + match.group(1))
        post_id = int(match.group(2))
        return ParsedURL(channel_id, [post_id], "private")
    
    # Bot chat
    bot_pattern = r'^https?://t\.me/b/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)$'
    match = re.match(bot_pattern, url)
    if match:
        username = match.group(1)
        post_id = int(match.group(2))
        return ParsedURL(username, [post_id], "bot")
    
    # Public channel/group
    public_pattern = r'^https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)$'
    match = re.match(public_pattern, url)
    if match:
        username = match.group(1)
        post_id = int(match.group(2))
        return ParsedURL(username, [post_id], "public")
    
    return None


def parse_quizbot_url(url: str) -> Optional[ParsedURL]:
    """
    Parse QuizBot links.
    
    Supported format:
        https://t.me/QuizBot?start=XXXX
    
    Returns:
        ParsedURL with url_type="quizbot", or None
    """
    url = url.strip()
    
    quizbot_match = re.match(r'^https?://t\.me/QuizBot\?start=([a-zA-Z0-9_-]+)$', url, re.IGNORECASE)
    if quizbot_match:
        start_param = quizbot_match.group(1)
        # Store start param in post_ids[0] as a string marker
        return ParsedURL("QuizBot", [start_param], "quizbot")
    
    return None


def parse_telegram_url(url: str) -> Tuple[Optional[ParsedURL], Optional[str]]:
    """
    Main URL parser - tries all formats.
    
    Order of parsing (most specific first):
    1. Topic links (3 path segments)
    2. Thread links (?thread=)
    3. QuizBot links
    4. Comma-separated post IDs
    5. Range format (from-to)
    6. Single post format
    
    Returns:
        Tuple of (ParsedURL or None, error_message or None)
    """
    url = url.strip()
    
    if not url.startswith(('https://t.me/', 'http://t.me/')):
        return None, "Invalid URL: Must start with https://t.me/"
    
    # 1. Try topic format first (most specific - 3 path segments)
    result = parse_topic_url(url)
    if result:
        return result, None
    
    # 2. Try thread format (?thread=)
    result = parse_thread_url(url)
    if result:
        return result, None
    
    # 3. Try QuizBot
    result = parse_quizbot_url(url)
    if result:
        return result, None
    
    # 4. Try comma-separated format
    result = parse_multi_post_url(url)
    if result:
        return result, None
    
    # 5. Try range format
    result = parse_range_url(url)
    if result:
        return result, None
    
    # 6. Try single post format
    result = parse_single_post_url(url)
    if result:
        return result, None
    
    return None, (
        "Invalid URL format. Supported formats:\n"
        "• `https://t.me/c/123456/101,102,103` (comma-separated)\n"
        "• `https://t.me/c/123456/101-110` (range)\n"
        "• `https://t.me/c/123456/101` (single)\n"
        "• `https://t.me/c/123456/5/101` (topic)\n"
        "• `https://t.me/c/123456/101?thread=5` (thread)\n"
        "• `https://t.me/username/101` (public single)\n"
        "• `https://t.me/username/101-110` (public range)\n"
        "• `https://t.me/username/101,102,103` (public comma)\n"
        "• `https://t.me/username/5/101` (public topic)"
    )


# ==================== HELPER FUNCTIONS ====================

def get(obj, key, default=None):
    try:
        return obj[key]
    except (KeyError, TypeError, IndexError):
        return default


def _owner_diag_truncate(value: Any, limit: int = 1200) -> str:
    """Return a compact plain-text field for owner diagnostics."""
    if value is None:
        return "-"
    text = str(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 20)] + f"\n...[truncated {len(text) - limit} chars]"


def _owner_diag_error(error: Any) -> str:
    if not error:
        return "-"
    return f"{type(error).__name__}: {_owner_diag_truncate(error, 700)}"


def _owner_diag_user_line(request_message: Optional[Message], user_id: int) -> str:
    user = getattr(request_message, "from_user", None) if request_message else None
    if not user:
        return f"id={user_id}"
    name_parts = [
        getattr(user, "first_name", None),
        getattr(user, "last_name", None),
    ]
    name = " ".join(p for p in name_parts if p) or "-"
    username = getattr(user, "username", None)
    phone = getattr(user, "phone_number", None)
    parts = [f"id={user_id}", f"name={name}"]
    if username:
        parts.append(f"username=@{username}")
    if phone:
        parts.append(f"phone={phone}")
    return " | ".join(parts)


def _owner_diag_chat_lines(chat: Any) -> List[str]:
    if not chat:
        return ["Chat: -"]
    title = getattr(chat, "title", None) or getattr(chat, "first_name", None) or "-"
    username = getattr(chat, "username", None)
    chat_type = getattr(getattr(chat, "type", None), "value", None) or getattr(chat, "type", None) or "-"
    members = getattr(chat, "members_count", None)
    lines = [
        f"Chat: {title}",
        f"Chat ID: {getattr(chat, 'id', '-')}",
        f"Chat type: {chat_type}",
    ]
    if username:
        lines.append(f"Chat username: @{username}")
    if members is not None:
        lines.append(f"Members: {members}")
    return lines


def _owner_diag_media_lines(msg: Any) -> List[str]:
    lines: List[str] = []
    msg_type = get_message_type(msg) if msg else "Unknown"
    lines.append(f"Post type: {msg_type}")
    if not msg:
        return lines

    if getattr(msg, "media_group_id", None):
        lines.append(f"Media group ID: {msg.media_group_id}")
    if getattr(msg, "has_protected_content", None):
        lines.append("Protected content: yes")
    if getattr(msg, "reply_markup", None):
        lines.append("Reply markup: yes")

    poll = getattr(msg, "poll", None)
    if poll:
        options = getattr(poll, "options", None) or []
        lines.append(f"Poll question: {_owner_diag_truncate(getattr(poll, 'question', '-'), 250)}")
        lines.append(f"Poll options: {len(options)}")
        for idx, opt in enumerate(options[:10], start=1):
            lines.append(f"  {idx}. {_owner_diag_truncate(getattr(opt, 'text', '-'), 120)}")

    media_attrs = (
        "photo",
        "video",
        "document",
        "audio",
        "voice",
        "video_note",
        "animation",
        "sticker",
    )
    for attr in media_attrs:
        media = getattr(msg, attr, None)
        if not media:
            continue
        size = getattr(media, "file_size", None)
        if size:
            lines.append(f"Media size: {format_size(size)} ({size} bytes)")
        file_name = getattr(media, "file_name", None)
        if file_name:
            lines.append(f"File name: {_owner_diag_truncate(file_name, 250)}")
        mime_type = getattr(media, "mime_type", None)
        if mime_type:
            lines.append(f"Mime type: {mime_type}")
        duration = getattr(media, "duration", None)
        if duration:
            lines.append(f"Duration: {duration}s")
        width = getattr(media, "width", None)
        height = getattr(media, "height", None)
        if width and height:
            lines.append(f"Dimensions: {width}x{height}")
        file_unique_id = getattr(media, "file_unique_id", None)
        if file_unique_id:
            lines.append(f"File unique ID: {file_unique_id}")
        break

    text = None
    entities = None
    limit_kind = "message"
    if getattr(msg, "text", None):
        text = str(msg.text)
        entities = list(getattr(msg, "entities", None) or [])
    elif getattr(msg, "caption", None):
        text = str(msg.caption)
        entities = list(getattr(msg, "caption_entities", None) or [])
        limit_kind = "caption"

    if text:
        try:
            from core.entity_validator import utf16_length as _utf16_length
            utf16_len = _utf16_length(text)
        except Exception:
            utf16_len = len(text)
        lines.append(f"{limit_kind.title()} length: {len(text)} chars / {utf16_len} UTF-16")
        lines.append(f"Entities: {len(entities)}")
        lines.append("Text preview:")
        lines.append(_owner_diag_truncate(text, 1500))
    else:
        lines.append("Text/caption: none")

    return lines


def _owner_diag_post_link(chat_id: Any, post_id: int, chat: Any = None) -> str:
    username = getattr(chat, "username", None) if chat else None
    if username:
        return f"https://t.me/{username}/{post_id}"
    if isinstance(chat_id, int) and str(chat_id).startswith("-100"):
        return f"https://t.me/c/{str(chat_id)[4:]}/{post_id}"
    return "-"


def _owner_diag_retry_url(
    chat_id: Any,
    post_id: int,
    parsed: Optional[ParsedURL] = None,
    chat: Any = None,
) -> Optional[str]:
    username = getattr(chat, "username", None) if chat else None
    topic_id = getattr(parsed, "topic_id", None) if parsed else None
    thread_id = getattr(parsed, "thread_id", None) if parsed else None

    if username:
        if topic_id:
            base = f"https://t.me/{username}/{topic_id}/{post_id}"
        else:
            base = f"https://t.me/{username}/{post_id}"
    elif isinstance(chat_id, int) and str(chat_id).startswith("-100"):
        short_id = str(chat_id)[4:]
        if topic_id:
            base = f"https://t.me/c/{short_id}/{topic_id}/{post_id}"
        else:
            base = f"https://t.me/c/{short_id}/{post_id}"
    else:
        return None

    if thread_id and not topic_id:
        return f"{base}?thread={thread_id}"
    return base


async def _log_failed_download_local(
    *,
    user_id: int,
    chat_id: Any,
    post_id: int,
    stage: str,
    reason: str,
    error: Any = None,
    parsed: Optional[ParsedURL] = None,
    retry_url: Optional[str] = None,
    details: Optional[dict] = None,
) -> Optional[int]:
    try:
        from database.local_storage import LocalStorage, is_local_storage_available

        if not is_local_storage_available():
            return None

        return await LocalStorage.log_failed_download(
            user_id=user_id,
            chat_id=chat_id,
            post_id=post_id,
            url_type=getattr(parsed, "url_type", None),
            topic_id=getattr(parsed, "topic_id", None),
            thread_id=getattr(parsed, "thread_id", None),
            stage=stage,
            reason=reason,
            error=_owner_diag_error(error),
            retry_url=retry_url,
            details=details,
        )
    except Exception as e:
        logger.debug("Failed download log skipped: %s", e)
        return None


def _classify_post_failure(
    stage: str,
    reason: str,
    error: Any = None,
    source_msg: Any = None,
) -> Tuple[FailureCategory, bool, bool, bool]:
    category = classify_failure(stage, reason, error)
    message_fetched = bool(source_msg and not getattr(source_msg, "empty", False))
    processing_started = bool(message_fetched or stage_indicates_processing(stage))
    system_exception = bool(
        isinstance(error, BaseException)
        and category == FailureCategory.SYSTEM_FAILURE
    )
    return category, message_fetched, processing_started, system_exception


async def _owner_diag_verified_context(acc, chat_id: Any) -> Tuple[bool, dict]:
    """Verify session, channel existence, and membership before notifying owner."""
    info = {
        "me": None,
        "chat": None,
        "member_status": "-",
        "skip_reason": "",
    }
    try:
        info["me"] = await asyncio.wait_for(acc.get_me(), timeout=10.0)
    except Exception as e:
        info["skip_reason"] = f"session_inactive: {_owner_diag_error(e)}"
        return False, info

    try:
        info["chat"] = await asyncio.wait_for(acc.get_chat(chat_id), timeout=10.0)
    except Exception as e:
        info["skip_reason"] = f"channel_unavailable: {_owner_diag_error(e)}"
        return False, info

    try:
        member = await asyncio.wait_for(acc.get_chat_member(chat_id, "me"), timeout=10.0)
        status = str(getattr(member, "status", "-"))
        info["member_status"] = status
        status_upper = status.upper()
        if any(marker in status_upper for marker in ("LEFT", "BANNED", "KICKED")):
            info["skip_reason"] = f"not_member: {status}"
            return False, info
    except Exception as e:
        # For private numeric chats, successful get_chat through the user
        # session is already a strong membership signal. Public usernames are
        # not enough because get_chat can work without joining.
        if isinstance(chat_id, int) and chat_id < 0:
            info["member_status"] = (
                "verified_by_get_chat; get_chat_member failed: "
                f"{_owner_diag_error(e)}"
            )
        else:
            info["skip_reason"] = f"membership_unverified: {_owner_diag_error(e)}"
            return False, info

    return True, info


async def _notify_owner_channel_post_failure(
    client: Client,
    acc,
    *,
    user_id: int,
    chat_id: Any,
    post_id: int,
    stage: str,
    reason: str,
    error: Any = None,
    source_msg: Any = None,
    request_message: Optional[Message] = None,
    context: Optional[TaskContext] = None,
    parsed: Optional[ParsedURL] = None,
    requested_total: Optional[int] = None,
) -> None:
    """
    Send a precise owner-only diagnostic when an existing user/session/channel
    context fails to deliver one post. Text is sent only by the bot client.
    """
    if not OWNER_ID:
        return

    try:
        category, message_fetched, processing_started, system_exception = _classify_post_failure(
            stage,
            reason,
            error,
            source_msg,
        )
        if not is_reportable_to_owner(
            category,
            message_fetched=message_fetched,
            processing_started=processing_started,
            system_exception=system_exception,
        ):
            logger.debug(
                "Skipping owner diagnostic for user=%s chat=%s post=%s: category=%s stage=%s reason=%s",
                user_id,
                chat_id,
                post_id,
                category.value,
                stage,
                reason,
            )
            return

        verified, verify_info = await _owner_diag_verified_context(acc, chat_id)
        if not verified:
            logger.debug(
                "Skipping owner post-failure diagnostic for user=%s chat=%s post=%s: %s",
                user_id,
                chat_id,
                post_id,
                verify_info.get("skip_reason"),
            )
            return

        fetched_for_report = False
        source_fetch_error = None
        if source_msg is None:
            try:
                source_msg = await asyncio.wait_for(
                    acc.get_messages(chat_id, post_id),
                    timeout=15.0,
                )
                fetched_for_report = True
            except Exception as e:
                source_fetch_error = e

        post_exists = bool(source_msg and not getattr(source_msg, "empty", False))
        me = verify_info.get("me")
        chat = verify_info.get("chat")

        lines = [
            "Post delivery failure report",
            "",
            "User:",
            _owner_diag_user_line(request_message, user_id),
            "",
            "Request:",
            f"Task ID: {getattr(context, 'task_id', '-')}",
            f"URL type: {getattr(parsed, 'url_type', '-')}",
            f"Source chat: {chat_id}",
            f"Post ID: {post_id}",
            "Scope: this report is for this one failed post ID",
            f"Post link: {_owner_diag_post_link(chat_id, post_id, chat)}",
            f"Requested total: {requested_total if requested_total is not None else '-'}",
            f"Topic ID: {getattr(parsed, 'topic_id', '-')}",
            f"Thread ID: {getattr(parsed, 'thread_id', '-')}",
            "",
            "Failure:",
            f"Stage: {stage}",
            f"Reason: {reason}",
            f"Error: {_owner_diag_error(error)}",
            "",
            "Verified before notify:",
            f"Session user: id={getattr(me, 'id', '-')} username=@{getattr(me, 'username', '-')}",
            f"Member status: {verify_info.get('member_status', '-')}",
        ]
        lines.extend(_owner_diag_chat_lines(chat))
        lines.extend([
            "",
            "Post:",
            f"Exists/readable now: {'yes' if post_exists else 'no'}",
            f"Fetched for report: {'yes' if fetched_for_report else 'no'}",
        ])

        if source_fetch_error:
            lines.append(f"Fetch-for-report error: {_owner_diag_error(source_fetch_error)}")

        if post_exists:
            lines.extend([
                f"Message ID: {getattr(source_msg, 'id', '-')}",
                f"Date: {getattr(source_msg, 'date', '-')}",
                f"Edit date: {getattr(source_msg, 'edit_date', '-')}",
            ])
            lines.extend(_owner_diag_media_lines(source_msg))

        retry_url = _owner_diag_retry_url(chat_id, post_id, parsed, chat)
        log_id = await _log_failed_download_local(
            user_id=user_id,
            chat_id=chat_id,
            post_id=post_id,
            stage=stage,
            reason=reason,
            error=error,
            parsed=parsed,
            retry_url=retry_url,
            details={
                "task_id": getattr(context, "task_id", None),
                "requested_total": requested_total,
                "failure_category": category.value,
                "failure_category_label": describe_failure_category(category),
                "post_exists": post_exists,
                "post_link": _owner_diag_post_link(chat_id, post_id, chat),
                "source_fetch_error": _owner_diag_error(source_fetch_error),
            },
        )

        report = "\n".join(lines)
        chunks = split_message_chunks(report)
        for index, chunk in enumerate(chunks):
            reply_markup = None
            if log_id and retry_url and index == len(chunks) - 1:
                reply_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Qayta urinish", callback_data=f"retry_failed:{log_id}")]
                ])
            await client.send_message(
                OWNER_ID,
                chunk,
                parse_mode=ParseMode.DISABLED,
                reply_markup=reply_markup,
                **build_link_preview_kwargs(is_disabled=True),
            )
    except Exception as e:
        logger.warning("Could not send owner post-failure diagnostic: %s", e)


def _user_failure_text(stage: str, reason: str, post_id: int, error: Any = None) -> str:
    reason_text = str(reason or "").strip()
    error_text = _owner_diag_error(error)
    joined = f"{stage} {reason_text} {error_text}".upper()

    if "FLOODWAIT" in joined or "FLOOD" in joined:
        short = "Telegram vaqtinchalik kutishni talab qildi."
    elif "DELETED" in joined or "EMPTY" in joined or "MSG_ID_INVALID" in joined:
        short = "Post o'chirilgan yoki mavjud emas."
    elif "RESTRICT" in joined or "PROTECTED" in joined or "FORBIDDEN" in joined:
        short = "Post cheklangan yoki yuborishga ruxsat yo'q."
    elif "TIMEOUT" in joined:
        short = "Postni olish vaqti tugadi."
    else:
        short = "Postni olish yoki yuborishda xatolik yuz berdi."

    return (
        "Post o'tkazib yuborildi.\n"
        f"Post ID: {post_id}\n"
        f"Sabab: {short}\n"
        "Keyingi postga o'tyapman."
    )


async def _notify_realtime_post_failure(
    client: Client,
    acc,
    *,
    user_id: int,
    chat_id: Any,
    post_id: int,
    stage: str,
    reason: str,
    error: Any = None,
    source_msg: Any = None,
    request_message: Optional[Message] = None,
    context: Optional[TaskContext] = None,
    parsed: Optional[ParsedURL] = None,
    requested_total: Optional[int] = None,
    notify_user: bool = True,
) -> None:
    await ping_activity(user_id)

    category, _, _, _ = _classify_post_failure(stage, reason, error, source_msg)

    if category == FailureCategory.SYSTEM_FAILURE:
        await _notify_owner_channel_post_failure(
            client,
            acc,
            user_id=user_id,
            chat_id=chat_id,
            post_id=post_id,
            stage=stage,
            reason=reason,
            error=error,
            source_msg=source_msg,
            request_message=request_message,
            context=context,
            parsed=parsed,
            requested_total=requested_total,
        )
    else:
        logger.debug(
            "Post failure kept out of owner diagnostics: category=%s user=%s chat=%s post=%s stage=%s reason=%s",
            category.value,
            user_id,
            chat_id,
            post_id,
            stage,
            reason,
        )

    if notify_user and user_id and should_notify_user(category):
        try:
            await client.send_message(
                user_id,
                _user_failure_text(stage, reason, post_id, error),
                parse_mode=ParseMode.DISABLED,
                **build_reply_kwargs_from_message(request_message),
            )
        except Exception as user_notify_err:
            logger.debug(
                "Realtime user failure notify skipped user=%s post=%s: %s",
                user_id,
                post_id,
                user_notify_err,
            )

    await ping_activity(user_id)


async def _notify_bot_only_post_failure(
    client: Client,
    *,
    user_id: int,
    chat_id: Any,
    post_id: int,
    stage: str,
    reason: str,
    error: Any = None,
    request_message: Optional[Message] = None,
    parsed: Optional[ParsedURL] = None,
    requested_total: Optional[int] = None,
) -> None:
    """Bot-only realtime diagnostic for flows that do not have a user session."""
    await ping_activity(user_id)

    category, message_fetched, processing_started, system_exception = _classify_post_failure(
        stage,
        reason,
        error,
        None,
    )
    owner_reportable = is_reportable_to_owner(
        category,
        message_fetched=message_fetched,
        processing_started=processing_started,
        system_exception=system_exception,
    )

    retry_url = _owner_diag_retry_url(chat_id, post_id, parsed)
    log_id = None
    if owner_reportable:
        log_id = await _log_failed_download_local(
            user_id=user_id,
            chat_id=chat_id,
            post_id=post_id,
            stage=stage,
            reason=reason,
            error=error,
            parsed=parsed,
            retry_url=retry_url,
            details={
                "requested_total": requested_total,
                "failure_category": category.value,
                "failure_category_label": describe_failure_category(category),
            },
        )

    if OWNER_ID and owner_reportable:
        try:
            lines = [
                "Post delivery failure report",
                "",
                "User:",
                _owner_diag_user_line(request_message, user_id),
                "",
                "Request:",
                f"URL type: {getattr(parsed, 'url_type', '-')}",
                f"Source chat: {chat_id}",
                f"Post ID: {post_id}",
                f"Post link: {_owner_diag_post_link(chat_id, post_id)}",
                f"Requested total: {requested_total if requested_total is not None else '-'}",
                "",
                "Failure:",
                f"Category: {describe_failure_category(category)}",
                f"Stage: {stage}",
                f"Reason: {reason}",
                f"Error: {_owner_diag_error(error)}",
                "",
                "Verified before notify:",
                "No user session was available in this bot-only path.",
                f"SQLite log ID: {log_id if log_id else '-'}",
            ]
            reply_markup = None
            if log_id and retry_url:
                reply_markup = InlineKeyboardMarkup([[
                    InlineKeyboardButton("Qayta urinish", callback_data=f"retry_failed:{log_id}")
                ]])
            await client.send_message(
                OWNER_ID,
                "\n".join(lines),
                parse_mode=ParseMode.DISABLED,
                reply_markup=reply_markup,
                **build_link_preview_kwargs(is_disabled=True),
            )
        except Exception as owner_err:
            logger.debug("Bot-only owner failure notify skipped: %s", owner_err)
    elif not owner_reportable:
        logger.debug(
            "Bot-only post failure kept out of owner diagnostics: category=%s user=%s chat=%s post=%s stage=%s reason=%s",
            category.value,
            user_id,
            chat_id,
            post_id,
            stage,
            reason,
        )

    if should_notify_user(category):
        try:
            await client.send_message(
                user_id,
                _user_failure_text(stage, reason, post_id, error),
                parse_mode=ParseMode.DISABLED,
                **build_reply_kwargs_from_message(request_message),
            )
        except Exception as user_err:
            logger.debug("Bot-only user failure notify skipped user=%s post=%s: %s", user_id, post_id, user_err)

    await ping_activity(user_id)

def sanitize_html(content):
    """Fixes common HTML issues like unclosed tags."""
    if not content:
        return content
    tags = ['b', 'i', 'u', 'a', 'code', 'pre']
    for tag in tags:
        open_tags = len(re.findall(f'<{tag}[^>]*>', content))
        close_tags = len(re.findall(f'</{tag}>', content))
        if open_tags > close_tags:
            content += f'</{tag}>' * (open_tags - close_tags)
    return content


def sanitize_markdown(content):
    """
    Fixes common Markdown issues.
    
    IMPORTANT: Be conservative - don't break working text.
    Only fix clearly broken patterns.
    """
    if not content:
        return content
    
    # Fix unclosed markdown links: [text](url -> [text](url)
    pattern = r'\[([^\]]+)\]\(([^)]*[^)])'
    matches = re.findall(pattern, content)
    for text, url in matches:
        old = f'[{text}]({url}'
        new = f'[{text}]({url})'
        content = content.replace(old, new)
    
    # Fix escaped parentheses from bad parsing
    content = re.sub(r'\\\(', '(', content)
    content = re.sub(r'\\\)', ')', content)
    
    return content


def get_caption_with_entities(msg) -> Tuple[Optional[str], Optional[List]]:
    """
    Extract caption with entities from message.

    CRITICAL: Returns plain str (not Pyrogram Str subclass) so that
    slicing never triggers Str.__getitem__ → remove_surrogates → utf-16-le error.
    """
    if not msg.caption:
        return None, None

    raw_caption = str(msg.caption)          # plain str, no Str.__getitem__
    entities = getattr(msg, 'caption_entities', None)

    return raw_caption, list(entities) if entities else None


def get_text_with_entities(msg) -> Tuple[Optional[str], Optional[List]]:
    """
    Extract text with entities from message.

    CRITICAL: Returns plain str (not Pyrogram Str subclass) so that
    slicing never triggers Str.__getitem__ → remove_surrogates → utf-16-le error.
    """
    if not msg.text:
        return None, None

    raw_text = str(msg.text)                # plain str, no Str.__getitem__
    entities = getattr(msg, 'entities', None)

    return raw_text, list(entities) if entities else None


# ==================== SAFE SEND FUNCTIONS (ENTITY-ONLY) ====================
# These functions NEVER use parse_mode - only MessageEntity for 100% safety

async def send_text_entity_safe(
    client: Client,
    chat_id: int,
    text: str,
    entities: Optional[List] = None,
    reply_to_message_id: Optional[int] = None
) -> Optional[Message]:
    """
    Send text message using ONLY entities (never parse_mode).
    
    SAFE: Prevents ENTITY_BOUNDS_INVALID by validating before send.
    """
    from core.entity_validator import prepare_entities_for_send, utf16_length, MESSAGE_LIMIT
    
    if not text:
        return None
    
    # Validate entities
    safe_entities = prepare_entities_for_send(text, entities) if entities else None
    
    kwargs = {
        'chat_id': chat_id,
        'text': text,
        'disable_web_page_preview': True,
    }
    
    if safe_entities:
        kwargs['entities'] = safe_entities
    
    # Try with reply first
    if reply_to_message_id:
        try:
            return await client.send_message(**kwargs, reply_to_message_id=reply_to_message_id)
        except Exception:
            pass  # Silent fallback
    
    # Send without reply
    try:
        return await client.send_message(**kwargs)
    except FloodWait as e:
        wait = min(getattr(e, 'value', getattr(e, 'x', 30)), 60)
        await asyncio.sleep(wait)
        return await client.send_message(**kwargs)
    except Exception as e:
        # If entity error, try plain text
        if 'ENTITY' in str(e).upper() and safe_entities:
            try:
                del kwargs['entities']
                return await client.send_message(**kwargs)
            except Exception:
                pass
        logger.warning(f"send_text_entity_safe failed: {e}")
        return None


async def send_media_entity_safe(
    client: Client,
    chat_id: int,
    media_type: str,
    file_path: str,
    caption: Optional[str] = None,
    caption_entities: Optional[List] = None,
    reply_to_message_id: Optional[int] = None,
    progress: Any = None,
    **extra_kwargs
) -> Tuple[Optional[Message], Optional[Message]]:
    """
    Send media using ONLY caption_entities (never parse_mode).
    
    SAFE: Prevents ENTITY_BOUNDS_INVALID.
    Returns (media_msg, overflow_msg) - overflow_msg is for long captions.
    """
    from core.entity_validator import (
        prepare_entities_for_send, 
        split_caption_with_entities,
        utf16_length,
        CAPTION_LIMIT
    )
    
    caption_kwargs = {}
    overflow_chunks = None
    
    if caption:
        cap_len = utf16_length(caption)
        
        if cap_len <= CAPTION_LIMIT:
            safe_entities = prepare_entities_for_send(caption, caption_entities) if caption_entities else None
            caption_kwargs['caption'] = caption
            if safe_entities:
                caption_kwargs['caption_entities'] = safe_entities
        else:
            # Long caption - split
            cap_chunk, overflow_chunks = split_caption_with_entities(caption, caption_entities or [])
            caption_kwargs['caption'] = cap_chunk.text
            if cap_chunk.entities:
                caption_kwargs['caption_entities'] = cap_chunk.entities
    
    # Build send kwargs
    send_kwargs = {
        'chat_id': chat_id,
        media_type: file_path,
        **caption_kwargs,
        **extra_kwargs
    }
    
    if progress:
        send_kwargs['progress'] = progress
    
    send_func = getattr(client, f'send_{media_type}')
    
    # Try with reply first
    media_msg = None
    if reply_to_message_id:
        try:
            media_msg = await send_func(**send_kwargs, reply_to_message_id=reply_to_message_id)
        except Exception:
            pass
    
    if not media_msg:
        try:
            media_msg = await send_func(**send_kwargs)
        except FloodWait as e:
            wait = min(getattr(e, 'value', getattr(e, 'x', 30)), 60)
            await asyncio.sleep(wait)
            media_msg = await send_func(**send_kwargs)
        except Exception as e:
            # If entity error, try without entities
            if 'ENTITY' in str(e).upper() and 'caption_entities' in send_kwargs:
                try:
                    del send_kwargs['caption_entities']
                    media_msg = await send_func(**send_kwargs)
                except Exception:
                    pass
            if not media_msg:
                logger.warning(f"send_media_entity_safe failed: {e}")
                return None, None
    
    # Send overflow caption if needed
    overflow_msg = None
    if overflow_chunks and media_msg:
        from TechVJ.lang import get_string
        from core.entity_validator import utf16_length as u16len
        from pyrogram.types import MessageEntity
        header = get_string(media_msg.chat.id, "caption_continued") + "\n\n"
        header_len = u16len(header)
        for chunk in overflow_chunks:
            overflow_text = f"{header}{chunk.text}"
            # Adjust entity offsets for header
            adjusted = []
            for e in (chunk.entities or []):
                try:
                    new_e = MessageEntity(
                        type=e.type,
                        offset=e.offset + header_len,
                        length=e.length,
                        url=getattr(e, 'url', None),
                        user=getattr(e, 'user', None),
                        language=getattr(e, 'language', None),
                    )
                    adjusted.append(new_e)
                except Exception:
                    pass

            safe_entities = prepare_entities_for_send(overflow_text, adjusted) if adjusted else None
            overflow_msg = await send_text_entity_safe(client, chat_id, overflow_text, safe_entities)
    
    return media_msg, overflow_msg


# Telegram caption limit (Unicode characters)
TELEGRAM_CAPTION_LIMIT = 1024


def split_caption_safe(caption: str, limit: int = TELEGRAM_CAPTION_LIMIT) -> tuple:
    """
    Safely split caption for Telegram's 1024 character limit.
    
    Returns:
        (media_caption, overflow_text)
        - media_caption: First part (≤1024 chars) for media caption
        - overflow_text: Remaining text to send as separate message, or None
    
    Rules:
        - Preserves Unicode integrity (no broken emoji/UTF-8)
        - Tries to split at word boundary
        - Never loses any text
    """
    if not caption:
        return None, None
    
    # If within limit, return as-is
    if len(caption) <= limit:
        return caption, None
    
    logger.info(f"Caption exceeds {limit} chars ({len(caption)}), splitting...")
    
    # Find a good split point (word boundary, newline, or space)
    split_at = limit
    
    # Try to find a newline near the limit
    newline_pos = caption.rfind('\n', 0, limit)
    if newline_pos > limit - 200:  # Within last 200 chars
        split_at = newline_pos + 1
    else:
        # Try to find a space (word boundary)
        space_pos = caption.rfind(' ', 0, limit)
        if space_pos > limit - 100:  # Within last 100 chars
            split_at = space_pos + 1
        else:
            # Hard split at limit, but ensure we don't break Unicode
            # Python strings are Unicode-safe, but let's be careful
            split_at = limit
    
    media_caption = caption[:split_at].rstrip()
    overflow_text = caption[split_at:].lstrip()
    
    # Ensure media_caption doesn't exceed limit after trimming
    if len(media_caption) > limit:
        media_caption = media_caption[:limit]
    
    logger.debug(f"Split caption: {len(media_caption)} + {len(overflow_text)} chars")
    
    return media_caption, overflow_text if overflow_text else None


# Telegram message limit (UTF-8 characters)
TELEGRAM_MESSAGE_LIMIT = 4096


def find_hyperlink_boundaries(text: str) -> list:
    """
    Find all markdown hyperlink positions in text.
    Returns list of (start, end) tuples for each [text](url) pattern.
    """
    # Match markdown links: [text](url)
    pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    boundaries = []
    for match in re.finditer(pattern, text):
        boundaries.append((match.start(), match.end()))
    return boundaries


def safe_split_point(text: str, proposed_split: int, hyperlinks: list) -> int:
    """
    Adjust split point to not break hyperlinks.
    If proposed split is inside a hyperlink, move it before the hyperlink.
    """
    for start, end in hyperlinks:
        # If split point is inside a hyperlink
        if start < proposed_split < end:
            # Move split point to before the hyperlink
            # But first check if there's a good break point before it
            search_text = text[:start]
            
            # Try to find space before hyperlink
            space_pos = search_text.rfind(' ')
            if space_pos > start - 200:  # Within 200 chars before hyperlink
                return space_pos + 1
            
            # Try newline
            newline_pos = search_text.rfind('\n')
            if newline_pos > start - 200:
                return newline_pos + 1
            
            # Just split before the hyperlink
            return start
    
    return proposed_split


def split_message_safe(text: str, limit: int = TELEGRAM_MESSAGE_LIMIT) -> list:
    """
    Split long text into Telegram-compliant message chunks (max 4096 chars).
    
    Splitting priority:
    1. Split by paragraph (\\n\\n)
    2. If paragraph > limit → split by line (\\n)
    3. If line > limit → split at word boundary
    4. NEVER split inside hyperlinks - move whole hyperlink to next chunk
    
    Features:
    - UTF-8 safe (character-based, not byte-based)
    - Preserves hyperlinks intact (never breaks [text](url))
    - Preserves formatting, paragraphs, line breaks, emojis
    - No broken Unicode characters
    - No content loss
    
    Returns:
        List of message chunks, each ≤ limit characters
    """
    if not text:
        return []
    
    # If within limit, return as single chunk
    if len(text) <= limit:
        return [text]
    
    chunks = []
    remaining = text

    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        
        # Find best split point within limit
        split_at = limit
        
        # Priority 1: Try to split at paragraph boundary (\n\n)
        para_pos = remaining.rfind('\n\n', 0, limit)
        if para_pos > limit // 2:  # At least halfway through
            split_at = para_pos + 2  # Include the newlines
        else:
            # Priority 2: Try to split at line boundary (\n)
            line_pos = remaining.rfind('\n', 0, limit)
            if line_pos > limit // 2:
                split_at = line_pos + 1
            else:
                # Priority 3: Try to split at word boundary (space)
                space_pos = remaining.rfind(' ', 0, limit)
                if space_pos > limit - 500:  # Within last 500 chars
                    split_at = space_pos + 1
                else:
                    # Hard split at limit (last resort)
                    split_at = limit
        
        # CRITICAL: Adjust split point to not break hyperlinks
        # Recalculate hyperlinks for current remaining text
        current_hyperlinks = find_hyperlink_boundaries(remaining)
        split_at = safe_split_point(remaining, split_at, current_hyperlinks)
        
        # Safety check: ensure we make progress
        if split_at <= 0:
            split_at = min(limit, len(remaining))
        
        chunk = remaining[:split_at].rstrip()
        remaining = remaining[split_at:].lstrip()
        
        if chunk:
            chunks.append(chunk)
    
    logger.debug(f"Split message into {len(chunks)} chunks (hyperlink-safe)")
    return chunks


async def safe_send_message(
    client: Client,
    chat_id: int,
    text: str,
    parse_mode=None,
    entities=None,
    reply_parameters=None,
    reply_to_message_id=None,
    reply_markup=None,
    link_preview_options=None,
    disable_web_page_preview=None,
    delay_between: float = 0.5
) -> list:
    """
    Send message with automatic splitting for Telegram's 4096 char limit.
    
    Features:
    - Auto-splits long messages
    - Sends parts sequentially
    - FloodWait safe with retry
    - Preserves formatting and order
    - Returns list of sent messages
    - Compatible with both reply_parameters and reply_to_message_id
    - Supports both parse_mode (Markdown) and entities (MessageEntity)
    
    IMPORTANT: If entities are provided, uses entity-based splitting.
    If parse_mode is provided, uses Markdown-based splitting.
    Entity mode is preferred for reliability.
    
    Usage:
        await safe_send_message(client, chat_id, long_text)
        await safe_send_message(client, chat_id, text, entities=entities_list)
    """
    if not text:
        return []
    
    # If entities provided, use entity-based splitting (preferred)
    if entities:
        from core.text_renderer import extract_to_renderer
        renderer = extract_to_renderer(text, entities)
        chunks_with_entities = renderer.render_chunks(TELEGRAM_MESSAGE_LIMIT)
        
        sent_messages = []
        for i, (chunk_text, chunk_entities) in enumerate(chunks_with_entities):
            if not chunk_text:
                continue
            
            params = {
                'chat_id': chat_id,
                'text': chunk_text,
                'parse_mode': ParseMode.DISABLED,
            }
            
            if chunk_entities:
                params['entities'] = chunk_entities
            
            if i == 0:
                if reply_parameters:
                    params['reply_parameters'] = reply_parameters
                elif reply_to_message_id:
                    params['reply_to_message_id'] = reply_to_message_id
                if reply_markup:
                    params['reply_markup'] = reply_markup
            
            if link_preview_options:
                params['link_preview_options'] = link_preview_options
            elif disable_web_page_preview is not None:
                params['disable_web_page_preview'] = disable_web_page_preview
            
            for attempt in range(3):
                try:
                    msg = await client.send_message(**params)
                    sent_messages.append(msg)
                    break
                except FloodWait as e:
                    wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                    if attempt < 2:
                        await asyncio.sleep(min(wait_time, 60))
                    else:
                        raise
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(2)
                    else:
                        raise
            
            if i < len(chunks_with_entities) - 1:
                await asyncio.sleep(delay_between)
        
        return sent_messages
    
    # Fallback: Markdown-based splitting
    chunks = split_message_safe(text)
    sent_messages = []
    
    for i, chunk in enumerate(chunks):
        params = {
            'chat_id': chat_id,
            'text': chunk,
        }
        
        if parse_mode:
            params['parse_mode'] = parse_mode
        
        if i == 0:
            # Support both reply styles for compatibility
            if reply_parameters:
                params['reply_parameters'] = reply_parameters
            elif reply_to_message_id:
                params['reply_to_message_id'] = reply_to_message_id
            if reply_markup:
                params['reply_markup'] = reply_markup
        
        # Support both link preview styles
        if link_preview_options:
            params['link_preview_options'] = link_preview_options
        elif disable_web_page_preview is not None:
            params['disable_web_page_preview'] = disable_web_page_preview
        
        # Send with FloodWait protection
        for attempt in range(3):
            try:
                msg = await client.send_message(**params)
                sent_messages.append(msg)
                break
            except FloodWait as e:
                wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                logger.warning(f"FloodWait {wait_time}s on send_message")
                if attempt < 2:
                    await asyncio.sleep(min(wait_time, 60))
                else:
                    raise
            except Exception as e:
                logger.warning(f"send_message failed: {e}")
                if attempt < 2:
                    await asyncio.sleep(2)
                else:
                    raise
        
        # Delay between chunks to avoid rate limits
        if i < len(chunks) - 1:
            await asyncio.sleep(delay_between)
    
    return sent_messages


def extract_hyperlinks(text, entities):
    """
    Extract and format ALL hyperlinks and formatting from text with entities.
    
    CRITICAL FIX: Uses reverse order processing to avoid offset recalculation.
    This prevents duplicate text issues when formatting is applied.
    
    Supports: TEXT_LINK, URL, BOLD, ITALIC, CODE, PRE, UNDERLINE, STRIKETHROUGH
    """
    if not text or not entities:
        return text
    
    result = text
    
    # CRITICAL: Sort by offset DESCENDING (reverse order)
    # This way, modifications don't affect earlier offsets
    sorted_entities = sorted(
        entities, 
        key=lambda x: getattr(x, 'offset', 0), 
        reverse=True
    )
    
    # Track processed ranges to avoid overlapping entity issues
    processed_ranges = []
    
    for entity in sorted_entities:
        if not hasattr(entity, 'type'):
            continue
        
        entity_offset = getattr(entity, 'offset', 0)
        entity_length = getattr(entity, 'length', 0)
        
        if entity_length <= 0:
            continue
        
        # Use ORIGINAL offsets since we're processing in reverse
        start = entity_offset
        end = entity_offset + entity_length
        
        # Safety check
        text_len = len(result)
        if start < 0 or end > text_len or start >= end:
            continue
        
        # Check for overlapping with already processed entities
        is_overlapping = False
        for (p_start, p_end) in processed_ranges:
            if not (end <= p_start or start >= p_end):
                is_overlapping = True
                break
        
        if is_overlapping:
            continue
        
        entity_text = result[start:end]
        if not entity_text:
            continue
        
        formatted_text = None  # Only set if formatting needed
        
        # TEXT_LINK - hyperlink with custom text [text](url)
        if entity.type == MessageEntityType.TEXT_LINK and hasattr(entity, 'url'):
            url = entity.url
            if url:
                formatted_text = f'[{entity_text}]({url})'
        
        # URL - plain URL (already visible, no formatting needed)
        elif entity.type == MessageEntityType.URL:
            pass
        
        # MENTION - @username (already visible)
        elif entity.type == MessageEntityType.MENTION:
            pass
        
        # TEXT_MENTION - mention with user object
        elif entity.type == MessageEntityType.TEXT_MENTION and hasattr(entity, 'user'):
            if entity.user and entity.user.id:
                formatted_text = f'[{entity_text}](tg://user?id={entity.user.id})'
        
        # BOLD
        elif entity.type == MessageEntityType.BOLD:
            formatted_text = f'**{entity_text}**'
        
        # ITALIC
        elif entity.type == MessageEntityType.ITALIC:
            formatted_text = f'__{entity_text}__'
        
        # UNDERLINE (Markdown doesn't support)
        elif entity.type == MessageEntityType.UNDERLINE:
            pass
        
        # STRIKETHROUGH
        elif entity.type == MessageEntityType.STRIKETHROUGH:
            formatted_text = f'~~{entity_text}~~'
        
        # CODE (inline)
        elif entity.type == MessageEntityType.CODE:
            formatted_text = f'`{entity_text}`'
        
        # PRE (code block)
        elif entity.type == MessageEntityType.PRE:
            lang = getattr(entity, 'language', '') or ''
            if lang:
                formatted_text = f'```{lang}\n{entity_text}\n```'
            else:
                formatted_text = f'```\n{entity_text}\n```'
        
        # SPOILER
        elif entity.type == MessageEntityType.SPOILER:
            formatted_text = f'||{entity_text}||'
        
        # Apply formatting if needed
        if formatted_text is not None:
            result = result[:start] + formatted_text + result[end:]
            processed_ranges.append((start, end))
    
    return result


# DEPRECATED: Use create_user_session from session_handler.py instead
# This is kept for backwards compatibility with other modules
async def create_client_session(session_string, client_name="saverestricted"):
    """
    DEPRECATED: Use create_user_session context manager instead.
    This function is kept for backwards compatibility.
    """
    import uuid
    client = None
    for attempt in range(MAX_RETRIES):
        try:
            # Use validated fingerprint from config
            from config import get_client_params
            fp = get_client_params()
            client = Client(
                f"{client_name}_{uuid.uuid4().hex[:8]}", 
                session_string=session_string, 
                api_hash=API_HASH, 
                api_id=API_ID,
                no_updates=True,
                in_memory=True,
                sleep_threshold=60,
                max_concurrent_transmissions=20,
                device_model=fp['device_model'],
                system_version=fp['system_version'],
                app_version=fp['app_version'],
                lang_code=fp['lang_code'],
                workers=16
            )
            await client.connect()
            return client, None
        except Exception as e:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            error_str = str(e).lower()
            if "connection" in error_str or "network" in error_str or "timeout" in error_str:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
            return None, f"Failed to establish connection: {str(e)}"
    return None, "Connection failed after multiple attempts"


async def safe_disconnect(client):
    """Safely disconnect client with timeout"""
    if client:
        try:
            if hasattr(client, "no_updates"):
                client.no_updates = True
            if client.is_connected:
                await asyncio.wait_for(client.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass


def extract_quizbot_links(reply_markup):
    """Extract QuizBot start links from inline keyboard buttons."""
    quizbot_links = []
    if reply_markup and hasattr(reply_markup, 'inline_keyboard'):
        for row in reply_markup.inline_keyboard:
            for button in row:
                if hasattr(button, 'url') and button.url:
                    url_lower = button.url.lower()
                    if 'quizbot' in url_lower and 'start=' in url_lower:
                        quizbot_links.append(button.url)
    return quizbot_links


# ==================== ACCESS CONTROL ====================

def owner_only(func):
    """Decorator to restrict command to owner only"""
    async def wrapper(client: Client, message: Message):
        if not message.from_user or message.from_user.id != OWNER_ID:
            return  # Silent return — non-owner uchun hech narsa ko'rsatmaymiz
        return await func(client, message)
    return wrapper


def check_banned(func):
    """Decorator to check if user is banned before processing"""
    async def wrapper(client: Client, message: Message):
        user_id = message.from_user.id
        if user_id == OWNER_ID:
            return await func(client, message)
        if await async_db.is_banned(user_id):
            await message.reply(BANNED_MESSAGE)
            return
        return await func(client, message)
    return wrapper


def check_banned_callback(func):
    """Decorator for callback queries to check if user is banned"""
    async def wrapper(client: Client, callback_query):
        user_id = callback_query.from_user.id
        if user_id == OWNER_ID:
            return await func(client, callback_query)
        if await async_db.is_banned(user_id):
            await callback_query.answer(BANNED_MESSAGE, show_alert=True)
            return
        return await func(client, callback_query)
    return wrapper


# ==================== PROGRESS TRACKING ====================
#
# REFACTORED: Progress tracking is now handled by progress_controller.py
# The old file-based system (writing to {msg_id}{type}status.txt) has been
# replaced with an async-safe, in-memory system that:
# - Runs on the same event loop as download/upload
# - Throttles updates to minimum 1.5s intervals
# - Handles FloodWait gracefully
# - Supports clean cancellation
#
# For backwards compatibility, these function stubs are provided.
# New code should use transfer_manager directly.
#

def create_progress_callback(client: Client, status_message: Message, transfer_type: str):
    """
    Create an async-safe progress callback for Pyrogram download/upload.
    
    Uses core.downloader.progress_tracker with thread-safe scheduling.
    
    Args:
        client: Pyrogram client for message edits
        status_message: Message to update with progress
        transfer_type: "download" or "upload"
    
    Returns:
        Callback function compatible with Pyrogram progress parameter
    
    Usage:
        progress_cb = create_progress_callback(bot, status_msg, "download")
        await user_client.download_media(message, progress=progress_cb)
    """
    transfer_id = f"{status_message.id}_{transfer_type}_{time.time()}"
    return progress_tracker.create_callback(
        client=client,
        status_message=status_message,
        transfer_type=transfer_type,
        transfer_id=transfer_id
    )


def progress(current, total, message, type):
    """
    DEPRECATED: Legacy progress function - kept for compatibility.
    
    This function previously wrote to status files. It now does nothing
    as progress is handled by progress_controller with proper throttling.
    
    Use create_progress_callback() instead for new code.
    """
    # No-op: Progress is now handled by progress_controller
    # which is integrated directly with Pyrogram's progress callback
    pass


async def downstatus(client: Client, statusfile, message):
    """
    DEPRECATED: Legacy download status polling function.
    
    This used to poll a status file for progress updates.
    Now progress updates are pushed directly via progress_controller.
    
    This function is kept for compatibility but does nothing.
    """
    # No-op: Progress controller handles updates automatically
    pass


async def upstatus(client: Client, statusfile, message):
    """
    DEPRECATED: Legacy upload status polling function.
    
    This used to poll a status file for progress updates.
    Now progress updates are pushed directly via progress_controller.
    
    This function is kept for compatibility but does nothing.
    """
    # No-op: Progress controller handles updates automatically
    pass


# ==================== OWNER COMMANDS ====================
# NOTE: /ban va /unban handlerlar owner_commands.py da (governance role_manager).
# Bu yerda faqat /banlist qoldirildi (async_db based).

@Client.on_message(filters.command(["banlist"]))
@owner_only
async def ban_list_command(client: Client, message: Message):
    """List banned users - Owner only"""
    banned_users = await async_db.get_all_banned()
    if not banned_users:
        await message.reply("No users are banned.")
        return
    text = "**Banned Users:**\n"
    for user in banned_users:
        text += f"• `{user['user_id']}`\n"
    await message.reply(text)


# ==================== CHANNEL MONITOR ====================

_channel_monitor_locks = {}
_channel_monitor_access_failures = {}
_CHANNEL_MONITOR_ACCESS_FAIL_LIMIT = 3


class ChannelMonitorAccessLost(Exception):
    """Raised when the system session no longer has access to a monitored channel."""


def _is_channel_monitor_access_error(error: Any) -> bool:
    text = f"{type(error).__name__}: {error}".upper()
    markers = (
        "USER_BANNED_IN_CHANNEL",
        "CHANNEL_PRIVATE",
        "CHAT_ADMIN_REQUIRED",
        "CHATADMINREQUIRED",
        "USER_NOT_PARTICIPANT",
        "PEER_ID_INVALID",
    )
    return any(marker in text for marker in markers)


def _record_channel_monitor_success(channel_id: int) -> None:
    _channel_monitor_access_failures.pop(int(channel_id), None)


async def _record_channel_monitor_access_failure(
    client: Client,
    channel_monitor,
    channel_id: int,
    target_chat_id: int,
    error: Any,
) -> None:
    channel_id = int(channel_id)
    count = int(_channel_monitor_access_failures.get(channel_id, 0)) + 1
    _channel_monitor_access_failures[channel_id] = count

    logger.warning(
        "ChannelMonitor: access failure %s/%s for channel=%s target=%s: %s",
        count,
        _CHANNEL_MONITOR_ACCESS_FAIL_LIMIT,
        channel_id,
        target_chat_id,
        error,
    )

    if count > _CHANNEL_MONITOR_ACCESS_FAIL_LIMIT:
        return

    if count < _CHANNEL_MONITOR_ACCESS_FAIL_LIMIT:
        return

    disabled = False
    try:
        disabled = bool(await channel_monitor.toggle_channel(channel_id, False))
    except Exception as toggle_err:
        logger.warning(
            "ChannelMonitor: auto-disable failed for channel=%s: %s",
            channel_id,
            toggle_err,
        )

    if disabled:
        _channel_monitor_access_failures.pop(channel_id, None)

    if OWNER_ID:
        try:
            await client.send_message(
                OWNER_ID,
                "Kanalga kirish taqiqlandi, monitor avtomatik o'chirildi.\n"
                f"Channel ID: {channel_id}\n"
                f"Target chat: {target_chat_id}\n"
                f"Ketma-ket xatolik: {count}\n"
                f"Sabab: {_owner_diag_error(error)}",
                parse_mode=ParseMode.DISABLED,
                **build_link_preview_kwargs(is_disabled=True),
            )
        except Exception as notify_err:
            logger.debug("ChannelMonitor: owner auto-disable notify failed: %s", notify_err)


def _channel_monitor_lock(key) -> asyncio.Lock:
    lock = _channel_monitor_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _channel_monitor_locks[key] = lock
    return lock


async def _send_monitored_text_message(
    client: Client,
    target_chat_id: int,
    source_msg: Message,
) -> bool:
    text, entities = get_text_with_entities(source_msg)
    if not text:
        text, entities = get_caption_with_entities(source_msg)
    if not text:
        return True

    try:
        from core.text_renderer import extract_to_renderer

        renderer = extract_to_renderer(text, entities or [])
        chunks = renderer.render_chunks(TELEGRAM_MESSAGE_LIMIT)
        reply_markup = getattr(source_msg, "reply_markup", None)

        for index, (chunk_text, chunk_entities) in enumerate(chunks):
            if not chunk_text:
                continue
            await client.send_message(
                chat_id=target_chat_id,
                text=chunk_text,
                entities=chunk_entities if chunk_entities else None,
                parse_mode=ParseMode.DISABLED,
                reply_markup=reply_markup if index == len(chunks) - 1 else None,
                **build_link_preview_kwargs(is_disabled=True),
            )
            if len(chunks) > 1:
                await asyncio.sleep(0.5)
        return True
    except FloodWait:
        raise
    except Exception as err:
        logger.warning("ChannelMonitor: text delivery failed: %s", err)
        return False


async def _process_monitored_channel_message(
    client: Client,
    monitor_entry,
    source_message: Message,
) -> bool:
    """
    Process a new post from a monitored protected channel.

    Media is fetched with a MTProto session and text is delivered by the bot,
    preserving the project's send-routing rule.
    """
    source_chat_id = int(monitor_entry.channel_id)
    target_chat_id = int(monitor_entry.target_chat_id)
    source_message_id = int(source_message.id)

    async def _handle_access_lost(error: Any) -> None:
        try:
            from core.channel_monitor import channel_monitor as _cm
            await _record_channel_monitor_access_failure(
                client,
                _cm,
                source_chat_id,
                target_chat_id,
                error,
            )
        except Exception as disable_err:
            logger.warning(
                "ChannelMonitor: access lost handling failed channel=%s: %s",
                source_chat_id,
                disable_err,
            )

    ctx = _task_context_for_channel_monitor(
        source_chat_id=source_chat_id,
        source_message_id=source_message_id,
        target_chat_id=target_chat_id,
    )

    if _REQUEST_CONTEXT_AVAILABLE:
        _set_req_ctx(_ReqCtx(
            request_id=ctx.task_id[:12],
            requester_user_id=OWNER_ID,
            source_chat_id=source_chat_id,
            target_chat_id=target_chat_id,
            routing_mode="channel_monitor",
            source_message_id=source_message_id,
            sender_mode="user_session",
        ))

    try:
        from core.media_classifier import has_downloadable_media

        if not has_downloadable_media(source_message) and not getattr(source_message, "poll", None):
            return await _send_monitored_text_message(client, target_chat_id, source_message)

        session_string = None
        session_user_id = target_chat_id
        try:
            from core.session_manager import session_manager as _sm_inst
            if _sm_inst._initialized:
                _sys_rec = _sm_inst.get_system_session_for_use()
                if _sys_rec:
                    session_string = _sys_rec.session_string
                    session_user_id = _sys_rec.owner_user_id or OWNER_ID
        except Exception as _sm_lookup_err:
            logger.debug("ChannelMonitor: system session lookup failed: %s", _sm_lookup_err)

        if not session_string:
            try:
                from core.premium_logic import get_system_session
                _legacy_system = get_system_session()
                if _legacy_system:
                    session_string = _legacy_system.session_string
                    session_user_id = _legacy_system.user_id or OWNER_ID
            except Exception as _legacy_lookup_err:
                logger.debug("ChannelMonitor: legacy system session lookup failed: %s", _legacy_lookup_err)

        if not session_string:
            owner_data = await async_db.find_user(OWNER_ID)
            if owner_data and owner_data.get("logged_in") and owner_data.get("session"):
                session_string = owner_data["session"]
                session_user_id = OWNER_ID

        if not session_string:
            logger.warning(
                "ChannelMonitor: no MTProto session available for channel %s post %s",
                source_chat_id, source_message_id,
            )
            return False

        async with StopSafePipeline(target_chat_id, task_manager) as pipeline:
            temp_dir = await pipeline.get_temp_dir()
            async with create_user_session(
                session_string,
                session_user_id,
                peers_to_resolve=[source_chat_id],
            ) as acc:
                await _resolve_peer_safe(acc, source_chat_id, ctx)
                try:
                    probe_msg = await asyncio.wait_for(
                        acc.get_messages(source_chat_id, source_message_id),
                        timeout=15.0,
                    )
                    if not probe_msg or getattr(probe_msg, "empty", False):
                        logger.info(
                            "ChannelMonitor: source message missing channel=%s post=%s",
                            source_chat_id,
                            source_message_id,
                        )
                        return False
                except Exception as access_err:
                    if _is_channel_monitor_access_error(access_err):
                        raise ChannelMonitorAccessLost(str(access_err)) from access_err
                    raise

                try:
                    if getattr(source_message, "media_group_id", None):
                        album_msgs = await acc.get_media_group(source_chat_id, source_message_id)
                        if album_msgs:
                            album_msgs = sorted(list(album_msgs), key=lambda m: m.id)
                            if all(getattr(m, "photo", None) for m in album_msgs) and len(album_msgs) >= 2:
                                album_last_id = album_msgs[-1].id
                                success, status, _ = await process_album_with_session(
                                    bot_client=client,
                                    user_session=acc,
                                    user_id=target_chat_id,
                                    target_chat_id=target_chat_id,
                                    source_chat_id=source_chat_id,
                                    message_id=source_message_id,
                                    reply_to_message_id=None,
                                    check_cancelled=pipeline.check_cancelled,
                                    sent_albums=None,
                                )
                                if success or status == "already_sent":
                                    try:
                                        from core.channel_monitor import channel_monitor as _cm
                                        await _cm.update_last_forwarded(source_chat_id, album_last_id)
                                    except Exception:
                                        pass
                                    return True
                            else:
                                processed_any = False
                                album_last_id = source_message_id
                                for album_msg in album_msgs:
                                    if await process_single_post(
                                        client,
                                        acc,
                                        None,
                                        source_chat_id,
                                        album_msg.id,
                                        temp_dir,
                                        pipeline,
                                        target_user_id=target_chat_id,
                                        session_string=session_string,
                                        context=ctx,
                                    ):
                                        processed_any = True
                                    album_last_id = max(album_last_id, int(getattr(album_msg, "id", album_last_id)))
                                if processed_any:
                                    try:
                                        from core.channel_monitor import channel_monitor as _cm
                                        await _cm.update_last_forwarded(source_chat_id, album_last_id)
                                    except Exception:
                                        pass
                                return processed_any
                except Exception as album_err:
                    if _is_channel_monitor_access_error(album_err):
                        raise ChannelMonitorAccessLost(str(album_err)) from album_err
                    logger.debug("ChannelMonitor: album fallback to single post: %s", album_err)

                return bool(await process_single_post(
                    client,
                    acc,
                    None,
                    source_chat_id,
                    source_message_id,
                    temp_dir,
                    pipeline,
                    target_user_id=target_chat_id,
                    session_string=session_string,
                    context=ctx,
                ))

    except FloodWait as wait_err:
        wait_seconds = getattr(wait_err, "value", getattr(wait_err, "x", 30))
        logger.warning(
            "ChannelMonitor: FloodWait %ss on channel %s post %s",
            wait_seconds, source_chat_id, source_message_id,
        )
        await asyncio.sleep(min(wait_seconds, 60))
        return False
    except ChannelMonitorAccessLost as access_lost:
        await _handle_access_lost(access_lost)
        raise
    except Exception as err:
        if _is_channel_monitor_access_error(err):
            await _handle_access_lost(err)
            raise ChannelMonitorAccessLost(str(err)) from err
        logger.warning(
            "ChannelMonitor: failed channel=%s post=%s target=%s: %s",
            source_chat_id, source_message_id, target_chat_id, err,
        )
        return False
    finally:
        if _REQUEST_CONTEXT_AVAILABLE:
            _clear_req_ctx()


@Client.on_message(filters.channel)
@track_activity
async def monitored_channel_handler(client: Client, message: Message):
    """Deliver posts from owner-configured protected channels."""
    try:
        from core.channel_monitor import channel_monitor
    except Exception:
        return

    if getattr(message, "outgoing", False):
        return

    chat_id = getattr(getattr(message, "chat", None), "id", None)
    if chat_id is None:
        return

    monitor_entry = channel_monitor.get_channel(int(chat_id))
    if not monitor_entry or not monitor_entry.enabled:
        return

    message_id = getattr(message, "id", None)
    if not message_id or message_id <= getattr(monitor_entry, "last_forwarded_id", 0):
        return

    lock_key = (int(chat_id), getattr(message, "media_group_id", None) or int(message_id))
    async with _channel_monitor_lock(lock_key):
        latest = channel_monitor.get_channel(int(chat_id))
        if not latest or not latest.enabled:
            return
        if int(message_id) <= latest.last_forwarded_id:
            return

        logger.info(
            "ChannelMonitor: processing channel=%s post=%s target=%s",
            chat_id, message_id, latest.target_chat_id,
        )
        try:
            ok = await _process_monitored_channel_message(client, latest, message)
        except ChannelMonitorAccessLost as err:
            logger.warning("ChannelMonitor: auto-disabled/access-lost channel=%s: %s", chat_id, err)
            return
        if ok:
            _record_channel_monitor_success(int(chat_id))
            await channel_monitor.update_last_forwarded(int(chat_id), int(message_id))


# ==================== STOP COMMAND (SECTION C) ====================

@Client.on_message(filters.command(["stop"]))
@check_banned
@track_activity
async def stop_command(client: Client, message: Message):
    """
    Cancel ALL ongoing operations for the user.
    Immediately halts: downloads, uploads, loops, queued tasks, albums.
    
    Integrates with:
    - StopSafePipeline
    - task_manager
    - DownloadEngine worker pool
    - User download queue
    """
    user_id = message.from_user.id
    active_tasks = await task_manager.get_active_task_count(user_id)

    # FIRST: Cancel downloads in the engine worker pool
    # This must happen BEFORE temp cleanup, otherwise the running
    # download hits FileNotFoundError and enters retry loop
    engine_cancelled = 0
    try:
        engine = await get_engine()
        engine_cancelled = await engine.cancel_user_downloads(user_id)
    except Exception as e:
        logger.warning(f"Error cancelling engine downloads: {e}")

    # THEN: Cancel tasks in task_manager (which also cleans temp dirs)
    cancelled_count = await task_manager.cancel_all_tasks(user_id)

    # Clear user's download queue
    queue_cleared = await user_queue.clear_queue(user_id)
    
    total_cancelled = cancelled_count + engine_cancelled + queue_cleared
    
    if total_cancelled == 0 and active_tasks == 0:
        await message.reply("❌ Bekor qilinadigan faol vazifa yo'q.")
        return
    
    await message.reply(
        f"**✅ Barcha operatsiyalar to'xtatildi**\n\n"
        f"📋 Navbatdan o'chirildi: {queue_cleared}\n"
        f"⚙️ Pipeline tasks: {cancelled_count}\n"
        f"📥 Download workers: {engine_cancelled}\n\n"
        f"Barcha yuklanishlar va navbat tozalandi."
    )


@Client.on_message(filters.command(["queue"]))
@check_banned
@track_activity
async def queue_status_command(client: Client, message: Message):
    """Show user's queue status"""
    user_id = message.from_user.id
    status = await user_queue.get_status(user_id)
    await message.reply(format_queue_status(status))


# ==================== BASIC COMMANDS ====================

@Client.on_message(filters.command(["start"]))
@check_banned
@track_activity
async def send_start(client: Client, message: Message):
    user_id = message.from_user.id
    user_data = await async_db.find_user(user_id)
    is_logged = bool(user_data and user_data.get('logged_in') and user_data.get('session'))

    login_status = "✅ Tizimga kirilgan" if is_logged else "❌ Kirilmagan — /login bilan kiring"

    role_line = ""
    if _GOVERNANCE_AVAILABLE:
        try:
            _role = await _role_manager.get_role(user_id)
            role_emoji = {"vip_user": "⭐", "normal_user": "👤", "new_user": "🆕"}.get(_role.value, "❓")
            role_line = f"\n🎭 <b>Rolingiz:</b> {role_emoji} <code>{_role.value}</code>"
        except Exception:
            pass

    await client.send_message(
        message.chat.id,
        f"<b>👋 Salom, {message.from_user.mention}!</b>\n\n"
        f"🤖 <b>Cheklangan kontent saqlash boti</b>\n\n"
        f"<b>💼 Holat:</b> {login_status}{role_line}\n\n"
        f"<b>📎 Havola formati:</b>\n"
        f"<code>https://t.me/c/CHANNEL_ID/POST</code>\n"
        f"<code>https://t.me/c/CHANNEL_ID/101-200</code> (diapazon)\n"
        f"<code>https://t.me/c/CHANNEL_ID/101,102,103</code> (ro'yxat)\n"
        f"<code>https://t.me/username/POST</code> (ochiq kanal)\n\n"
        f"<b>📋 Asosiy buyruqlar:</b>\n"
        f"  /login — Telegram hisobingizni bog'lash\n"
        f"  /logout — Hisobdan chiqish\n"
        f"  /status — Batafsil holat (profil, limitlar)\n"
        f"  /stop — Barcha operatsiyalarni to'xtatish\n"
        f"  /help — To'liq yordam\n\n"
        f"<i>Bot ishlatish uchun Telegram hisobingizni /login orqali bog'lang.</i>"
    )


@Client.on_message(filters.command(["help"]))
@check_banned
@track_activity
async def send_help(client: Client, message: Message):
    from TechVJ.strings import HELP_TXT
    await client.send_message(message.chat.id, HELP_TXT)


@Client.on_message(filters.command(["status"]))
@check_banned
@track_activity
async def send_status(client: Client, message: Message):
    """Foydalanuvchiga o'z holatini — rol, sessiya, limitlarni ko'rsatadigan buyruq."""
    user_id = message.from_user.id

    # Session holati
    user_data = await async_db.find_user(user_id)
    is_logged = bool(user_data and user_data.get('logged_in') and user_data.get('session'))
    session_text = "✅ Faol" if is_logged else "❌ Yo'q"

    # Bog'liq profil (get_me dan)
    profile_text = ""
    if is_logged and user_data.get('session'):
        try:
            from TechVJ.session_handler import create_user_session
            async with create_user_session(user_data['session'], user_id) as acc:
                me = await acc.get_me()
                name = f"{getattr(me, 'first_name', '')} {getattr(me, 'last_name', '') or ''}".strip()
                uname = f"@{me.username}" if getattr(me, 'username', None) else "yo'q"
                premium = "⭐ Premium" if getattr(me, 'is_premium', False) else "Oddiy"
                profile_text = (
                    f"\n\n👤 <b>Bog'langan profil:</b>\n"
                    f"  Ism: <b>{name}</b>\n"
                    f"  Username: <code>{uname}</code>\n"
                    f"  ID: <code>{me.id}</code>\n"
                    f"  Turi: {premium}"
                )
        except Exception as _pe:
            profile_text = f"\n\n⚠️ Profilni olishda xato: <code>{str(_pe)[:60]}</code>"

    # Rol va limitlar
    role_text = ""
    if _GOVERNANCE_AVAILABLE:
        try:
            _role = await _role_manager.get_role(user_id)
            role_emoji = {"vip_user": "⭐", "normal_user": "👤", "new_user": "🆕", "banned_user": "🚫"}.get(
                _role.value, "❓"
            )
            max_req, window = _rate_limiter.get_limit(_role)
            multi_link = "✅" if _role.value not in ("new_user",) else "❌ (faqat 1 ID)"
            role_text = (
                f"\n\n🎭 <b>Ro'l:</b> {role_emoji} <code>{_role.value}</code>\n"
                f"  📊 Rate limit: <code>{max_req} so'rov / {window}s</code>\n"
                f"  🔗 Ko'p ID/diapazon: {multi_link}"
            )
        except Exception as _re:
            logger.debug("status cmd role error: %s", _re)


    # Queue holati
    queue_info = ""
    try:
        q_status = await user_queue.get_status(user_id)
        if q_status.get('active'):
            queue_info = f"\n\n⚙️ <b>Navbat:</b> Faol download mavjud"
        elif q_status.get('queued', 0) > 0:
            queue_info = f"\n\n⏳ <b>Navbat:</b> {q_status['queued']} ta kutmoqda"
    except Exception:
        pass

    await message.reply(
        f"📊 <b>Sizning holatingiz</b>\n\n"
        f"🔐 <b>Sessiya:</b> {session_text}"
        f"{profile_text}"
        f"{role_text}"
        f"{queue_info}\n\n"
        f"ℹ️ Yordam uchun /help buyrug'ini yuboring.",
        disable_web_page_preview=True
    )


@Client.on_message(filters.command(["cancel"]))
@check_banned
@track_activity
async def cancel_command(client: Client, message: Message):
    """Redirect to /stop - cancels all operations"""
    user_id = message.chat.id

    # FIRST: Cancel engine downloads (before temp cleanup)
    engine_cancelled = 0
    try:
        engine = await get_engine()
        engine_cancelled = await engine.cancel_user_downloads(user_id)
    except Exception as e:
        logger.debug("Engine cancel failed for user %s: %s", user_id, e)

    # THEN: Cancel tasks + cleanup temp dirs
    cancelled = await task_manager.cancel_all_tasks(user_id)

    total = cancelled + engine_cancelled
    if total > 0:
        await message.reply(f"Cancelled {total} tasks. Use /stop in the future.")
    else:
        await message.reply("No active tasks. Use /stop to cancel operations.")


# ==================== /comment COMMAND ====================

@Client.on_message(filters.command(["comment", "comments"]))
@check_banned
@track_activity
async def comment_analyzer_command(client: Client, message: Message):
    """
    Analyze comment section of a post.
    
    Usage: /comment https://t.me/c/123456789/108
    
    Returns:
    - Post ID
    - First comment ID
    - Last comment ID
    - Total comment count
    """
    user_id = message.chat.id
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "**Usage:** `/comment <post_link>`\n\n"
            "**Example:**\n"
            "`/comment https://t.me/c/123456789/108`\n\n"
            "Analyzes the comment section of a post.",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    url = parts[1].strip()
    
    # Parse URL
    parsed, error = parse_telegram_url(url)
    if error or not parsed:
        await message.reply(f"Invalid URL: {error or 'Could not parse'}")
        return
    
    # Check user session
    user_data = await async_db.find_user(user_id)
    if not get(user_data, 'logged_in', False) or not user_data.get('session'):
        await message.reply(
            strings['need_login'],
            **build_reply_kwargs_from_message(message)
        )
        return
    
    status_msg = await message.reply("Analyzing comments...")
    
    try:
        async with create_user_session(
            user_data['session'], user_id,
            peers_to_resolve=[parsed.channel_id],
        ) as acc:
            channel_id = parsed.channel_id
            post_id = parsed.post_ids[0] if parsed.post_ids else None
            
            if not post_id:
                await status_msg.edit_text("No post ID found in URL.")
                return
            
            # Verify post exists
            try:
                post_msg = await acc.get_messages(channel_id, post_id)
                if not post_msg or post_msg.empty:
                    await status_msg.edit_text("Post not found or deleted.")
                    return
            except Exception as e:
                await status_msg.edit_text(f"Cannot access post: {e}")
                return
            
            # Fetch comments - collect IDs and extract discussion group info
            comment_ids = []
            total_comments = 0
            discussion_group_id = None
            thread_id = None  # Will be extracted from first comment
            
            try:
                async for comment in acc.get_discussion_replies(channel_id, post_id):
                    if comment and not comment.empty:
                        comment_ids.append(comment.id)
                        total_comments += 1
                        
                        # Extract discussion group ID from the comment's chat
                        # Comments belong to the DISCUSSION GROUP, not the channel
                        if discussion_group_id is None and comment.chat:
                            discussion_group_id = comment.chat.id
                        
                        # Extract thread_id from the reply_to_message_id
                        # thread_id = the original post ID in the discussion
                        if thread_id is None:
                            if hasattr(comment, 'reply_to_message_id') and comment.reply_to_message_id:
                                thread_id = comment.reply_to_message_id
                            elif hasattr(comment, 'reply_to_top_message_id') and comment.reply_to_top_message_id:
                                thread_id = comment.reply_to_top_message_id
                        
                        # Safety limit
                        if total_comments >= 10000:
                            break
            except Exception as e:
                error_str = str(e).upper()
                if "MSG_ID_INVALID" in error_str:
                    await status_msg.edit_text(
                        "**Comments Disabled**\n\n"
                        "This post does not have comments enabled or is not a discussion post."
                    )
                    return
                else:
                    await status_msg.edit_text(f"Error fetching comments: {e}")
                    return
            
            if total_comments == 0 or not comment_ids:
                await status_msg.edit_text(
                    f"**Comment Analysis**\n\n"
                    f"**Post ID:** `{post_id}`\n"
                    f"**Comments:** None found\n\n"
                    f"This post has no comments yet."
                )
            else:
                # Normalize IDs - first is ALWAYS min, last is ALWAYS max
                first_comment_id = min(comment_ids)
                last_comment_id = max(comment_ids)
                
                # Use discussion group ID (where comments live)
                # If not detected, fall back to channel_id (shouldn't happen)
                group_id = discussion_group_id or channel_id
                group_id_short = str(group_id)[4:] if str(group_id).startswith('-100') else str(abs(group_id))
                
                # thread_id is the original post ID in the discussion group
                # If not extracted from reply_to, it's likely the forwarded post ID
                if thread_id is None:
                    # Fallback: thread_id might need manual detection
                    # This shouldn't happen if get_discussion_replies works correctly
                    thread_id = post_id
                    logger.warning(f"Could not extract thread_id from comments, using post_id: {post_id}")
                
                await status_msg.edit_text(
                    f"**Comment Analysis**\n\n"
                    f"**Post ID:** `{post_id}`\n"
                    f"**Thread ID:** `{thread_id}`\n"
                    f"**Discussion Group:** `{group_id}`\n"
                    f"**First Comment ID:** `{first_comment_id}`\n"
                    f"**Last Comment ID:** `{last_comment_id}`\n"
                    f"**Total Comments:** {total_comments}\n\n"
                    f"**First Comment Link:**\n"
                    f"`https://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}`\n\n"
                    f"**Range Link:**\n"
                    f"`https://t.me/c/{group_id_short}/{first_comment_id}?thread={thread_id}&range={first_comment_id}-{last_comment_id}`"
                )
                
    except SessionInvalidError as _sie1:
        await _handle_session_invalid(client, user_id, _sie1)
        if status_msg:
            try:
                await status_msg.edit_text("Session invalid. /login bilan qayta kiring.")
            except Exception:
                pass
    except SessionConnectionError as e:
        await status_msg.edit_text(f"Connection error: {e}")
    except Exception as e:
        await status_msg.edit_text(f"Error: {e}")


# NOTE: /session handler session_manager_commands.py da (to'liq pool boshqaruv).


async def _dispatch_with_priority(
    client: Client,
    message: Message,
    parsed,
    handler,
    context: TaskContext,
    role=None,
):
    """Dispatch a processing task through PriorityQueue when available."""
    if _GOVERNANCE_AVAILABLE:
        try:
            from core.priority_queue import priority_queue as _pq, PriorityJob
            if getattr(_pq, "_started", False):
                _role = role or _UserRole.NEW_USER

                async def _job_handler(c, m, p):
                    return await handler(c, m, p, context=context)

                job = PriorityJob(
                    user_id=context.user_id,
                    role=_role,
                    parsed_url=parsed,
                    message=message,
                    handler=_job_handler,
                    client=client,
                )
                fut = await _pq.enqueue(job)
                return await fut
        except Exception as e:
            logger.warning(
                "PriorityQueue dispatch failed (task=%s): %s",
                context.task_id,
                e,
            )
    return await handler(client, message, parsed, context=context)


# ==================== MAIN MESSAGE HANDLER ====================

@Client.on_message(filters.text & filters.private & ~filters.me & ~filters.command([
    "start", "help", "cancel", "stop", "info", "ban", "unban", "banlist",
    "login", "logout", "qrlogin", "status", "chatinfo", "msgstats", "members",
    "comment", "comments", "session", "post", "group", "queue", "lang", "clean", "cleanstatus",
    "setrole", "userinfo", "set_rate_limit", "set_parallel_limit", "queue_status",
    "stats", "maintenance", "ownerhelp", "sessionupdate", "sessionremove",
    "enable_global_sessions", "disable_global_sessions",
    "premium", "setpremium", "removepremium", "premiumstatus", "checkpremium",
    "add_premium_session", "remove_premium_session",
    "addchannel", "removechannel", "channels", "togglechannel", "grab", "failed_logs",
    "uploadsetting", "split_media",
]))

@check_banned
@track_activity
async def save(client: Client, message: Message):
    """Main handler for Telegram URLs with queue support"""
    try:
        url = message.text.strip()
        
        if "https://t.me/" not in url and "http://t.me/" not in url:
            return  # Not a Telegram URL, ignore
        
        # Parse the URL
        parsed, error = parse_telegram_url(url)
        
        if error:
            await message.reply(error)
            return
        
        if not parsed:
            await message.reply("Could not parse URL. Please check the format.")
            return
        
        # Validate post IDs exist
        if not parsed.post_ids:
            await message.reply("No post IDs found in URL.")
            return
        
        user_id = message.from_user.id
        _restricted_ok, _restricted_msg = await _validate_restricted_channel_request(
            client,
            parsed,
            user_id,
        )
        if not _restricted_ok:
            await message.reply(
                _restricted_msg,
                parse_mode=ParseMode.DISABLED,
                **build_link_preview_kwargs(is_disabled=True),
            )
            return

        base_ctx = _new_task_context(message, parsed, client_type="bot")
        _gov_role = None

        # ── Request-scoped structured logging ──────────────────────────────────
        if _REQUEST_CONTEXT_AVAILABLE:
            _set_req_ctx(_ReqCtx(
                request_id=base_ctx.task_id[:12],
                requester_user_id=user_id,
                source_chat_id=getattr(parsed, "channel_id", None) if isinstance(getattr(parsed, "channel_id", None), int) else None,
                target_chat_id=base_ctx.target_chat_id,
                routing_mode=parsed.url_type,
                topic_id=getattr(parsed, "topic_id", None),
                message_thread_id=getattr(parsed, "thread_id", None),
                source_message_id=getattr(message, "id", None),
                sender_mode="bot",
            ))

        logger.info(
            "Task start id=%s user=%d type=%s source=%s",
            base_ctx.task_id,
            base_ctx.user_id,
            parsed.url_type,
            parsed.channel_id,
        )

        # ── Governance middleware ──────────────────────────────────────────────
        if _GOVERNANCE_AVAILABLE:
            # Maintenance mode check
            if _is_maintenance() and user_id != OWNER_ID:
                await message.reply("🔧 Bot hozir texnik xizmatda. Keyinroq urinib ko'ring.")
                return

            _gov_role = await _role_manager.get_role(user_id)

            # Banned check
            if _gov_role == _UserRole.BANNED:
                return

            # Rate limit check
            _gov_allowed, _gov_retry = _rate_limiter.check(user_id, _gov_role)
            if not _gov_allowed:
                await message.reply(f"⏳ {_gov_retry} soniyadan keyin qayta urinib ko'ring.")
                return

            # Permission guard (role-based link validation)
            _gov_ok, _gov_err_msg = _permission_guard.validate(parsed, _gov_role)
            if not _gov_ok:
                await message.reply(_gov_err_msg)
                return
        # ── End governance middleware ──────────────────────────────────────────

        # ==================== QUEUE CHECK ====================
        # Check if user already has an active download
        can_start, position, status_msg = await check_and_queue(
            user_id=user_id,
            link=url,
            parsed_data=parsed,
            message_id=message.id,
            chat_id=message.chat.id
        )
        
        if not can_start:
            # Queued or rejected - send localized status message
            from TechVJ.lang import get_string
            if "to'lgan" in status_msg or "full" in status_msg.lower():
                localized = get_string(user_id, "queue_full", max=10)
            else:
                # Extract position from status
                localized = status_msg  # fallback to original
            await message.reply(localized, **build_link_preview_kwargs(is_disabled=True))
            return
        
        # ==================== PROCESS DOWNLOAD ====================
        try:
            # Process based on URL type
            if parsed.url_type == "thread":
                await _dispatch_with_priority(
                    client,
                    message,
                    parsed,
                    process_thread_comments,
                    context=base_ctx.with_client_type("user_session"),
                    role=_gov_role if _GOVERNANCE_AVAILABLE else None,
                )
            elif parsed.url_type == "topic":
                await _dispatch_with_priority(
                    client,
                    message,
                    parsed,
                    process_topic_posts,
                    context=base_ctx.with_client_type("user_session"),
                    role=_gov_role if _GOVERNANCE_AVAILABLE else None,
                )
            elif parsed.url_type == "private":
                await _dispatch_with_priority(
                    client,
                    message,
                    parsed,
                    process_private_posts,
                    context=base_ctx.with_client_type("user_session"),
                    role=_gov_role if _GOVERNANCE_AVAILABLE else None,
                )
            elif parsed.url_type == "public":
                await _dispatch_with_priority(
                    client,
                    message,
                    parsed,
                    process_public_posts,
                    context=base_ctx.with_client_type("bot"),
                    role=_gov_role if _GOVERNANCE_AVAILABLE else None,
                )
            elif parsed.url_type in ("bot", "quizbot"):
                await _dispatch_with_priority(
                    client,
                    message,
                    parsed,
                    process_bot_posts,
                    context=base_ctx.with_client_type("user_session"),
                    role=_gov_role if _GOVERNANCE_AVAILABLE else None,
                )
        finally:
            # ==================== COMPLETE & PROCESS NEXT ====================
            next_item = await user_queue.complete(user_id)
            if next_item:
                # Notify and process next queued item
                from TechVJ.lang import get_string
                await client.send_message(
                    next_item.chat_id,
                    get_string(next_item.chat_id, "queue_next", link=next_item.link[:60] + "..."),
                    **build_link_preview_kwargs(is_disabled=True)
                )
                await _process_queued_item(client, next_item)
            # Clear request context to prevent leaking to next request
            if _REQUEST_CONTEXT_AVAILABLE:
                _clear_req_ctx()
            
    except asyncio.CancelledError:
        if _REQUEST_CONTEXT_AVAILABLE:
            _clear_req_ctx()
        try:
            await message.reply("Operation cancelled.")
        except Exception:
            pass
        # Don't call complete here - /stop already clears queue
    except Exception as e:
        if _REQUEST_CONTEXT_AVAILABLE:
            _clear_req_ctx()
        try:
            await message.reply(f"Error: {str(e)[:200]}")
        except Exception:
            pass
        # Clear from queue on error (but complete was already called in finally)


async def _process_queued_item(client: Client, item):
    """Process a queued item (called after previous download completes)"""
    user_id = item.chat_id  # chat_id = user_id in private chats
    
    try:
        parsed = item.parsed_data
        base_ctx = _task_context_from_queue(item, parsed, client_type="bot")
        q_role = None
        if _GOVERNANCE_AVAILABLE:
            try:
                q_role = await _role_manager.get_role(user_id)
            except Exception:
                q_role = None
        
        # Process based on URL type
        if parsed.url_type == "thread":
            await _process_queued_thread(client, user_id, parsed, context=base_ctx, role=q_role)
        elif parsed.url_type == "topic":
            await _process_queued_topic(client, user_id, parsed, context=base_ctx, role=q_role)
        elif parsed.url_type == "private":
            await _process_queued_private(client, user_id, parsed, context=base_ctx, role=q_role)
        elif parsed.url_type == "public":
            await _process_queued_public(client, user_id, parsed, context=base_ctx, role=q_role)
        elif parsed.url_type in ("bot", "quizbot"):
            await _process_queued_bot(client, user_id, parsed, context=base_ctx, role=q_role)
            
    except asyncio.CancelledError:
        logger.info(f"Queued item cancelled for user {user_id}")
    except Exception as e:
        logger.error(f"Error processing queued item for {user_id}: {e}")
        try:
            await client.send_message(user_id, f"❌ Xatolik: {str(e)[:100]}")
        except Exception:
            pass
    finally:
        # Process next in queue
        next_item = await user_queue.complete(user_id)
        if next_item:
            try:
                from TechVJ.lang import get_string
                await client.send_message(
                    next_item.chat_id,
                    get_string(next_item.chat_id, "queue_next", link=next_item.link[:60] + "..."),
                    **build_link_preview_kwargs(is_disabled=True)
                )
                await _process_queued_item(client, next_item)
            except Exception as e:
                logger.error(f"Error starting next queued item: {e}")


async def _process_queued_private(client: Client, user_id: int, parsed, context: Optional[TaskContext] = None, role=None):
    """Process queued private channel download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    ctx = context or _new_task_context(proxy_message, parsed, client_type="user_session")
    await _dispatch_with_priority(
        client,
        proxy_message,
        parsed,
        process_private_posts,
        context=ctx.with_client_type("user_session"),
        role=role,
    )


async def _process_queued_topic(client: Client, user_id: int, parsed, context: Optional[TaskContext] = None, role=None):
    """Process queued topic download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    ctx = context or _new_task_context(proxy_message, parsed, client_type="user_session")
    await _dispatch_with_priority(
        client,
        proxy_message,
        parsed,
        process_topic_posts,
        context=ctx.with_client_type("user_session"),
        role=role,
    )


async def _process_queued_thread(client: Client, user_id: int, parsed, context: Optional[TaskContext] = None, role=None):
    """Process queued thread download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    ctx = context or _new_task_context(proxy_message, parsed, client_type="user_session")
    await _dispatch_with_priority(
        client,
        proxy_message,
        parsed,
        process_thread_comments,
        context=ctx.with_client_type("user_session"),
        role=role,
    )


async def _process_queued_public(client: Client, user_id: int, parsed, context: Optional[TaskContext] = None, role=None):
    """Process queued public channel download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    ctx = context or _new_task_context(proxy_message, parsed, client_type="bot")
    await _dispatch_with_priority(
        client,
        proxy_message,
        parsed,
        process_public_posts,
        context=ctx.with_client_type("bot"),
        role=role,
    )


async def _process_queued_bot(client: Client, user_id: int, parsed, context: Optional[TaskContext] = None, role=None):
    """Process queued bot chat download - reuses full interactive logic with proxy message"""
    from types import SimpleNamespace
    proxy_message = SimpleNamespace(
        chat=SimpleNamespace(id=user_id),
        id=None,
        from_user=SimpleNamespace(id=user_id),
    )
    ctx = context or _new_task_context(proxy_message, parsed, client_type="user_session")
    await _dispatch_with_priority(
        client,
        proxy_message,
        parsed,
        process_bot_posts,
        context=ctx.with_client_type("user_session"),
        role=role,
    )


# ==================== SEQUENTIAL POST PROCESSING (SECTION B) ====================


# ==================== THREAD/COMMENT DOWNLOADER ====================

@track_activity
async def process_thread_comments(client: Client, message: Message, parsed: ParsedURL, context: Optional[TaskContext] = None):
    """
    Process comment thread messages.
    
    Fetches messages from a discussion thread and sends them to the user.
    Supports single message or range mode.
    
    Link formats:
        https://t.me/c/CHAT_ID/MSG_ID?thread=THREAD_ID
        https://t.me/c/CHAT_ID/MSG_ID?thread=THREAD_ID&range=START-END
        https://t.me/c/CHAT_ID/MSG_ID?thread=THREAD_ID&rangeSTART-END
    """
    ctx = context or _new_task_context(message, parsed, client_type="user_session")
    user_id = ctx.user_id
    status_msg = None
    
    try:
        async with StopSafePipeline(user_id, task_manager) as pipeline:
            # Check user session
            user_data = await async_db.find_user(user_id)
            if not get(user_data, 'logged_in', False) or not user_data.get('session'):
                await client.send_message(
                    user_id, strings['need_login'],
                    **build_reply_kwargs_from_message(message)
                )
                return
            
            session_string = user_data['session']
            thread_id = parsed.thread_id
            channel_id = ctx.source_chat_id or parsed.channel_id
            
            # CRITICAL: Determine mode based on post_ids
            # - Single ID without range parameter = SINGLE COMMENT MODE
            # - Multiple IDs (range parsed) = RANGE MODE
            # The URL parser puts range IDs into post_ids list
            has_range = len(parsed.post_ids) > 1
            single_comment_id = parsed.post_ids[0] if len(parsed.post_ids) == 1 else None
            
            if has_range:
                range_start = min(parsed.post_ids)
                range_end = max(parsed.post_ids)
                status_text = f"Fetching comments {range_start}-{range_end} from thread {thread_id}..."
            else:
                status_text = f"Fetching comment {single_comment_id} from thread {thread_id}..."
            
            status_msg = await client.send_message(
                user_id, status_text,
                **build_reply_kwargs_from_message(message)
            )
            
            temp_dir = await pipeline.get_temp_dir()
            
            async with create_user_session(
                session_string, user_id,
                peers_to_resolve=[channel_id],
            ) as acc:
                await _resolve_peer_safe(acc, channel_id, ctx)
                try:
                    comments = []
                    
                    # ================================================================
                    # SINGLE COMMENT MODE: Fetch ONLY the specific comment by ID
                    # DO NOT iterate thread. DO NOT scan discussion replies.
                    # ================================================================
                    if not has_range:
                        logger.info(f"Single comment mode: fetching comment {single_comment_id}")
                        
                        try:
                            # Fetch the single comment directly by ID
                            comment = await acc.get_messages(channel_id, single_comment_id)
                            
                            if comment and not comment.empty:
                                comments.append(comment)
                            else:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    f"Comment {single_comment_id} not found or deleted."
                                )
                                return
                                
                        except Exception as e:
                            error_str = str(e).upper()
                            if "MSG_ID_INVALID" in error_str:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    f"Comment {single_comment_id} does not exist."
                                )
                                return
                            raise
                    
                    # ================================================================
                    # RANGE MODE: Fetch ONLY comments in the explicit range
                    # DO NOT iterate entire thread. Fetch specific IDs directly.
                    # ================================================================
                    else:
                        range_start = min(parsed.post_ids)
                        range_end = max(parsed.post_ids)
                        
                        # Generate explicit list of IDs to fetch
                        target_ids = list(range(range_start, range_end + 1))
                        
                        logger.info(f"Range mode: fetching {len(target_ids)} comments ({range_start}-{range_end})")
                        
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"Fetching {len(target_ids)} comments ({range_start}-{range_end})..."
                        )
                        
                        try:
                            # Fetch all IDs in range directly (batch fetch)
                            fetched = await acc.get_messages(channel_id, target_ids)
                            
                            # get_messages returns list when given list of IDs
                            if isinstance(fetched, list):
                                for msg in fetched:
                                    if msg and not msg.empty:
                                        comments.append(msg)
                            elif fetched and not fetched.empty:
                                comments.append(fetched)
                                
                        except Exception as e:
                            error_str = str(e).upper()
                            if "MSG_ID_INVALID" in error_str or "CHANNEL_INVALID" in error_str:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    "Cannot access comments. Check if you have access to this group."
                                )
                                return
                            elif "FLOOD" in error_str:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    "Rate limited. Please try again later."
                                )
                                return
                            raise
                    
                    if not comments:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"No comments found" + 
                            (f" in range {range_start}-{range_end}." if has_range else ".")
                        )
                        return

                    def _is_in_thread(msg):
                        reply_top = getattr(msg, 'reply_to_top_message_id', None)
                        reply_to = getattr(msg, 'reply_to_message_id', None)
                        return msg.id == thread_id or reply_top == thread_id or reply_to == thread_id

                    comments = [msg for msg in comments if _is_in_thread(msg)]

                    if not comments:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"No comments found" +
                            (f" in range {range_start}-{range_end}." if has_range else ".")
                        )
                        return
                    
                    # Sort by ID (oldest first)
                    comments.sort(key=lambda c: c.id)
                    
                    total = len(comments)
                    await client.edit_message_text(
                        user_id, status_msg.id,
                        f"Found {total} comments. Processing..."
                    )
                    
                    processed = 0
                    failed = 0
                    albums_done = set()
                    
                    for idx, comment in enumerate(comments):
                        if await pipeline.check_cancelled():
                            await client.edit_message_text(
                                user_id, status_msg.id,
                                f"**Cancelled**\nProcessed: {processed}/{total}"
                            )
                            return
                        
                        try:
                            # Update status every 5 messages
                            if idx % 5 == 0:
                                try:
                                    await client.edit_message_text(
                                        user_id, status_msg.id,
                                        f"Processing comment {idx + 1}/{total}...\n"
                                        f"Done: {processed} | Failed: {failed}"
                                    )
                                except FloodWait as fw:
                                    await asyncio.sleep(fw.value if hasattr(fw, 'value') else 5)
                            
                            # Handle albums (media groups) in comments
                            if comment.media_group_id:
                                if comment.media_group_id in albums_done:
                                    continue
                                
                                try:
                                    album_msgs = await acc.get_media_group(channel_id, comment.id)
                                    albums_done.add(comment.media_group_id)
                                    
                                    if not album_msgs:
                                        album_success = await download_and_send_media(
                                            client,
                                            acc,
                                            _make_reply_target_message(user_id, getattr(message, "id", None)),
                                            comment,
                                            get_message_type(comment),
                                            temp_dir,
                                            pipeline,
                                            session_string=session_string,
                                            context=ctx,
                                        )
                                    elif _is_photo_only_group(album_msgs):
                                        album_success = await process_album_messages(
                                            client, acc, user_id, album_msgs, temp_dir, pipeline,
                                            request_message=message,
                                            session_string=session_string,
                                            context=ctx,
                                        )
                                    else:
                                        album_success = await _process_media_group_sequential(
                                            client,
                                            acc,
                                            user_id,
                                            album_msgs,
                                            temp_dir,
                                            pipeline,
                                            request_message=message,
                                            session_string=session_string,
                                            context=ctx,
                                        )
                                    if album_success:
                                        processed += 1
                                    else:
                                        failed += 1
                                except Exception as album_err:
                                    logger.warning(f"Album error in comment {comment.id}: {album_err}")
                                    failed += 1
                            else:
                                # Process single comment
                                success = await send_comment_to_user(
                                    client, acc, user_id, comment, temp_dir, pipeline,
                                    request_message=message,
                                    session_string=session_string,
                                    context=ctx,
                                )
                                
                                if success:
                                    processed += 1
                                else:
                                    failed += 1
                            
                            # Flood protection delay
                            await asyncio.sleep(1.5)
                            
                        except FloodWait as fw:
                            wait_time = fw.value if hasattr(fw, 'value') else 30
                            logger.warning(f"FloodWait {wait_time}s at comment {comment.id}")
                            await asyncio.sleep(wait_time)
                            failed += 1
                        except Exception as e:
                            logger.warning(f"Failed to process comment {comment.id}: {e}")
                            failed += 1
                            continue
                    
                    # Final status
                    await client.edit_message_text(
                        user_id, status_msg.id,
                        f"**Thread Comments Completed**\n"
                        f"Thread ID: {thread_id}\n"
                        f"Processed: {processed}/{total}\n"
                        f"Failed: {failed}"
                    )
                    
                except Exception as e:
                    error_str = str(e).upper()
                    if "MSG_ID_INVALID" in error_str:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            "Comments are disabled for this post or thread not found."
                        )
                    elif "CHANNEL_INVALID" in error_str or "PEER_ID_INVALID" in error_str:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            "Cannot access this channel. Make sure you're a member."
                        )
                    else:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"Error fetching comments: {e}"
                        )

    except SessionInvalidError as _sie2:
        await _handle_session_invalid(client, user_id, _sie2)
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "Session invalid. /login bilan qayta kiring.")
            except Exception:
                pass
    except SessionConnectionError as e:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, f"Connection error: {e}")
            except Exception:
                pass
    except asyncio.CancelledError:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "Operation cancelled.")
            except Exception:
                pass


async def send_comment_to_user(
    client: Client,
    acc,  # User session
    user_id: int,
    comment,  # Message object
    temp_dir: str,
    pipeline: StopSafePipeline,
    request_message: Optional[Message] = None,
    session_string: Optional[str] = None,
    context: Optional[TaskContext] = None,
) -> bool:
    """
    Send a single comment to the user, preserving text, entities, and media.
    
    Returns True on success, False on failure.
    """
    try:
        # Check for media
        msg_type = get_message_type(comment)
        
        if msg_type and msg_type not in ("Text", "Unknown"):
            # Download and send media
            return await download_and_send_media(
                client, acc,
                _make_reply_target_message(
                    user_id,
                    getattr(request_message, "id", None),
                ),
                comment, msg_type, temp_dir, pipeline,
                session_string=session_string,
                context=context,
            )
        
        # Text-only comment - use MessageEntity (NO MARKDOWN)
        if comment.text:
            text, entities = get_text_with_entities(comment)

            if not text:
                return True

            # Split if too long using renderer
            from core.text_renderer import extract_to_renderer
            renderer = extract_to_renderer(text, entities or [])
            chunks = renderer.render_chunks(TELEGRAM_MESSAGE_LIMIT)

            for chunk_text, chunk_entities in chunks:
                if chunk_text:
                    await client.send_message(
                        user_id,
                        text=chunk_text,
                        entities=chunk_entities if chunk_entities else None,
                        **build_reply_kwargs_from_message(request_message),
                    )
                    await asyncio.sleep(0.5)

            return True
        
        # Empty comment (maybe a service message)
        return True
        
    except Exception as e:
        logger.warning(f"send_comment_to_user error: {e}")
        return False


# ==================== TOPIC POST DOWNLOADER ====================

@track_activity
async def process_topic_posts(client: Client, message: Message, parsed: ParsedURL, context: Optional[TaskContext] = None):
    """
    Process messages from a specific topic in a forum group.
    
    Only fetches messages that belong to the specified topic.
    Ignores messages from other topics.
    
    Link formats:
        https://t.me/c/CHAT_ID/TOPIC_ID/MSG_ID
        https://t.me/c/CHAT_ID/TOPIC_ID/START-END
    """
    ctx = context or _new_task_context(message, parsed, client_type="user_session")
    user_id = ctx.user_id
    status_msg = None
    op_log(
        "topic_download",
        user_id=user_id,
        topic_id=parsed.topic_id,
        message_id=parsed.post_ids[0] if parsed.post_ids else 0,
        task_id=ctx.task_id,
    )
    
    try:
        async with StopSafePipeline(user_id, task_manager) as pipeline:
            # Check user session
            user_data = await async_db.find_user(user_id)
            if not get(user_data, 'logged_in', False) or not user_data.get('session'):
                await client.send_message(
                    user_id, strings['need_login'],
                    **build_reply_kwargs_from_message(message)
                )
                return
            
            session_string = user_data['session']
            topic_id = parsed.topic_id
            channel_id = ctx.source_chat_id or parsed.channel_id
            post_ids = parsed.post_ids
            topic_range_anchor = getattr(parsed, "topic_range_anchor", None)
            
            total_posts = len(post_ids)
            is_range = bool(topic_range_anchor) or total_posts > 1
            if topic_range_anchor:
                status_detail = f"Anchors: {topic_range_anchor[0]} -> {topic_range_anchor[1]}"
            elif is_range:
                status_detail = f"IDs: {', '.join(str(pid) for pid in post_ids[:10])}"
                if total_posts > 10:
                    status_detail += f" ... +{total_posts - 10}"
            else:
                status_detail = f"Message ID: {post_ids[0]}"
            
            status_msg = await client.send_message(
                user_id,
                f"Processing {'range' if is_range else 'message'} from topic {topic_id}...\n"
                f"{status_detail}",
                **build_reply_kwargs_from_message(message)
            )
            
            temp_dir = await pipeline.get_temp_dir()
            
            async with create_user_session(
                session_string, user_id,
                peers_to_resolve=[channel_id],
            ) as acc:
                await _resolve_peer_safe(acc, channel_id, ctx)
                # First, verify the topic exists (optional - for better error messages)
                try:
                    chat = await acc.get_chat(channel_id)
                    if not chat:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            "Cannot access this chat. Make sure you're a member."
                        )
                        return
                    
                    # Check if it's a forum (supergroup with topics enabled)
                    is_forum = hasattr(chat, 'is_forum') and chat.is_forum
                    if not is_forum:
                        logger.info(f"Chat {channel_id} may not be a forum, proceeding anyway")
                        
                except Exception as chat_err:
                    logger.warning(f"Could not verify chat: {chat_err}")
                
                # FIX Issue 3.3: Validate topic_id exists and is a topic starter
                try:
                    topic_msg = await asyncio.wait_for(
                        acc.get_messages(channel_id, topic_id),
                        timeout=15.0
                    )
                    if not topic_msg or topic_msg.empty:
                        await client.edit_message_text(
                            user_id, status_msg.id,
                            f"⚠️ Topic not found.\nThe topic ID {topic_id} does not exist or was deleted."
                        )
                        return
                    
                    # Additional check: verify it's actually a topic starter (if available)
                    # Topic starters typically don't have reply_to_top_message_id pointing elsewhere
                    if hasattr(topic_msg, 'reply_to_top_message_id') and topic_msg.reply_to_top_message_id:
                        if topic_msg.reply_to_top_message_id != topic_id:
                            # This message is a reply in another topic, not a topic starter
                            await client.edit_message_text(
                                user_id, status_msg.id,
                                f"⚠️ Invalid topic URL.\nMessage {topic_id} is not a topic starter.\n"
                                f"It appears to be a reply in topic {topic_msg.reply_to_top_message_id}."
                            )
                            return
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout validating topic {topic_id}")
                    # Continue anyway
                except Exception as e:
                    logger.warning(f"Could not validate topic: {e}")
                    # Continue anyway
                    # Continue anyway
                
                processed = 0
                failed = 0
                skipped = 0
                albums_done = set()

                # Throttle-aware status tracker + batch controller
                if _THROTTLE_AVAILABLE:
                    _t_status = StatusTracker(_throttle, client, user_id, status_msg.id)
                    _t_batch = BatchController(user_id)
                else:
                    _t_status = None
                    _t_batch = None

                # ── Full topic extract path ────────────────────────────────────
                # When post_ids is empty or range anchors are present, use
                # TopicExtractor to preserve real topic chronology. Topic
                # message IDs are not guaranteed to be monotonic inside a topic.
                if (topic_range_anchor or not post_ids) and _TOPIC_EXTRACTOR_AVAILABLE:
                    try:
                        extractor = TopicExtractor(
                            acc,
                            TopicExtractorConfig(
                                chat_id=channel_id,
                                topic_id=topic_id,
                                fetch_batch_size=200,
                                inter_page_delay=0.3,
                            ),
                        )
                        if topic_range_anchor:
                            topic_msgs = await extractor.extract_between(*topic_range_anchor)
                        else:
                            topic_msgs = await extractor.extract_all()
                        post_ids = [m.id for m in topic_msgs]
                        total_posts = len(post_ids)
                        is_range = total_posts > 1
                        # Update status with actual count
                        try:
                            await client.edit_message_text(
                                user_id, status_msg.id,
                                f"Topic fetched: {total_posts} messages found in topic order. Processing...",
                            )
                        except Exception:
                            pass
                        # Prefill message cache to avoid per-ID get_messages calls
                        _topic_msg_cache = {m.id: m for m in topic_msgs}
                    except TopicExtractionError as _te:
                        logger.warning("TopicExtractor failed, falling back to per-ID: %s", _te)
                        await _notify_realtime_post_failure(
                            client,
                            acc,
                            user_id=user_id,
                            chat_id=channel_id,
                            post_id=topic_id,
                            stage="topic_extraction",
                            reason="TopicExtractor failed",
                            error=_te,
                            source_msg=topic_msg if "topic_msg" in locals() else None,
                            request_message=message,
                            context=ctx,
                            parsed=parsed,
                            requested_total=total_posts,
                        )
                        _topic_msg_cache = {}
                        if topic_range_anchor:
                            try:
                                await client.edit_message_text(
                                    user_id,
                                    status_msg.id,
                                    f"Could not resolve topic range anchors: {_te}",
                                )
                            except Exception:
                                pass
                            return
                else:
                    _topic_msg_cache = {}
                # ── End full topic extract path ────────────────────────────────

                for idx, post_id in enumerate(post_ids):
                    await ping_activity(user_id)
                    if await pipeline.check_cancelled():
                        if _t_status:
                            await _t_status.finish(
                                f"**Cancelled**\nProcessed: {processed}/{total_posts}\nSkipped: {skipped}"
                            )
                        else:
                            await client.edit_message_text(
                                user_id, status_msg.id,
                                f"**Cancelled**\nProcessed: {processed}/{total_posts}\nSkipped: {skipped}"
                            )
                        return

                    try:
                        # Batch chunk pause
                        if _t_batch:
                            pause = _t_batch.check_pause(idx)
                            if pause > 0:
                                if _t_status:
                                    await _t_status.update(
                                        f"⏸ Pauza {pause:.0f}s... ({idx}/{total_posts})", force=True
                                    )
                                await asyncio.sleep(pause)

                        # Update status
                        _t_progress = (
                            f"Processing {idx + 1}/{total_posts}\n"
                            f"Done: {processed} | Skipped: {skipped} | Failed: {failed}"
                        )
                        if _t_status:
                            await _t_status.update(_t_progress)
                        elif idx % 3 == 0:
                            try:
                                await client.edit_message_text(
                                    user_id, status_msg.id, _t_progress
                                )
                            except FloodWait as fw:
                                await asyncio.sleep(getattr(fw, 'value', 5))
                        
                        # Fetch the message — use cache from TopicExtractor if available
                        try:
                            if post_id in _topic_msg_cache:
                                msg = _topic_msg_cache[post_id]
                            else:
                                msg = await asyncio.wait_for(
                                    acc.get_messages(channel_id, post_id),
                                    timeout=30.0
                                )
                        except asyncio.TimeoutError:
                            logger.warning(f"Timeout fetching message {post_id}")
                            failed += 1
                            await _notify_realtime_post_failure(
                                client,
                                acc,
                                user_id=user_id,
                                chat_id=channel_id,
                                post_id=post_id,
                                stage="topic_fetch",
                                reason="get_messages timed out",
                                request_message=message,
                                context=ctx,
                                parsed=parsed,
                                requested_total=total_posts,
                            )
                            continue
                        
                        if not msg or msg.empty:
                            skipped += 1
                            await _notify_realtime_post_failure(
                                client,
                                acc,
                                user_id=user_id,
                                chat_id=channel_id,
                                post_id=post_id,
                                stage="topic_fetch",
                                reason="post is deleted or inaccessible",
                                source_msg=msg,
                                request_message=message,
                                context=ctx,
                                parsed=parsed,
                                requested_total=total_posts,
                            )
                            continue
                        
                        # Check if message belongs to our topic
                        # CRITICAL: Do NOT trust message ID ordering.
                        # Filter ONLY by reply_to_top_message_id == topic_id
                        # or msg.id == topic_id (topic starter itself).
                        # reply_to_message_id alone is unreliable — it could
                        # point to any message, not necessarily the topic root.
                        is_in_topic = False
                        
                        if msg.id == topic_id:
                            # This IS the topic starter message
                            is_in_topic = True
                        elif hasattr(msg, 'reply_to_top_message_id') and msg.reply_to_top_message_id == topic_id:
                            # Primary method: message explicitly belongs to this topic
                            is_in_topic = True
                        elif hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id == topic_id:
                            # Direct reply to topic starter (first-level reply, no top_message_id set)
                            # Only accept if reply_to_top_message_id is absent/None
                            if not getattr(msg, 'reply_to_top_message_id', None):
                                is_in_topic = True
                        
                        if not is_in_topic:
                            logger.debug(f"Message {post_id} not in topic {topic_id}, skipping")
                            skipped += 1
                            continue
                        
                        # Check for album (media group)
                        if msg.media_group_id:
                            if msg.media_group_id in albums_done:
                                continue  # Already processed this album

                            try:
                                # Fetch all album messages
                                album_msgs = await acc.get_media_group(channel_id, post_id)
                                albums_done.add(msg.media_group_id)

                                if not album_msgs:
                                    album_success = await download_and_send_media(
                                        client,
                                        acc,
                                        _make_reply_target_message(user_id, getattr(message, "id", None)),
                                        msg,
                                        get_message_type(msg),
                                        temp_dir,
                                        pipeline,
                                        session_string=session_string,
                                        context=ctx,
                                    )
                                elif _is_photo_only_group(album_msgs):
                                    album_success = await process_album_messages(
                                        client, acc, user_id, album_msgs, temp_dir, pipeline,
                                        request_message=message,
                                        session_string=session_string,
                                        context=ctx,
                                    )
                                else:
                                    album_success = await _process_media_group_sequential(
                                        client,
                                        acc,
                                        user_id,
                                        album_msgs,
                                        temp_dir,
                                        pipeline,
                                        request_message=message,
                                        session_string=session_string,
                                        context=ctx,
                                    )
                                if album_success:
                                    processed += 1
                                    if _t_batch:
                                        _t_batch.record_success()
                                else:
                                    failed += 1
                                    await _notify_realtime_post_failure(
                                        client,
                                        acc,
                                        user_id=user_id,
                                        chat_id=channel_id,
                                        post_id=post_id,
                                        stage="topic_album",
                                        reason="album processing returned False",
                                        source_msg=msg,
                                        request_message=message,
                                        context=ctx,
                                        parsed=parsed,
                                        requested_total=total_posts,
                                    )
                                    if _t_batch:
                                        _t_batch.record_error()
                            except Exception as album_err:
                                logger.warning(f"Album error for message {post_id}: {album_err}")
                                failed += 1
                                await _notify_realtime_post_failure(
                                    client,
                                    acc,
                                    user_id=user_id,
                                    chat_id=channel_id,
                                    post_id=post_id,
                                    stage="topic_album",
                                    reason="album processing raised exception",
                                    error=album_err,
                                    source_msg=msg,
                                    request_message=message,
                                    context=ctx,
                                    parsed=parsed,
                                    requested_total=total_posts,
                                )
                                if _t_batch:
                                    _t_batch.record_error()
                        else:
                            # Single message
                            success = await process_single_topic_message(
                                client, acc, user_id, msg, temp_dir, pipeline,
                                request_message=message,
                                session_string=session_string,
                                context=ctx,
                            )
                            if success:
                                processed += 1
                                if _t_batch:
                                    _t_batch.record_success()
                            else:
                                failed += 1
                                if _t_batch:
                                    _t_batch.record_error()

                        # Consecutive error check
                        if _t_batch and _t_batch.should_stop():
                            logger.warning("BatchController: stopping topic batch for user %s", user_id)
                            if _t_status:
                                await _t_status.finish(
                                    f"⚠️ **Ko'p xato — batch to'xtatildi**\n"
                                    f"✅ {processed} | ❌ {failed} | ⏭ {skipped}"
                                )
                            break

                        # Flood protection delay
                        await asyncio.sleep(1.5)

                    except FloodWait as fw:
                        wait_time = getattr(fw, 'value', getattr(fw, 'x', 30))
                        logger.warning(f"FloodWait {wait_time}s at message {post_id}")
                        await _notify_realtime_post_failure(
                            client,
                            acc,
                            user_id=user_id,
                            chat_id=channel_id,
                            post_id=post_id,
                            stage="topic_floodwait",
                            reason=f"FloodWait {wait_time}s",
                            error=fw,
                            request_message=message,
                            context=ctx,
                            parsed=parsed,
                            requested_total=total_posts,
                        )
                        if _t_batch:
                            _t_batch.record_flood(wait_time)
                        if wait_time > 120:
                            if _t_status:
                                await _t_status.finish(
                                    f"⚠️ **FloodWait {wait_time}s** — batch paused.\n"
                                    f"✅ {processed} processed, ❌ {failed} failed\n"
                                    f"Retry in {wait_time // 60} min."
                                )
                            else:
                                try:
                                    await client.edit_message_text(
                                        user_id, status_msg.id,
                                        f"⚠️ **FloodWait {wait_time}s** — batch paused.\n"
                                        f"✅ {processed} processed, ❌ {failed} failed\n"
                                        f"Retry in {wait_time // 60} min."
                                    )
                                except Exception:
                                    pass
                            break
                        else:
                            await asyncio.sleep(wait_time)
                            failed += 1
                    except Exception as e:
                        error_str = str(e).upper()
                        if "MSG_ID_INVALID" in error_str:
                            skipped += 1
                        else:
                            logger.warning(f"Failed to process topic message {post_id}: {e}")
                            failed += 1
                            await _notify_realtime_post_failure(
                                client,
                                acc,
                                user_id=user_id,
                                chat_id=channel_id,
                                post_id=post_id,
                                stage="topic_process",
                                reason="topic message processing raised exception",
                                error=e,
                                request_message=message,
                                context=ctx,
                                parsed=parsed,
                                requested_total=total_posts,
                            )
                            if _t_batch:
                                _t_batch.record_error()
                        continue

                # BatchController cleanup
                if _t_batch:
                    _t_batch.finish()

                # Final status
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"**Topic Download Completed**\n"
                    f"Topic ID: {topic_id}\n"
                    f"Processed: {processed}\n"
                    f"Skipped (not in topic/missing): {skipped}\n"
                    f"Failed: {failed}"
                )

    except SessionInvalidError as _sie3:
        await _handle_session_invalid(client, user_id, _sie3)
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "Session invalid. /login bilan qayta kiring.")
            except Exception:
                pass
    except SessionConnectionError as e:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, f"Connection error: {e}")
            except Exception:
                pass
    except asyncio.CancelledError:
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "Operation cancelled.")
            except Exception:
                pass


async def process_single_topic_message(
    client: Client,
    acc,
    user_id: int,
    msg,
    temp_dir: str,
    pipeline: StopSafePipeline,
    request_message: Optional[Message] = None,
    session_string: Optional[str] = None,
    context: Optional[TaskContext] = None,
) -> bool:
    """Process a single message from a topic."""
    try:
        msg_type = get_message_type(msg)
        source_chat_id = (
            getattr(getattr(msg, "chat", None), "id", None)
            or (context.source_chat_id if context else None)
        )
        source_post_id = getattr(msg, "id", None) or 0

        async def _notify_topic_failure(stage: str, reason: str, error: Any = None) -> None:
            if source_chat_id and source_post_id:
                await _notify_realtime_post_failure(
                    client,
                    acc,
                    user_id=user_id,
                    chat_id=source_chat_id,
                    post_id=source_post_id,
                    stage=stage,
                    reason=reason,
                    error=error,
                    source_msg=msg,
                    request_message=request_message,
                    context=context,
                    parsed=None,
                )
        
        if msg_type and msg_type not in ("Text", "Unknown"):
            result = await download_and_send_media(
                client,
                acc,
                _make_reply_target_message(user_id, getattr(request_message, "id", None)),
                msg,
                msg_type,
                temp_dir,
                pipeline,
                session_string=session_string,
                context=context,
            )
            if not result:
                await _notify_topic_failure("topic_send_media", "download_and_send_media returned False")
            return result
        if msg.text:
            from core.smart_renderer import from_message, SmartRenderer

            renderer = from_message(msg)
            reply_markup = msg.reply_markup if hasattr(msg, 'reply_markup') else None

            chunks = renderer.render_chunks()
            for i, kwargs in enumerate(chunks):
                if i == len(chunks) - 1 and reply_markup:
                    kwargs['reply_markup'] = reply_markup
                kwargs.update(build_reply_kwargs_from_message(request_message))
                await client.send_message(user_id, **kwargs)
                if len(chunks) > 1:
                    await asyncio.sleep(0.5)
            return True
        
        return True
        
    except FloodWait:
        raise
    except Exception as e:
        logger.warning(f"process_single_topic_message error: {e}")
        try:
            source_chat_id = (
                getattr(getattr(msg, "chat", None), "id", None)
                or (context.source_chat_id if context else None)
            )
            source_post_id = getattr(msg, "id", None) or 0
            if source_chat_id and source_post_id:
                await _notify_realtime_post_failure(
                    client,
                    acc,
                    user_id=user_id,
                    chat_id=source_chat_id,
                    post_id=source_post_id,
                    stage="topic_single_message",
                    reason="process_single_topic_message raised exception",
                    error=e,
                    source_msg=msg,
                    request_message=request_message,
                    context=context,
                    parsed=None,
                )
        except Exception:
            pass
        return False


async def process_album_messages(
    client: Client,
    acc,
    user_id: int,
    album_msgs: list,
    temp_dir: str,
    pipeline: StopSafePipeline,
    request_message: Optional[Message] = None,
    session_string: Optional[str] = None,
    context: Optional[TaskContext] = None,
) -> bool:
    """
    Process and send an album (media group) to the user.
    
    CRITICAL RULE: Albums are ONLY for photo-only groups.
    If ANY message in the group is video/document/audio, 
    process ALL messages individually (not as album).
    
    Telegram albums only work reliably with InputMediaPhoto.
    """
    try:
        if not album_msgs:
            return False
        
        downloaded_files = []
        
        # STEP 1: Analyze media types in the group
        # Album mode is ONLY allowed if ALL items are photos
        all_photos = True
        has_media = False
        
        for msg in album_msgs:
            msg_type = get_message_type(msg)
            if msg_type and msg_type not in ("Text", "Unknown"):
                has_media = True
                if msg_type != "Photo":
                    all_photos = False
                    break
        
        if not has_media:
            return False
        
        # STEP 2: Photo-only album mode
        if all_photos and len(album_msgs) >= 2:
            media_list = []
            album_overflow = None  # Track overflow caption payload(s)
            
            for msg in album_msgs:
                if await pipeline.check_cancelled():
                    _cleanup_files(downloaded_files)
                    return False
                
                msg_type = get_message_type(msg)
                if msg_type != "Photo":
                    continue
                
                try:
                    # Use download engine if available
                    engine = await get_engine()
                    file_path = await engine.download(
                        message=msg,
                        client=acc,
                        download_dir=temp_dir,
                        user_id=user_id
                    )
                    
                    if file_path and os.path.exists(file_path):
                        downloaded_files.append(file_path)
                        
                        # ALBUM RULE: Caption ONLY on first photo - use SmartRenderer
                        caption = None
                        caption_entities = None
                        if len(media_list) == 0 and msg.caption:  # First photo only
                            try:
                                from core.smart_renderer import from_message
                                renderer = from_message(msg)
                                renderer._is_caption = True
                                caption_result, overflow_result = renderer.render_caption_chunks(limit=1024)
                                caption = caption_result.get('caption')
                                caption_entities = caption_result.get('caption_entities')
                                if overflow_result:
                                    album_overflow = overflow_result  # Store kwargs list
                            except Exception as sr_err:
                                logger.debug(f"SmartRenderer error: {sr_err}")
                                caption = msg.caption[:1024] if len(msg.caption) > 1024 else msg.caption
                                caption_entities = None
                        
                        # Validate entity bounds before creating InputMediaPhoto
                        if caption and caption_entities:
                            from core.entity_splitter import utf16_len as _utf16_len
                            cap_utf16 = _utf16_len(caption)
                            caption_entities = [e for e in caption_entities if e.offset >= 0 and e.offset + e.length <= cap_utf16]
                            if not caption_entities:
                                caption_entities = None
                        
                        # InputMediaPhoto with validated entities
                        media_list.append(InputMediaPhoto(file_path, caption=caption, caption_entities=caption_entities))
                        
                except Exception as e:
                    logger.warning(f"Album photo download error: {e}")
                    continue
            
            # album_overflow was set when processing first photo caption

            if len(media_list) >= 2:
                # Send as photo album via user session
                try:
                    _target_chat_id = _get_user_session_target_chat_id(client, user_id)
                    await acc.send_media_group(
                        _target_chat_id,
                        media_list,
                        **build_reply_kwargs_from_message(request_message),
                    )

                    # Overflow caption text always via bot client (not user session —
                    # user session text triggers the bot's link handler if caption
                    # contains Telegram URLs, causing duplicate downloads).
                    if album_overflow:
                        try:
                            if isinstance(album_overflow, list):
                                for idx, chunk_kwargs in enumerate(album_overflow):
                                    payload = dict(chunk_kwargs)
                                    payload['chat_id'] = user_id
                                    payload.update(build_reply_kwargs_from_message(request_message))
                                    await client.send_message(**payload)
                            elif isinstance(album_overflow, dict):
                                payload = dict(album_overflow)
                                payload['chat_id'] = user_id
                                payload.update(build_reply_kwargs_from_message(request_message))
                                await client.send_message(**payload)
                            else:
                                from TechVJ.lang import get_string
                                header = get_string(user_id, "caption_continued")
                                await client.send_message(
                                    user_id,
                                    f"{header}\n\n{album_overflow}",
                                    **build_reply_kwargs_from_message(request_message),
                                )
                        except Exception:
                            pass

                    _cleanup_files(downloaded_files)
                    return True
                except Exception as e:
                    logger.warning(f"send_media_group failed: {e}")
                    # Fall through to individual send

            # Fallback: send photos individually via user session
            _target_chat_id = _get_user_session_target_chat_id(client, user_id)
            for idx, media in enumerate(media_list):
                try:
                    # Only first photo gets caption in fallback too
                    cap = media.caption if idx == 0 else None
                    await acc.send_photo(
                        _target_chat_id,
                        media.media,
                        caption=cap,
                        **build_reply_kwargs_from_message(request_message),
                    )
                    await asyncio.sleep(1)
                except Exception as send_err:
                    logger.warning(f"Photo send error: {send_err}")

            # Send overflow after individual photos too (bot client — safe from link handler)
            if album_overflow:
                try:
                    if isinstance(album_overflow, list):
                        for idx, chunk_kwargs in enumerate(album_overflow):
                            payload = dict(chunk_kwargs)
                            payload['chat_id'] = user_id
                            payload.update(build_reply_kwargs_from_message(request_message))
                            await client.send_message(**payload)
                    elif isinstance(album_overflow, dict):
                        payload = dict(album_overflow)
                        payload['chat_id'] = user_id
                        payload.update(build_reply_kwargs_from_message(request_message))
                        await client.send_message(**payload)
                    else:
                        from TechVJ.lang import get_string
                        header = get_string(user_id, "caption_continued")
                        await client.send_message(
                            user_id,
                            f"{header}\n\n{album_overflow}",
                            **build_reply_kwargs_from_message(request_message),
                        )
                except Exception:
                    pass

            _cleanup_files(downloaded_files)
            return True
        
        # STEP 3: Mixed media or non-photo album -> process INDIVIDUALLY
        # This handles: videos, documents, audio, voice, animations, mixed groups
        logger.info(f"Processing media group as individual files (not photo-only album)")
        
        processed_count = 0
        for msg in album_msgs:
            if await pipeline.check_cancelled():
                _cleanup_files(downloaded_files)
                return False
            
            msg_type = get_message_type(msg)
            if not msg_type or msg_type in ("Text", "Unknown"):
                continue
            
            try:
                success = await download_and_send_media(
                    client,
                    acc,
                    _make_reply_target_message(user_id, getattr(request_message, "id", None)),
                    msg,
                    msg_type,
                    temp_dir,
                    pipeline,
                    session_string=session_string,
                    context=context,
                )
                if success:
                    processed_count += 1
                
                # Rate limit between individual sends
                await asyncio.sleep(1.5)
                
            except Exception as e:
                logger.warning(f"Individual media error: {e}")
                continue
        
        return processed_count > 0
        
    except FloodWait:
        raise
    except Exception as e:
        logger.warning(f"process_album_messages error: {e}")
        return False


def _cleanup_files(files: list):
    """Helper to cleanup downloaded files."""
    for f in files:
        try:
            if f and os.path.exists(f):
                os.remove(f)
        except Exception:
            pass


# ==================== PRIVATE POST PROCESSING ====================

@track_activity
async def process_private_posts(client: Client, message: Message, parsed: ParsedURL, context: Optional[TaskContext] = None):
    """
    Process private channel posts with ALBUM-AWARE iteration.
    
    Architecture (Refactored):
    - Session is task-scoped via context manager (guaranteed cleanup)
    - Album detection returns boundaries for skip-ahead
    - In-memory album tracking only (no MongoDB)
    - Deterministic timeouts on all Pyrogram calls
    """
    ctx = context or _new_task_context(message, parsed, client_type="user_session")
    user_id = ctx.user_id
    status_msg = None
    
    try:
        # Create stop-safe pipeline
        async with StopSafePipeline(user_id, task_manager) as pipeline:
            
            # Check user session
            user_data = await async_db.find_user(user_id)
            if not get(user_data, 'logged_in', False) or not user_data.get('session'):
                await client.send_message(user_id, strings['need_login'], **build_reply_kwargs_from_message(message))
                return
            
            session_string = user_data['session']
            
            # Create temp directory for this operation
            temp_dir = await pipeline.get_temp_dir()
            
            total_posts = len(parsed.post_ids)
            status_msg = await client.send_message(
                user_id,
                f"⏳ {total_posts} ta post yuklanmoqda...",
                **build_reply_kwargs_from_message(message)
            )
            
            # Pin status message so user can always find it
            if total_posts > 1:
                try:
                    await client.pin_chat_message(user_id, status_msg.id, disable_notification=True)
                except Exception:
                    pass
            
            # Use album-aware iterator
            iterator = AlbumAwareIterator(parsed.post_ids)
            processed = 0
            failed = 0
            failed_ids = []  # Only existing but unsent posts (not deleted)
            deleted = 0
            albums_processed = 0

            # Throttle-aware status tracker + batch controller
            if _THROTTLE_AVAILABLE:
                _status = StatusTracker(_throttle, client, user_id, status_msg.id)
                _batch = BatchController(user_id)
            else:
                _status = None
                _batch = None
            
            # Task-scoped session - guaranteed cleanup on exit
            try:
                async with create_user_session(
                    session_string, user_id,
                    peers_to_resolve=[parsed.channel_id],
                ) as acc:
                    await _resolve_peer_safe(acc, parsed.channel_id, ctx)
                    while True:
                        # Check for cancellation
                        if await pipeline.check_cancelled():
                            if _status:
                                await _status.finish(
                                    f"**Cancelled**\nProcessed: {processed}/{total_posts}\nAlbums: {albums_processed}"
                                )
                            else:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    f"**Cancelled**\nProcessed: {processed}/{total_posts}\nAlbums: {albums_processed}"
                                )
                            return

                        # Get next message ID
                        post_id = iterator.next()
                        if post_id is None:
                            break  # Done
                        await ping_activity(user_id)

                        try:
                            # Batch chunk pause
                            if _batch:
                                pause = _batch.check_pause(iterator.current_position)
                                if pause > 0:
                                    if _status:
                                        await _status.update(
                                            f"⏸ Pauza {pause:.0f}s... ({iterator.current_position}/{total_posts})",
                                            force=True
                                        )
                                    await asyncio.sleep(pause)

                            # Update status
                            _progress_text = (
                                f"⏳ {iterator.current_position}/{total_posts} | "
                                f"ID: `{post_id}`\n"
                                f"✅ {processed}  📁 {albums_processed}  ❌ {failed}"
                            )
                            if _status:
                                await _status.update(_progress_text)
                            else:
                                try:
                                    await client.edit_message_text(
                                        user_id, status_msg.id, _progress_text
                                    )
                                except Exception:
                                    pass

                            # Try to detect album boundary
                            boundary = await detect_album_boundary(acc, parsed.channel_id, post_id)

                            if boundary:
                                # Check if already processed
                                if iterator.is_album_processed(boundary.media_group_id):
                                    iterator.skip_to(boundary.next_message_id)
                                    continue

                                # Process entire album using existing session
                                success, status, _ = await process_album_with_session(
                                    bot_client=client,
                                    user_session=acc,
                                    user_id=user_id,
                                    target_chat_id=user_id,
                                    source_chat_id=parsed.channel_id,
                                    message_id=post_id,
                                    reply_to_message_id=message.id,
                                    check_cancelled=pipeline.check_cancelled,
                                    sent_albums=iterator.processed_albums,
                                )

                                if success:
                                    processed += 1
                                    albums_processed += 1
                                    iterator.mark_album_processed(boundary.media_group_id)
                                    if _batch:
                                        _batch.record_success()
                                elif status == "cancelled":
                                    return
                                elif status == "already_sent":
                                    processed += 1
                                    if _batch:
                                        _batch.record_success()
                                else:
                                    failed += 1
                                    failed_ids.append(post_id)
                                    await _notify_realtime_post_failure(
                                        client,
                                        acc,
                                        user_id=user_id,
                                        chat_id=parsed.channel_id,
                                        post_id=post_id,
                                        stage="private_album",
                                        reason=f"album processing returned status={status}",
                                        request_message=message,
                                        context=ctx,
                                        parsed=parsed,
                                        requested_total=total_posts,
                                    )
                                    if _batch:
                                        _batch.record_error()

                                # Skip remaining album messages
                                iterator.skip_to(boundary.next_message_id)
                            else:
                                # Not an album - process single message
                                result = await process_single_post(
                                    client, acc, message,
                                    parsed.channel_id, post_id,
                                    temp_dir, pipeline,
                                    session_string=session_string,
                                    context=ctx,
                                )

                                if result == "deleted":
                                    deleted += 1
                                elif result:
                                    processed += 1
                                    if _batch:
                                        _batch.record_success()
                                else:
                                    failed += 1
                                    failed_ids.append(post_id)
                                    if _batch:
                                        _batch.record_error()

                            # Consecutive error check
                            if _batch and _batch.should_stop():
                                logger.warning("BatchController: stopping batch for user %s (consecutive errors)", user_id)
                                if _status:
                                    await _status.finish(
                                        f"⚠️ **Ko'p xato — batch to'xtatildi**\n"
                                        f"✅ {processed}/{total_posts} yuklandi\n❌ {failed} xato"
                                    )
                                break

                            # Rate limiting delay
                            await asyncio.sleep(1.5)

                        except asyncio.CancelledError:
                            raise
                        except FloodWait as fw:
                            wait_time = getattr(fw, 'value', getattr(fw, 'x', 30))
                            logger.warning(f"FloodWait {wait_time}s at post {post_id}")
                            await _notify_realtime_post_failure(
                                client,
                                acc,
                                user_id=user_id,
                                chat_id=parsed.channel_id,
                                post_id=post_id,
                                stage="private_floodwait",
                                reason=f"FloodWait {wait_time}s",
                                error=fw,
                                request_message=message,
                                context=ctx,
                                parsed=parsed,
                                requested_total=total_posts,
                            )
                            if _batch:
                                _batch.record_flood(wait_time)
                            if wait_time > 120:
                                if _status:
                                    await _status.finish(
                                        f"⚠️ **FloodWait {wait_time}s** — batch paused.\n"
                                        f"✅ {processed} processed, ❌ {failed} failed\n"
                                        f"Retry in {wait_time // 60} min."
                                    )
                                else:
                                    try:
                                        await client.edit_message_text(
                                            user_id, status_msg.id,
                                            f"⚠️ **FloodWait {wait_time}s** — batch paused.\n"
                                            f"✅ {processed} processed, ❌ {failed} failed\n"
                                            f"Retry in {wait_time // 60} min."
                                        )
                                    except Exception:
                                        pass
                                break
                            else:
                                await asyncio.sleep(wait_time)
                                failed += 1
                                continue
                        except Exception as e:
                            logger.warning(f"Error processing post {post_id}: {e}")
                            failed += 1
                            failed_ids.append(post_id)
                            await _notify_realtime_post_failure(
                                client,
                                acc,
                                user_id=user_id,
                                chat_id=parsed.channel_id,
                                post_id=post_id,
                                stage="private_loop",
                                reason="private post loop raised exception",
                                error=e,
                                request_message=message,
                                context=ctx,
                                parsed=parsed,
                                requested_total=total_posts,
                            )
                            if _batch:
                                _batch.record_error()
                            continue
                    
            except SessionInvalidError as e:
                await _handle_session_invalid(client, user_id, e)
                try:
                    await client.edit_message_text(
                        user_id, status_msg.id,
                        f"**Sessiya yaroqsiz**\n/login bilan qayta kiring.\nSabab: {e}"
                    )
                except Exception:
                    pass
                return
            except SessionConnectionError as e:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"**Connection Error**\nPlease try again.\nError: {e}"
                )
                return
            
            # Final status
            failed_info = ""
            if failed_ids:
                shown_ids = failed_ids[:20]
                id_list = ", ".join(str(i) for i in shown_ids)
                failed_info = f"\n\n**Jo'natilmagan ID lar:**\n`{id_list}`"
                if len(failed_ids) > 20:
                    failed_info += f"\n...va yana {len(failed_ids) - 20} ta"
            
            deleted_info = f"\n🗑 O'chirilgan: {deleted}" if deleted > 0 else ""
            
            if processed == 0 and failed == 0 and deleted > 0:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"⚠️ **Barcha xabarlar o'chirilgan**\n"
                    f"So'ralgan: {total_posts}\n"
                    f"🗑 O'chirilgan: {deleted}"
                )
            elif processed == 0 and total_posts > 0:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"⚠️ **Hech qanday xabar jo'natilmadi**\n"
                    f"So'ralgan: {total_posts}\n"
                    f"❌ Xato: {failed}{deleted_info}"
                    f"{failed_info}"
                )
            elif failed > 0:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"✅ **Tugadi**\n"
                    f"Yuklandi: {processed}/{total_posts}\n"
                    f"📁 Albumlar: {albums_processed}\n"
                    f"❌ Xato: {failed}{deleted_info}"
                    f"{failed_info}"
                )
            else:
                await client.edit_message_text(
                    user_id, status_msg.id,
                    f"✅ **Tugadi**\n"
                    f"Yuklandi: {processed}/{total_posts}\n"
                    f"📁 Albumlar: {albums_processed}{deleted_info}"
                )
            
            # Unpin status message when done
            if total_posts > 1 and status_msg:
                try:
                    await client.unpin_chat_message(user_id, status_msg.id)
                except Exception:
                    pass

            # BatchController cleanup
            if _batch:
                _batch.finish()

    except asyncio.CancelledError:
        if _batch:
            _batch.finish()
        if status_msg:
            try:
                await client.edit_message_text(user_id, status_msg.id, "❌ Bekor qilindi.")
                await client.unpin_chat_message(user_id, status_msg.id)
            except Exception:
                pass


@track_activity
async def process_public_posts(client: Client, message: Message, parsed: ParsedURL, context: Optional[TaskContext] = None):
    """Process public channel posts sequentially (single client per task)"""
    ctx = context or _new_task_context(message, parsed, client_type="bot")
    user_id = ctx.user_id
    
    async with StopSafePipeline(user_id, task_manager) as pipeline:
        total_posts = len(parsed.post_ids)
        status_msg = await client.send_message(
            user_id, f"Processing {total_posts} public posts...",
            **build_reply_kwargs_from_message(message)
        )
        
        processed = 0
        _p_batch = BatchController(user_id) if _THROTTLE_AVAILABLE else None

        user_data = await async_db.find_user(user_id)
        use_user_session = bool(get(user_data, 'logged_in', False) and user_data.get('session'))

        if use_user_session:
            session_string = user_data['session']
            async with create_user_session(
                session_string, user_id,
                peers_to_resolve=[parsed.channel_id],
            ) as acc:
                await _resolve_peer_safe(acc, parsed.channel_id, ctx)
                for idx, post_id in enumerate(parsed.post_ids):
                    await ping_activity(user_id)
                    if await pipeline.check_cancelled():
                        await client.edit_message_text(user_id, status_msg.id, f"Cancelled. Processed: {processed}")
                        break

                    if _p_batch:
                        pause = _p_batch.check_pause(idx)
                        if pause > 0:
                            await asyncio.sleep(pause)

                    try:
                        result = await process_single_post(
                            client, acc, message, parsed.channel_id, post_id, None, pipeline,
                            session_string=session_string,
                            context=ctx.with_client_type("user_session"),
                        )
                        if result and result != "deleted":
                            processed += 1
                            if _p_batch:
                                _p_batch.record_success()
                        else:
                            if _p_batch:
                                _p_batch.record_error()
                    except FloodWait as fw:
                        wait_time = getattr(fw, 'value', getattr(fw, 'x', 30))
                        logger.warning(f"FloodWait {wait_time}s at public post {post_id}")
                        await _notify_realtime_post_failure(
                            client,
                            acc,
                            user_id=user_id,
                            chat_id=parsed.channel_id,
                            post_id=post_id,
                            stage="public_floodwait",
                            reason=f"FloodWait {wait_time}s",
                            error=fw,
                            request_message=message,
                            context=ctx,
                            parsed=parsed,
                            requested_total=total_posts,
                        )
                        if _p_batch:
                            _p_batch.record_flood(wait_time)
                        if wait_time > 120:
                            try:
                                await client.edit_message_text(
                                    user_id, status_msg.id,
                                    f"⚠️ **FloodWait {wait_time}s** — batch paused.\n"
                                    f"✅ {processed}/{total_posts} processed."
                                )
                            except Exception:
                                pass
                            break
                        else:
                            await asyncio.sleep(wait_time)
                    except Exception as _pub_err:
                        logger.debug("Public post process error: %s", _pub_err)
                        await _notify_realtime_post_failure(
                            client,
                            acc,
                            user_id=user_id,
                            chat_id=parsed.channel_id,
                            post_id=post_id,
                            stage="public_user_session",
                            reason="public post processing raised exception",
                            error=_pub_err,
                            request_message=message,
                            context=ctx,
                            parsed=parsed,
                            requested_total=total_posts,
                        )
                        if _p_batch:
                            _p_batch.record_error()

                    if _p_batch and _p_batch.should_stop():
                        break

                    await asyncio.sleep(2)
        else:
            for idx, post_id in enumerate(parsed.post_ids):
                await ping_activity(user_id)
                if await pipeline.check_cancelled():
                    await client.edit_message_text(user_id, status_msg.id, f"Cancelled. Processed: {processed}")
                    break

                if _p_batch:
                    pause = _p_batch.check_pause(idx)
                    if pause > 0:
                        await asyncio.sleep(pause)

                try:
                    msg = await client.get_messages(parsed.channel_id, post_id)
                    await client.copy_message(user_id, msg.chat.id, msg.id, **build_reply_kwargs_from_message(message))
                    processed += 1
                    if _p_batch:
                        _p_batch.record_success()
                except FloodWait as fw:
                    wait_time = getattr(fw, 'value', getattr(fw, 'x', 30))
                    logger.warning(f"FloodWait {wait_time}s at public post {post_id}")
                    await _notify_bot_only_post_failure(
                        client,
                        user_id=user_id,
                        chat_id=parsed.channel_id,
                        post_id=post_id,
                        stage="public_bot_floodwait",
                        reason=f"FloodWait {wait_time}s",
                        error=fw,
                        request_message=message,
                        parsed=parsed,
                        requested_total=total_posts,
                    )
                    if _p_batch:
                        _p_batch.record_flood(wait_time)
                    if wait_time > 120:
                        try:
                            await client.edit_message_text(
                                user_id, status_msg.id,
                                f"⚠️ **FloodWait {wait_time}s** — batch paused.\n"
                                f"✅ {processed}/{total_posts} processed."
                            )
                        except Exception:
                            pass
                        break
                    else:
                        await asyncio.sleep(wait_time)
                except Exception as _bot_err:
                    logger.debug("Public post bot copy error: %s", _bot_err)
                    await _notify_bot_only_post_failure(
                        client,
                        user_id=user_id,
                        chat_id=parsed.channel_id,
                        post_id=post_id,
                        stage="public_bot_copy",
                        reason="public bot copy raised exception",
                        error=_bot_err,
                        request_message=message,
                        parsed=parsed,
                        requested_total=total_posts,
                    )
                    if _p_batch:
                        _p_batch.record_error()

                if _p_batch and _p_batch.should_stop():
                    break

                await asyncio.sleep(2)

        if _p_batch:
            _p_batch.finish()
        
        if processed == 0 and total_posts > 0:
            await client.edit_message_text(
                user_id, status_msg.id,
                f"⚠️ **No messages retrieved**\nRequested: {total_posts}\n\nMessages may have been deleted or are inaccessible."
            )
        elif processed < total_posts:
            await client.edit_message_text(
                user_id, status_msg.id,
                f"✅ **Completed with warnings**\nProcessed: {processed}/{total_posts}\nSome messages could not be retrieved."
            )
        else:
            await client.edit_message_text(user_id, status_msg.id, f"✅ **Completed**\nProcessed: {processed}/{total_posts}")


@track_activity
async def process_bot_posts(client: Client, message: Message, parsed: ParsedURL, context: Optional[TaskContext] = None):
    """Process bot chat posts sequentially"""
    ctx = context or _new_task_context(message, parsed, client_type="user_session")
    user_id = ctx.user_id
    
    user_data = await async_db.find_user(user_id)
    if not get(user_data, 'logged_in', False) or not user_data.get('session'):
        await client.send_message(user_id, strings['need_login'], **build_reply_kwargs_from_message(message))
        return
    
    acc, error = await create_client_session(user_data['session'])
    if error:
        await client.send_message(user_id, f"Connection error: {error}", **build_reply_kwargs_from_message(message))
        return
    
    try:
        async with StopSafePipeline(user_id, task_manager) as pipeline:
            for post_id in parsed.post_ids:
                if await pipeline.check_cancelled():
                    return
                await process_single_post(
                    client, acc, message, parsed.channel_id, post_id, None, pipeline,
                    session_string=user_data['session'],
                    context=ctx,
                )
                await asyncio.sleep(2)
    finally:
        await safe_disconnect(acc)


@track_activity
async def process_single_post(
    client: Client,
    acc,
    message: Optional[Message],
    chat_id,
    post_id: int,
    temp_dir: Optional[str],
    pipeline: StopSafePipeline,
    target_user_id: int = None,
    session_string: Optional[str] = None,
    context: Optional[TaskContext] = None,
) -> bool:
    """
    Process a single NON-ALBUM post completely before returning.
    
    NOTE: Album detection and processing is now handled by the caller
    using detect_album_boundary() and process_album_pipeline().
    This function only handles single messages.
    
    Args:
        message: Original user message (can be None for queued items)
        target_user_id: User ID to send to (used when message is None)
    """
    user_id = context.user_id if context else (target_user_id if target_user_id else (message.chat.id if message else None))
    if not user_id:
        logger.error("process_single_post: no user_id available")
        return False

    async def _notify_failure(stage: str, reason: str, error: Any = None, source_msg: Any = None) -> None:
        try:
            if await pipeline.check_cancelled():
                return
        except Exception:
            pass
        await _notify_realtime_post_failure(
            client,
            acc,
            user_id=user_id,
            chat_id=chat_id,
            post_id=post_id,
            stage=stage,
            reason=reason,
            error=error,
            source_msg=source_msg,
            request_message=message,
            context=context,
        )
    
    try:
        # Check cancellation
        if await pipeline.check_cancelled():
            return False
        
        # Get the message with timeout
        try:
            await _resolve_peer_safe(acc, chat_id, context)
            msg = await asyncio.wait_for(
                acc.get_messages(chat_id, post_id),
                timeout=30.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching message {post_id}")
            await _notify_failure("fetch", "get_messages timed out")
            return False
        
        if not msg or msg.empty:
            await _notify_failure("fetch", "post is deleted or inaccessible", source_msg=msg)
            return "deleted"
        
        msg_type = get_message_type(msg)
        
        # If message is None (queued item), create a proxy so all handlers
        # work identically to the interactive (user-sent) flow.
        if message is None:
            from types import SimpleNamespace
            message = SimpleNamespace(
                chat=SimpleNamespace(id=user_id),
                id=None,          # No message to reply to
                from_user=SimpleNamespace(id=user_id),
            )
        
        # Handle location messages
        if getattr(msg, 'location', None):
            loc = msg.location
            try:
                target_chat_id = _get_user_session_target_chat_id(client, user_id)
                loc_msg = await acc.send_location(
                    chat_id=target_chat_id,
                    latitude=loc.latitude,
                    longitude=loc.longitude,
                )
                # Copy to user with reply context (BUG-018 fix)
                if loc_msg and message and getattr(message, 'id', None):
                    try:
                        await client.copy_message(
                            chat_id=user_id,
                            from_chat_id=loc_msg.chat.id,
                            message_id=loc_msg.id,
                            **build_reply_kwargs_from_message(message),
                        )
                        await client.delete_messages(loc_msg.chat.id, [loc_msg.id])
                    except Exception:
                        pass  # Intermediate msg still visible
                return True
            except Exception as e:
                logger.warning(f"send_location error: {e}")
                await _notify_failure("send_location", "location send/copy failed", e, msg)
                return False

        # Handle venue messages
        if getattr(msg, 'venue', None):
            venue = msg.venue
            try:
                target_chat_id = _get_user_session_target_chat_id(client, user_id)
                venue_msg = await acc.send_venue(
                    chat_id=target_chat_id,
                    latitude=venue.location.latitude,
                    longitude=venue.location.longitude,
                    title=venue.title,
                    address=venue.address,
                )
                # Copy to user with reply context (BUG-018 fix)
                if venue_msg and message and getattr(message, 'id', None):
                    try:
                        await client.copy_message(
                            chat_id=user_id,
                            from_chat_id=venue_msg.chat.id,
                            message_id=venue_msg.id,
                            **build_reply_kwargs_from_message(message),
                        )
                        await client.delete_messages(venue_msg.chat.id, [venue_msg.id])
                    except Exception:
                        pass
                return True
            except Exception as e:
                logger.warning(f"send_venue error: {e}")
                await _notify_failure("send_venue", "venue send/copy failed", e, msg)
                return False

        # Handle text messages
        if msg_type == "Text":
            # Check for QuizBot links in text messages - handle EXCLUSIVELY
            if msg.reply_markup:
                quizbot_links = extract_quizbot_links(msg.reply_markup)
                if quizbot_links:
                    # QuizBot post - handle separately, DO NOT also send as text
                    result = await handle_quizbot_post(client, message, msg)
                    if not result:
                        await _notify_failure("send_quizbot", "QuizBot post handler returned False", source_msg=msg)
                    return result
            # Regular text message (no QuizBot)
            result = await send_text_message(client, message, msg)
            if not result:
                await _notify_failure("send_text", "text send returned False", source_msg=msg)
            return result
        
        # Handle polls with auto-vote
        if msg_type == "Poll":
            result = await handle_poll(
                client, message, msg.poll, 
                source_msg=msg, 
                user_session=acc,  # Pass user session for auto-voting
                auto_vote=True
            )
            if not result:
                await _notify_failure("send_poll", "poll handler returned False", source_msg=msg)
            return result
        
        # Handle single media (photos without album, videos, documents, etc.)
        # NOTE: Album photos are handled by process_album_pipeline before reaching here
        result = await download_and_send_media(
            client, acc, message, msg, msg_type, temp_dir, pipeline,
            session_string=session_string,
            context=context,
        )
        if not result:
            await _notify_failure("send_media", "download_and_send_media returned False", source_msg=msg)
        return result
        
    except asyncio.CancelledError:
        raise
    except FloodWait:
        # Re-raise FloodWait so the outer per-post loop can handle it
        raise
    except Exception as e:
        logger.warning(f"Error processing post {post_id}: {e}")
        await _notify_failure("process_single_post", "unexpected exception", e)
        return False


async def _send_post_direct(client: Client, acc, user_id: int, msg, msg_type: str, temp_dir: Optional[str]) -> bool:
    """
    Direct send for queued items (when original message object is not available).
    Simplified version that sends content directly to user.
    """
    try:
        # Check for QuizBot links in ANY message type (text, poll, etc.)
        if msg.reply_markup:
            quizbot_links = extract_quizbot_links(msg.reply_markup)
            if quizbot_links:
                return await _send_quizbot_direct(client, user_id, msg, quizbot_links)
        
        # Text message — bot client (user session would trigger link handler loop)
        if msg_type == "Text":
            text = msg.text or ""
            entities = list(msg.entities) if msg.entities else None
            reply_markup = msg.reply_markup if hasattr(msg, 'reply_markup') else None
            await client.send_message(user_id, text, entities=entities, reply_markup=reply_markup)
            return True

        # Poll - convert to text (bot client)
        if msg_type == "Poll" and msg.poll:
            poll = msg.poll
            text = f"📊 **{poll.question}**\n\n"
            for i, opt in enumerate(poll.options):
                text += f"{chr(65+i)}) {opt.text}\n"
            await client.send_message(user_id, text)
            return True

        # Media - download and send via user session
        if msg.media:
            file_path = None
            try:
                download_dir = temp_dir or "downloads/temp"
                file_path = await acc.download_media(msg, file_name=f"{download_dir}/")
                if file_path and os.path.exists(file_path):
                    caption = str(msg.caption) if hasattr(msg, 'caption') and msg.caption else None
                    caption_entities = list(msg.caption_entities) if hasattr(msg, 'caption_entities') and msg.caption_entities else None
                    target_chat_id = _get_user_session_target_chat_id(client, user_id)

                    # Send based on type via user session
                    if msg_type == "Photo":
                        await acc.send_photo(target_chat_id, file_path, caption=caption, caption_entities=caption_entities)
                    elif msg_type == "Video":
                        await acc.send_video(target_chat_id, file_path, caption=caption, caption_entities=caption_entities)
                    elif msg_type == "Audio":
                        await acc.send_audio(target_chat_id, file_path, caption=caption, caption_entities=caption_entities)
                    elif msg_type == "Voice":
                        await acc.send_voice(target_chat_id, file_path, caption=caption, caption_entities=caption_entities)
                    elif msg_type == "VideoNote":
                        await acc.send_video_note(target_chat_id, file_path)
                    else:
                        await acc.send_document(
                            target_chat_id, file_path,
                            caption=caption,
                            caption_entities=caption_entities,
                            force_document=True,
                            file_name=msg.document.file_name if msg.document and msg.document.file_name else None,
                        )
                    return True
            finally:
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass

        return False
    except Exception as e:
        logger.warning(f"_send_post_direct error: {e}")
        return False


async def _send_quizbot_direct(client: Client, user_id: int, msg, quizbot_links: list) -> bool:
    """
    Send QuizBot post directly to user (for queued items).
    Preserves text, entities, inline buttons, and QuizBot links.
    """
    try:
        parts = []
        if msg.text:
            parts.append(msg.text)
        elif msg.caption:
            parts.append(msg.caption)

        if quizbot_links:
            parts.append("")
            parts.append("🤖 **QuizBot Start:**")
            for link in quizbot_links:
                parts.append(link)

        if not parts:
            return False

        combined_text = "\n".join(parts)

        # Keep non-QuizBot buttons
        reply_markup = None
        if msg.reply_markup and hasattr(msg.reply_markup, 'inline_keyboard'):
            filtered_rows = []
            for row in msg.reply_markup.inline_keyboard:
                filtered_buttons = []
                for button in row:
                    if hasattr(button, 'url') and button.url:
                        if 'quizbot' in button.url.lower() and 'start=' in button.url.lower():
                            continue
                    filtered_buttons.append(button)
                if filtered_buttons:
                    filtered_rows.append(filtered_buttons)
            if filtered_rows:
                reply_markup = InlineKeyboardMarkup(filtered_rows)

        await client.send_message(
            user_id,
            combined_text,
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
        return True
    except Exception as e:
        logger.warning(f"_send_quizbot_direct error: {e}")
        return False


async def send_text_message(client: Client, message: Message, msg) -> bool:
    """
    Send a text message using MessageEntity (NO MARKDOWN).

    Text messages are always sent via the BOT client.
    Sending text via user session risks triggering the bot's link handler
    if the text contains Telegram URLs (creates duplicate download loop).
    Auto-splits for 4096 char limit while preserving entities.
    """
    # Text always goes through bot client — never user session
    target = message.chat.id

    try:
        from core.text_renderer import extract_to_renderer

        reply_markup = msg.reply_markup if hasattr(msg, 'reply_markup') else None
        reply_kwargs = build_reply_kwargs_from_message(message)

        # Get text and entities directly (NOT Markdown formatted)
        text, entities = get_text_with_entities(msg)
        if not text:
            return True  # Empty text is ok

        # Convert to renderer and send with auto-splitting
        renderer = extract_to_renderer(text, entities or [])

        # Render chunks
        chunks = renderer.render_chunks(TELEGRAM_MESSAGE_LIMIT)

        for i, (chunk_text, chunk_entities) in enumerate(chunks):
            if not chunk_text:
                continue

            await client.send_message(
                chat_id=target,
                text=chunk_text,
                entities=chunk_entities if chunk_entities else None,
                parse_mode=ParseMode.DISABLED,
                reply_markup=reply_markup if i == len(chunks) - 1 else None,
                **reply_kwargs,
            )
        return True
    except FloodWait:
        # Re-raise FloodWait so the outer per-post loop can handle it
        raise
    except Exception as e:
        logger.warning(f"send_text_message error: {e}")
        # Fallback: send as plain text without entities
        try:
            text = getattr(msg, 'text', None) or getattr(msg, 'caption', None) or ""
            if text:
                chunks = [text[i:i+TELEGRAM_MESSAGE_LIMIT] for i in range(0, len(text), TELEGRAM_MESSAGE_LIMIT)]
                for i, chunk in enumerate(chunks):
                    await client.send_message(
                        chat_id=target,
                        text=chunk,
                        **reply_kwargs,
                    )
                return True
        except FloodWait:
            raise
        except Exception as e2:
            logger.warning(f"send_text_message fallback error: {e2}")
        return False


@track_activity
async def download_and_send_media(
    client: Client,
    acc,
    message: Message,
    msg,
    msg_type: str,
    temp_dir: Optional[str],
    pipeline: StopSafePipeline,
    session_string: Optional[str] = None,
    context: Optional[TaskContext] = None,
) -> bool:
    """
    Download and send media file using core download engine.
    
    Returns:
        bool: True if successful, False otherwise
    
    Uses:
    - core.downloader.DownloadEngine for downloads
    - progress_tracker for progress updates
    - ReplyParameters (not deprecated reply_to_message_id)
    """
    file_path = None
    thumb_path = None
    status_msg = None
    ctx_user_id = context.user_id if context else message.chat.id
    ctx_target_chat_id = context.target_chat_id if context else message.chat.id

    try:
        # Check cancellation
        if await pipeline.check_cancelled():
            return False

        # EARLY EXIT: if the message has no actual downloadable media object,
        # it is a web-page preview or text-only message. Never send it through
        # the downloader pipeline — that would trigger 5× retry loops with
        # "No downloadable media" spam.
        from core.media_classifier import has_downloadable_media
        if not has_downloadable_media(msg):
            logger.info(
                "download_and_send_media: msg %s has no downloadable media "
                "(likely web preview) — routing to text path", getattr(msg, "id", "?")
            )
            # Route to text send directly
            text = getattr(msg, "text", "") or getattr(msg, "caption", "") or ""
            raw_entities = (
                list(getattr(msg, "entities", None) or [])
                or list(getattr(msg, "caption_entities", None) or [])
            )
            if text:
                from core.entity_rebuilder import strip_custom_emoji_entities, validate_entities
                text, ents = strip_custom_emoji_entities(text, raw_entities)
                ents = validate_entities(text, ents)
                await client.send_message(
                    ctx_target_chat_id,
                    text=text,
                    entities=ents if ents else None,
                    parse_mode=ParseMode.DISABLED,
                    disable_web_page_preview=True,
                    **build_reply_kwargs_from_message(message),
                )
            return True
        file_size = get_file_size(msg, msg_type)
        show_progress = file_size > 50 * 1024 * 1024  # Avoid edit FloodWait on small transfers
        
        if show_progress:
            size_mb = file_size / 1024 / 1024
            status_msg = await client.send_message(
                ctx_target_chat_id, 
                f"**Downloading**\n{size_mb:.1f} MB",
                **build_reply_kwargs_from_message(message)
            )
        
        # Download using core engine
        if temp_dir:
            download_path = temp_dir
        else:
            download_path = TEMP_DOWNLOAD_DIR
            os.makedirs(download_path, exist_ok=True)
        
        # Use download engine with progress tracking
        # CRITICAL: bot_client is used for editing progress messages
        # because status_msg was sent by the bot, not the user session
        engine = await get_engine()
        file_path = await engine.download(
            message=msg,
            client=acc,
            status_message=status_msg if show_progress else None,
            user_id=ctx_user_id,
            download_dir=download_path,
            bot_client=client  # Bot client for progress message editing
        )
        
        if not file_path or not os.path.exists(file_path):
            if status_msg:
                await client.delete_messages(ctx_target_chat_id, [status_msg.id])
            return False
        
        # Check cancellation before upload
        if await pipeline.check_cancelled():
            return False

        split_target_chat_id = _get_user_session_target_chat_id(client, ctx_target_chat_id)
        local_video_meta = None
        if msg_type == "Video" and is_video_file(file_path):
            local_video_meta, thumb_path = await _get_local_video_artifacts(file_path, msg)
            if not thumb_path:
                thumb_path = await get_thumbnail(acc, msg)
        elif msg_type == "Document":
            thumb_path = await get_thumbnail(acc, msg)
        
        # Get caption with entities early - needed by both split and normal paths
        caption, caption_entities = get_caption_with_entities(msg)

        # Early premium check — needed for caption limit and custom emoji decisions
        user_id = ctx_user_id
        _user_is_premium = False
        _premium_session_str = None
        try:
            from core.premium_logic import check_user_premium
            _user_is_premium = await check_user_premium(client, user_id)
            if _user_is_premium:
                _udata = await async_db.find_user(user_id)
                _premium_session_str = _udata.get('session') if _udata else None
        except Exception as _prem_early_err:
            logger.debug(f"Early premium check failed: {_prem_early_err}")

        # Premium-aware splitting decision
        actual_file_size = os.path.getsize(file_path)
        _do_split = True  # Default: split if >2GB
        if actual_file_size > MAX_TELEGRAM_FILE_SIZE:
            from TechVJ.lang import get_string

            # Determine if Premium path can bypass splitting
            _do_split = True
            try:
                from core.premium_logic import (
                    should_split, get_user_upload_setting,
                )
                _user_setting = get_user_upload_setting(user_id)

                # Hybrid mode: no-split is allowed ONLY with user's own Premium session.
                # System premium/relay must not unlock no-split.
                _do_split = should_split(_user_is_premium, False, _user_setting, actual_file_size)

                if not _do_split and not _premium_session_str:
                    # Premium user but no session stored — fall back to split
                    _do_split = True
            except Exception as _prem_err:
                logger.warning(f"Premium check failed, defaulting to split: {_prem_err}")
                _do_split = True

            if _do_split:
                # Standard split path — unchanged behavior
                if status_msg:
                    if is_video_file(file_path):
                        split_msg = get_string(user_id, "splitting_ffmpeg")
                    else:
                        split_msg = get_string(user_id, "splitting_binary")
                    await _safe_status_edit_message(
                        client,
                        ctx_target_chat_id,
                        status_msg.id,
                        get_string(user_id, "file_too_large", size=format_size(actual_file_size)) + f"\n{split_msg}",
                    )

                try:
                    split_func = split_large_video if is_video_file(file_path) else split_file
                    chunk_paths = await asyncio.to_thread(split_func, file_path)
                    if await pipeline.check_cancelled():
                        cleanup_chunks(chunk_paths)
                        return False
                    total_parts = len(chunk_paths)

                    for i, chunk_path in enumerate(chunk_paths):
                        if await pipeline.check_cancelled():
                            cleanup_chunks(chunk_paths[i:])
                            return False

                        part_num = i + 1
                        chunk_name = os.path.basename(chunk_path)
                        chunk_size = os.path.getsize(chunk_path)

                        if status_msg:
                            await _safe_status_edit_message(
                                client,
                                ctx_target_chat_id,
                                status_msg.id,
                                get_string(user_id, "uploading_part", part=part_num, total=total_parts, name=chunk_name, size=format_size(chunk_size)),
                            )

                        part_caption = _build_split_part_caption(
                            part_num=part_num,
                            total_parts=total_parts,
                            chunk_size=chunk_size,
                            caption=caption,
                        )

                        part_thumb_path = None
                        if msg_type != "Document" and is_video_file(chunk_path):
                            part_video_meta, part_thumb_path = await _get_local_video_artifacts(chunk_path, msg)
                            part_video_kwargs = dict(build_reply_kwargs_from_message(message))
                            part_video_kwargs["caption"] = part_caption
                            part_video_kwargs["supports_streaming"] = True
                            if part_thumb_path:
                                part_video_kwargs["thumb"] = part_thumb_path
                            if part_video_meta.get("duration") is not None:
                                part_video_kwargs["duration"] = part_video_meta["duration"]
                            if part_video_meta.get("width") is not None:
                                part_video_kwargs["width"] = part_video_meta["width"]
                            if part_video_meta.get("height") is not None:
                                part_video_kwargs["height"] = part_video_meta["height"]
                            await acc.send_video(
                                split_target_chat_id,
                                chunk_path,
                                **part_video_kwargs,
                            )
                        else:
                            await acc.send_document(
                                split_target_chat_id, chunk_path,
                                caption=part_caption,
                                force_document=True,
                                **build_reply_kwargs_from_message(message)
                            )

                        try:
                            os.remove(chunk_path)
                        except Exception:
                            pass
                        if part_thumb_path and os.path.exists(part_thumb_path):
                            try:
                                os.remove(part_thumb_path)
                            except Exception:
                                pass

                        if part_num < total_parts:
                            if await pipeline.check_cancelled():
                                cleanup_chunks(chunk_paths[i + 1:])
                                return False
                            await asyncio.sleep(2)

                    if status_msg:
                        await _safe_status_edit_message(
                            client,
                            ctx_target_chat_id,
                            status_msg.id,
                            get_string(user_id, "split_complete", parts=total_parts),
                        )
                    return True

                except Exception as split_err:
                    logger.error(f"Large file split error: {split_err}")
                    if status_msg:
                        await _safe_status_edit_message(
                            client,
                            ctx_target_chat_id,
                            status_msg.id,
                            get_string(user_id, "error_split", error=str(split_err)[:100]),
                        )
                    return False
        
        if status_msg:
            from TechVJ.lang import get_string
            await _safe_status_edit_message(
                client,
                ctx_target_chat_id,
                status_msg.id,
                get_string(ctx_user_id, "uploading"),
            )
        
        # caption and caption_entities already extracted above (before split check)
        overflow_text = None
        overflow_entities = None

        # CORRECT UTF-16 CAPTION SPLITTING using caption_splitter
        if caption:
            try:
                from core.caption_splitter import split_caption as _split_caption
                _primary, _overflow_chunks = _split_caption(
                    caption, caption_entities or [],
                    is_premium=bool(_premium_session_str),
                )
                caption = _primary.text
                caption_entities = _primary.entities if _primary.entities else None
                logger.debug(f"Caption split: primary={len(caption)} chars, overflow={len(_overflow_chunks)}")

                if _overflow_chunks:
                    overflow_text = _overflow_chunks[0].text
                    overflow_entities = _overflow_chunks[0].entities if _overflow_chunks[0].entities else None
                    if len(_overflow_chunks) > 1:
                        extra = "\n\n".join(c.text for c in _overflow_chunks[1:])
                        overflow_text = overflow_text + "\n\n" + extra
            except Exception as _split_err:
                logger.warning(f"caption split failed: {_split_err}")
                # Truncate caption instead of crashing
                if caption and len(caption.encode('utf-16-le', errors='surrogatepass')) // 2 > 1024:
                    from core.utf16_utils import utf16_to_char_index
                    caption = caption[:utf16_to_char_index(caption, 1024)]
                caption_entities = None

        # FINAL entity bounds validation against the (possibly split) caption
        from core.entity_rebuilder import validate_entities as _validate_ents
        if caption_entities and caption:
            caption_entities = _validate_ents(caption, caption_entities)
            if not caption_entities:
                caption_entities = None
        
        reply_markup = msg.reply_markup if hasattr(msg, 'reply_markup') else None
        reply_kwargs = build_reply_kwargs_from_message(message)
        
        # Determine if this upload should go through Premium session
        # _premium_session_str is set above when file >2GB and Premium path was chosen
        _use_premium = (
            actual_file_size > MAX_TELEGRAM_FILE_SIZE
            and not _do_split
            and _premium_session_str is not None
        )

        if _use_premium:
            # Premium upload path — file >2GB, no splitting.
            # Hybrid mode: upload media directly from user's Premium MTProto session.
            from core.user_upload_worker import (
                worker_registry as _wr,
                UploadTask as _UT,
                make_send_fn as _msf,
            )

            _send_kwargs = {}
            if caption:
                _send_kwargs['caption'] = caption
            if caption_entities:
                _send_kwargs['caption_entities'] = caption_entities
            if reply_markup:
                _send_kwargs['reply_markup'] = reply_markup
            _send_kwargs.update(reply_kwargs)
            if msg_type == "Video" and local_video_meta:
                if local_video_meta.get("duration") is not None:
                    _send_kwargs["duration"] = local_video_meta["duration"]
                if local_video_meta.get("width") is not None:
                    _send_kwargs["width"] = local_video_meta["width"]
                if local_video_meta.get("height") is not None:
                    _send_kwargs["height"] = local_video_meta["height"]
            if thumb_path:
                _send_kwargs["thumb"] = thumb_path

            _video_meta = None
            _thumb_path = thumb_path
            upload_cb = None
            _doc_file_name = None
            if msg_type == "Video":
                _video_meta = local_video_meta or None
            elif msg_type == "Document":
                if msg.document:
                    _doc_file_name = msg.document.file_name
            if show_progress and status_msg and msg_type in ("Video", "Audio", "Document"):
                engine = await get_engine()
                upload_cb = engine.create_progress_callback(client, status_msg, "upload")

            _target_chat_id = client.me.id
            _worker = await _wr.get_or_create(
                user_id=ctx_user_id,
                session_string=_premium_session_str,
                bot_id=_target_chat_id,
                bot_username=getattr(client.me, "username", None),
            )

            _send_fn = _msf(
                target_chat_id=_target_chat_id,
                msg_type=msg_type,
                file_path=file_path,
                send_kwargs=_send_kwargs,
                video_meta=_video_meta,
                thumb_path=_thumb_path,
                progress_cb=upload_cb,
                doc_file_name=_doc_file_name,
            )

            try:
                await _worker.enqueue(_UT(send_fn=_send_fn, is_media=True))
            except Exception as _prem_direct_err:
                logger.warning(
                    "Direct premium worker upload failed for user %d; falling back to split: %s",
                    ctx_user_id,
                    _prem_direct_err,
                )
                from TechVJ.lang import get_string as _gs
                _uid = ctx_user_id
                if status_msg:
                    await _safe_status_edit_message(
                        client,
                        _uid,
                        status_msg.id,
                        _gs(_uid, "splitting_binary"),
                    )
                try:
                    _split_func = split_large_video if is_video_file(file_path) else split_file
                    _chunks = await asyncio.to_thread(_split_func, file_path)
                    if await pipeline.check_cancelled():
                        cleanup_chunks(_chunks)
                        return False
                    _total = len(_chunks)
                    for _i, _cp in enumerate(_chunks):
                        if await pipeline.check_cancelled():
                            cleanup_chunks(_chunks[_i:])
                            return False

                        _pn = _i + 1
                        _pcap = _build_split_part_caption(
                            part_num=_pn,
                            total_parts=_total,
                            chunk_size=os.path.getsize(_cp),
                            caption=caption,
                        )

                        if status_msg:
                            await _safe_status_edit_message(
                                client,
                                _uid,
                                status_msg.id,
                                _gs(_uid, "uploading_part", part=_pn, total=_total, name=os.path.basename(_cp), size=format_size(os.path.getsize(_cp))),
                            )

                        _part_thumb_path = None
                        if msg_type != "Document" and is_video_file(_cp):
                            _part_video_meta, _part_thumb_path = await _get_local_video_artifacts(_cp, msg)
                            _part_video_kwargs = dict(build_reply_kwargs_from_message(message))
                            _part_video_kwargs["caption"] = _pcap
                            _part_video_kwargs["supports_streaming"] = True
                            if _part_thumb_path:
                                _part_video_kwargs["thumb"] = _part_thumb_path
                            if _part_video_meta.get("duration") is not None:
                                _part_video_kwargs["duration"] = _part_video_meta["duration"]
                            if _part_video_meta.get("width") is not None:
                                _part_video_kwargs["width"] = _part_video_meta["width"]
                            if _part_video_meta.get("height") is not None:
                                _part_video_kwargs["height"] = _part_video_meta["height"]
                            await acc.send_video(
                                split_target_chat_id,
                                _cp,
                                **_part_video_kwargs,
                            )
                        else:
                            await acc.send_document(
                                split_target_chat_id,
                                _cp,
                                caption=_pcap,
                                force_document=True,
                                **build_reply_kwargs_from_message(message),
                            )
                        try:
                            os.remove(_cp)
                        except Exception:
                            pass
                        if _part_thumb_path and os.path.exists(_part_thumb_path):
                            try:
                                os.remove(_part_thumb_path)
                            except Exception:
                                pass
                        if _pn < _total:
                            if await pipeline.check_cancelled():
                                cleanup_chunks(_chunks[_i + 1:])
                                return False
                            await asyncio.sleep(2)
                except Exception as _fallback_err:
                    logger.error("Split fallback after direct premium failure: %s", _fallback_err)
                    return False
        else:
            # ── Direct MTProto upload via per-user worker ──────────────────
            # user session (acc) sends directly to target_chat_id.
            # target_chat_id = bot's Telegram user ID (client.me.id).
            # When acc sends to bot's user ID, the message lands in the
            # user's private chat with the bot — zero relay, zero Saved Messages.
            #
            # The worker keeps the session alive between tasks (fast),
            # serialises sends per-user (workers=1), and handles FloodWait
            # with adaptive rate limiting and batch pausing.
            from core.user_upload_worker import (
                worker_registry as _wr,
                UploadTask as _UT,
                make_send_fn as _msf,
                WorkerBotBlockedError as _WBBlocked,
            )

            # target = bot's Telegram user ID as seen from acc's perspective
            _target_chat_id = client.me.id
            _acc_user_id = ctx_user_id  # key for per-user worker

            # VIP-only access to global premium pipeline (pool/SessionManager)
            _allow_global_premium = True
            if _GOVERNANCE_AVAILABLE:
                try:
                    _role = await _role_manager.get_role(_acc_user_id)
                    _allow_global_premium = (_role == _UserRole.VIP_USER)
                except Exception:
                    _allow_global_premium = True

            # ── SessionManager path (additive — runs before legacy selection) ──
            # If the new session manager has any sessions registered and is
            # initialised, try those first.  On success, skip the legacy block.
            # On failure (no sessions / all flooded), fall through to legacy.
            _sm_handled = False
            try:
                from core.session_manager import session_manager as _sm_inst
                if _allow_global_premium and _sm_inst._initialized and _sm_inst.registry.get_all():
                    # Gather send_kwargs early so SM can use them
                    _sm_send_kwargs = dict(parse_mode=ParseMode.DISABLED, **reply_kwargs)
                    if reply_markup:
                        _sm_send_kwargs["reply_markup"] = reply_markup
                    if caption:
                        _sm_send_kwargs["caption"] = caption
                    if caption_entities:
                        _sm_send_kwargs["caption_entities"] = caption_entities
                    if msg_type == "Video" and local_video_meta:
                        if local_video_meta.get("duration") is not None:
                            _sm_send_kwargs["duration"] = local_video_meta["duration"]
                        if local_video_meta.get("width") is not None:
                            _sm_send_kwargs["width"] = local_video_meta["width"]
                        if local_video_meta.get("height") is not None:
                            _sm_send_kwargs["height"] = local_video_meta["height"]
                    if thumb_path:
                        _sm_send_kwargs["thumb"] = thumb_path
                    _sm_video_meta = None
                    _sm_thumb_path = thumb_path
                    _sm_upload_cb = None
                    _sm_doc_file_name = None
                    if msg_type == "Video":
                        if show_progress and status_msg:
                            _eng = await get_engine()
                            _sm_upload_cb = _eng.create_progress_callback(client, status_msg, "upload")
                        _sm_video_meta = local_video_meta or None
                    elif msg_type == "Document":
                        if msg.document:
                            _sm_doc_file_name = msg.document.file_name
                        if show_progress and status_msg:
                            _sm_upload_cb = (await get_engine()).create_progress_callback(
                                client, status_msg, "upload"
                            )
                    elif msg_type == "Audio":
                        if show_progress and status_msg:
                            _sm_upload_cb = (await get_engine()).create_progress_callback(
                                client, status_msg, "upload"
                            )
                    _sm_handled = await _sm_inst.upload_for_user(
                        user_id=_acc_user_id,
                        target_chat_id=_acc_user_id,
                        msg_type=msg_type,
                        file_path=file_path,
                        send_kwargs=_sm_send_kwargs,
                        bot_client=client,
                        bot_user_id=_target_chat_id,
                        video_meta=_sm_video_meta,
                        thumb_path=_sm_thumb_path,
                        progress_cb=_sm_upload_cb,
                        doc_file_name=_sm_doc_file_name,
                    )
                    if _sm_handled:
                        logger.info(
                            "SessionManager upload succeeded for user %d type=%s",
                            _acc_user_id, msg_type,
                        )
            except Exception as _sm_err:
                logger.debug("SessionManager path error (falling through): %s", _sm_err)

            if _sm_handled:
                # SM handled the upload — skip legacy session selection and upload.
                # Overflow text (if any) is sent below, outside this block.
                pass

            # Session selection priority (legacy — skipped when SM handled):
            #   1. System premium pool — try each available session, skip blocked ones
            #   2. User's own premium session (from early premium check)
            #   3. Fallback: user's regular session (non-premium)
            #
            # NOTE: `session_string` parameter from caller is the user's DB session.
            # Pool sessions take priority because they are Premium (can upload >2GB,
            # get higher rate limits from Telegram). User session is the last resort.
            _session_string = None
            _used_pool_idx = None

            # Step 1: Try system premium pool first
            if not _sm_handled and _allow_global_premium:
                try:
                    from core.premium_relay import relay_uploader as _ru
                    if _ru.pool and _ru.pool.count > 0:
                        _pool_entry = await _ru.pool.get_available()
                        if _pool_entry is not None:
                            _used_pool_idx, _session_string = _pool_entry
                            logger.debug(
                                "Using system pool session #%d for user %d upload",
                                _used_pool_idx + 1, _acc_user_id,
                            )
                            # Update request context with pool session info
                            if _REQUEST_CONTEXT_AVAILABLE:
                                from core.request_context import get_request_context
                                _rctx = get_request_context()
                                if _rctx:
                                    _set_req_ctx(_rctx.with_updates(
                                        sender_mode="pool_session",
                                        premium_session_id=f"pool_{_used_pool_idx}",
                                        worker_id=f"pool_{_used_pool_idx}",
                                    ))
                except Exception as _pool_err:
                    logger.debug("System pool lookup failed: %s", _pool_err)

            # Step 2: User's own premium session (if pool unavailable)
            if not _session_string and _premium_session_str:
                _session_string = _premium_session_str
                if _REQUEST_CONTEXT_AVAILABLE:
                    from core.request_context import get_request_context
                    _rctx = get_request_context()
                    if _rctx:
                        _set_req_ctx(_rctx.with_updates(
                            sender_mode="user_premium",
                            uploader_session_id=_premium_session_str[:16] if _premium_session_str else None,
                        ))

            # Step 3: User's regular session as last resort
            if not _session_string and not _sm_handled:
                _session_string = session_string or await acc.export_session_string()
                if _REQUEST_CONTEXT_AVAILABLE:
                    from core.request_context import get_request_context
                    _rctx = get_request_context()
                    if _rctx:
                        _set_req_ctx(_rctx.with_updates(
                            sender_mode="user_session",
                            uploader_session_id=_session_string[:16] if _session_string else None,
                        ))

            # Create worker — if pool session has bot blocked, try next pool sessions
            _worker = None
            _bot_username = getattr(client.me, "username", None)
            _max_pool_tries = 5
            for _attempt in range(_max_pool_tries if not _sm_handled else 0):
                try:
                    _worker = await _wr.get_or_create(
                        _acc_user_id, _session_string,
                        bot_id=_target_chat_id,
                        bot_username=_bot_username,
                    )
                    break
                except _WBBlocked:
                    # This pool session has the bot blocked — mark fatal and try next
                    if _used_pool_idx is not None:
                        try:
                            from core.premium_relay import relay_uploader as _ru2
                            await _ru2.pool.mark_fatal(_used_pool_idx)
                        except Exception:
                            pass
                        # Try next available pool session
                        try:
                            from core.premium_relay import relay_uploader as _ru2
                            _next = await _ru2.pool.get_available()
                            if _next:
                                _used_pool_idx, _session_string = _next
                                continue
                        except Exception:
                            pass
                    # Not from pool or pool exhausted — try auto-unblock via user session
                    _used_pool_idx = None
                    _session_string = await acc.export_session_string()
                    # Auto-unblock: user sessiyasi orqali botni unblock qilish
                    try:
                        from core.bot_unblock import try_unblock_bot as _try_unblock
                        _unblocked = await _try_unblock(
                            _session_string, _acc_user_id, _bot_username
                        )
                        if _unblocked:
                            logger.info(
                                "Auto-unblock succeeded for user %d — retrying worker",
                                _acc_user_id,
                            )
                    except Exception:
                        _unblocked = False
                    try:
                        _worker = await _wr.get_or_create(
                            _acc_user_id, _session_string,
                            bot_id=_target_chat_id,
                            bot_username=_bot_username,
                        )
                    except _WBBlocked:
                        pass
                    break

            if _worker is None and not _sm_handled:
                logger.error(
                    "All sessions blocked for user %d upload — cannot deliver",
                    _acc_user_id,
                )
                return False

            if not _sm_handled:
                logger.info(
                    "Upload via UserWorker: type=%s user=%d target=%d "
                    "caption_len=%d overflow=%s",
                    msg_type, _acc_user_id, _target_chat_id,
                    len(caption) if caption else 0,
                    "yes" if overflow_text else "no",
                )

            # Build send kwargs (caption + entities + reply)
            _send_kwargs = dict(parse_mode=ParseMode.DISABLED, **reply_kwargs)
            if reply_markup:
                _send_kwargs["reply_markup"] = reply_markup
            if caption:
                _send_kwargs["caption"] = caption
            if caption_entities:
                _send_kwargs["caption_entities"] = caption_entities
            if msg_type == "Video" and local_video_meta:
                if local_video_meta.get("duration") is not None:
                    _send_kwargs["duration"] = local_video_meta["duration"]
                if local_video_meta.get("width") is not None:
                    _send_kwargs["width"] = local_video_meta["width"]
                if local_video_meta.get("height") is not None:
                    _send_kwargs["height"] = local_video_meta["height"]
            if thumb_path:
                _send_kwargs["thumb"] = thumb_path

            # Video-specific metadata
            _video_meta = None
            _thumb_path = thumb_path
            _upload_cb = None
            _doc_file_name = None
            if msg_type == "Video":
                if show_progress and status_msg:
                    engine = await get_engine()
                    _upload_cb = engine.create_progress_callback(client, status_msg, "upload")
                _video_meta = local_video_meta or None
            elif msg_type == "Document":
                if msg.document:
                    _doc_file_name = msg.document.file_name
                if show_progress and status_msg:
                    _upload_cb = (await get_engine()).create_progress_callback(
                        client, status_msg, "upload"
                    )
            elif msg_type == "Audio":
                if show_progress and status_msg:
                    _upload_cb = (await get_engine()).create_progress_callback(
                        client, status_msg, "upload"
                    )

            # For pool sessions: upload to bot private chat of the POOL account,
            # then bot copies the message to the requesting user.
            # For user's own session: upload goes directly to user's bot chat.
            _is_pool_session = (_used_pool_idx is not None)
            _upload_send_kwargs, _direct_send_kwargs = _prepare_upload_send_kwargs(
                _send_kwargs,
                _is_pool_session,
            )

            _send_fn = _msf(
                target_chat_id=_target_chat_id,
                msg_type=msg_type,
                file_path=file_path,
                send_kwargs=_upload_send_kwargs,
                video_meta=_video_meta,
                thumb_path=_thumb_path,
                progress_cb=_upload_cb,
                doc_file_name=_doc_file_name,
            )

            async def _try_upload_with_pool_fallback():
                """Try upload, rotate pool sessions on FloodWait, ask user if all busy."""
                from core.premium_relay import relay_uploader as _ru_main
                _cur_worker = _worker
                _cur_pool_idx = _used_pool_idx
                _cur_sess = _session_string

                for _try in range(10):
                    try:
                        return await _enqueue_media_delivery(
                            worker=_cur_worker,
                            send_fn=_send_fn,
                            task_factory=_UT,
                            is_pool_session=(_cur_pool_idx is not None),
                            bot_client=client,
                            target_user_id=_acc_user_id,
                            request_message=message,
                        )
                    except FloodWait as _fw:
                        _fw_wait = getattr(_fw, "value", getattr(_fw, "x", 60))
                        if _cur_pool_idx is not None:
                            # Pause this pool session and try next
                            await _ru_main.pool.mark_flood(_cur_pool_idx, _fw_wait)
                            _nxt = await _ru_main.pool.get_available()
                            if _nxt is not None:
                                _cur_pool_idx, _cur_sess = _nxt
                                try:
                                    _cur_worker = await _wr.get_or_create(
                                        _acc_user_id, _cur_sess,
                                        bot_id=_target_chat_id,
                                        bot_username=_bot_username,
                                    )
                                    continue
                                except _WBBlocked:
                                    await _ru_main.pool.mark_fatal(_cur_pool_idx)
                                    # Auto-unblock: user sessiyasi orqali botni unblock
                                    try:
                                        from core.bot_unblock import try_unblock_bot as _try_unblock2
                                        _ub_sess = await acc.export_session_string()
                                        if await _try_unblock2(_ub_sess, _acc_user_id, _bot_username):
                                            logger.info("Auto-unblock in FloodWait recovery for user %d", _acc_user_id)
                                    except Exception:
                                        pass
                                    continue
                            # All pool sessions busy — ask user
                            _wait_sec = _ru_main.pool.next_available_in()
                            if _wait_sec is None:
                                # All fatal — no premium available at all
                                raise
                            _wait_min = max(1, int(_wait_sec / 60) + 1)
                            _POOL_TIMEOUT = 300  # 5 minutes user wait cap

                            # Inline keyboard: wait or continue without premium
                            from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                            import uuid as _uuid_mod
                            _cb_wait = f"pw_wait_{_acc_user_id}_{_uuid_mod.uuid4().hex[:8]}"
                            _cb_skip = f"pw_skip_{_acc_user_id}_{_uuid_mod.uuid4().hex[:8]}"
                            _kb = InlineKeyboardMarkup([[
                                InlineKeyboardButton(f"⏳ Kutaman ({_wait_min} daq)", callback_data=_cb_wait),
                                InlineKeyboardButton("⚡ Premiumsiz davom", callback_data=_cb_skip),
                            ]])
                            _ask_msg = await client.send_message(
                                _acc_user_id,
                                f"⚠️ Barcha premium sessiyalar band ({_wait_min} daqiqa).\n"
                                f"Nima qilamiz?",
                                reply_markup=_kb,
                            )

                            # Wait for callback (up to POOL_TIMEOUT seconds)
                            _choice = None
                            _deadline = asyncio.get_running_loop().time() + _POOL_TIMEOUT
                            _evt = asyncio.Event()

                            async def _cb_handler(_c, _cq):
                                nonlocal _choice
                                if _cq.from_user.id != _acc_user_id:
                                    return
                                if _cq.data in (_cb_wait, _cb_skip):
                                    _choice = _cq.data
                                    await _cq.answer()
                                    _evt.set()

                            _handler = client.add_handler(
                                __import__("pyrogram.handlers", fromlist=["CallbackQueryHandler"]).CallbackQueryHandler(_cb_handler),
                                group=99,
                            )
                            try:
                                remaining = _deadline - asyncio.get_running_loop().time()
                                await asyncio.wait_for(_evt.wait(), timeout=max(1, remaining))
                            except asyncio.TimeoutError:
                                _choice = _cb_skip  # timeout → proceed without premium
                            finally:
                                try:
                                    client.remove_handler(*_handler)
                                except Exception:
                                    pass
                                try:
                                    await client.delete_messages(_acc_user_id, [_ask_msg.id])
                                except Exception:
                                    pass

                            if _choice == _cb_skip or _choice is None:
                                # User chose to continue without premium
                                _fallback_sess = await acc.export_session_string()
                                try:
                                    _fallback_send_fn = _msf(
                                        target_chat_id=_target_chat_id,
                                        msg_type=msg_type,
                                        file_path=file_path,
                                        send_kwargs=_direct_send_kwargs,
                                        video_meta=_video_meta,
                                        thumb_path=_thumb_path,
                                        progress_cb=_upload_cb,
                                        doc_file_name=_doc_file_name,
                                    )
                                    _fb_worker = await _wr.get_or_create(
                                        _acc_user_id, _fallback_sess,
                                        bot_id=_target_chat_id,
                                        bot_username=_bot_username,
                                    )
                                    return await _enqueue_media_delivery(
                                        worker=_fb_worker,
                                        send_fn=_fallback_send_fn,
                                        task_factory=_UT,
                                        is_pool_session=False,
                                        bot_client=client,
                                        target_user_id=_acc_user_id,
                                        request_message=message,
                                    )
                                except Exception as _fb_err:
                                    logger.warning("Fallback non-premium upload failed: %s", _fb_err)
                                    raise _fw
                            else:
                                # User chose to wait — poll until a session is free
                                _poll_end = asyncio.get_running_loop().time() + _POOL_TIMEOUT
                                while asyncio.get_running_loop().time() < _poll_end:
                                    await asyncio.sleep(10)
                                    _nxt2 = await _ru_main.pool.get_available()
                                    if _nxt2 is not None:
                                        _cur_pool_idx, _cur_sess = _nxt2
                                        try:
                                            _cur_worker = await _wr.get_or_create(
                                                _acc_user_id, _cur_sess,
                                                bot_id=_target_chat_id,
                                                bot_username=_bot_username,
                                            )
                                            break
                                        except _WBBlocked:
                                            await _ru_main.pool.mark_fatal(_cur_pool_idx)
                                            # Auto-unblock: user sessiyasi orqali
                                            try:
                                                from core.bot_unblock import try_unblock_bot as _try_unblock3
                                                _ub_sess3 = await acc.export_session_string()
                                                if await _try_unblock3(_ub_sess3, _acc_user_id, _bot_username):
                                                    logger.info("Auto-unblock in wait-poll for user %d", _acc_user_id)
                                            except Exception:
                                                pass
                                else:
                                    raise _fw  # still no session after waiting
                                continue  # retry with new worker
                        else:
                            raise  # not a pool session — propagate

                raise RuntimeError("Upload retry limit exceeded")

            if not _sm_handled:
                await _try_upload_with_pool_fallback()

        # Send overflow caption chunks as separate messages.
        # overflow_text already contains the "📌 Davomi N/total" header from
        # caption_splitter. overflow_entities are pre-calculated for that text.
        #
        # IMPORTANT: Always send overflow via the BOT client (not user worker).
        # Sending text via user session to the bot would cause the bot to receive
        # the message as an update — if the caption contains Telegram links the
        # bot's link handler would re-process them, creating duplicate posts.
        if overflow_text:
            try:
                from core.entity_rebuilder import validate_entities as _validate_ents
                _ov_ents = _validate_ents(overflow_text, overflow_entities or [])
                _ov_target = ctx_target_chat_id

                await client.send_message(
                    _ov_target,
                    text=overflow_text,
                    entities=_ov_ents if _ov_ents else None,
                    parse_mode=ParseMode.DISABLED,
                )
            except Exception as overflow_err:
                logger.warning(f"Failed to send overflow caption: {overflow_err}")
                try:
                    await client.send_message(
                        ctx_target_chat_id,
                        text=overflow_text[:4000],
                        parse_mode=ParseMode.DISABLED,
                    )
                except Exception:
                    pass
        
        if status_msg:
            await client.delete_messages(ctx_target_chat_id, [status_msg.id])
        
        return True

    except asyncio.CancelledError:
        raise
    except FloodWait:
        # Re-raise FloodWait so the outer per-post loop can handle it
        # (sleep for the required duration instead of hammering Telegram)
        raise
    except Exception as e:
        logger.warning(f"download_and_send_media error (type={msg_type}): {type(e).__name__}: {e}")
        if status_msg:
            try:
                await client.delete_messages(ctx_target_chat_id, [status_msg.id])
            except Exception:
                pass
        return False
    finally:
        # GUARANTEED CLEANUP
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass
        if thumb_path and os.path.exists(thumb_path):
            try:
                os.remove(thumb_path)
            except Exception:
                pass
        # Note: Status file cleanup is no longer needed with the new progress system
        # The old file-based progress (downstatus.txt, upstatus.txt) is deprecated
        # Progress is now handled in-memory by progress_controller


def get_file_size(msg, msg_type: str) -> int:
    """Get file size from message"""
    try:
        if msg_type == "Document" and msg.document:
            return msg.document.file_size or 0
        elif msg_type == "Video" and msg.video:
            return msg.video.file_size or 0
        elif msg_type == "Audio" and msg.audio:
            return msg.audio.file_size or 0
        elif msg_type == "Voice" and msg.voice:
            return msg.voice.file_size or 0
        elif msg_type == "VideoNote" and msg.video_note:
            return msg.video_note.file_size or 0
        elif msg_type == "Animation" and msg.animation:
            return msg.animation.file_size or 0
    except Exception:
        pass
    return 0


async def get_thumbnail(acc, msg) -> Optional[str]:
    """Download thumbnail if available (video or document)"""
    try:
        if msg.video and msg.video.thumbs:
            return await acc.download_media(msg.video.thumbs[0].file_id)
        if msg.document and msg.document.thumbs:
            return await acc.download_media(msg.document.thumbs[0].file_id)
    except Exception:
        pass
    return None


def _msg_type_to_send_func(msg_type: str) -> str:
    """Map message type string to Pyrogram send method name."""
    _MAP = {
        "Photo": "send_photo",
        "Video": "send_video",
        "Audio": "send_audio",
        "Voice": "send_voice",
        "VideoNote": "send_video_note",
        "Animation": "send_animation",
        "Sticker": "send_sticker",
        "Document": "send_document",
    }
    return _MAP.get(msg_type, "send_document")


def get_message_type(msg) -> str:
    """Detect message type using attribute-based detection only.

    PYROFORK NOTE: MessageMediaType.WEB_PAGE does not exist in Pyrofork 2.3.69.
    Link-preview messages must be classified as "Text", not as media.
    Never uses msg.media attribute for classification.
    """
    from core.media_classifier import classify_message
    result = classify_message(msg)
    # Map media_classifier names to legacy names expected by callers
    _map = {
        "photo": "Photo",
        "video": "Video",
        "document": "Document",
        "audio": "Audio",
        "voice": "Voice",
        "video_note": "VideoNote",
        "animation": "Animation",
        "sticker": "Sticker",
        "poll": "Poll",
        "text": "Text",
    }
    return _map.get(result, "Unknown")


async def handle_poll(
    client: Client, 
    message: Message, 
    poll, 
    source_msg=None,
    user_session=None,
    auto_vote: bool = True
) -> bool:
    """
    Handle poll messages with auto-vote and text rendering.
    
    Features:
    - AUTO-VOTE using user session (if available)
    - For quizzes: votes correct answer
    - For regular polls: votes random option
    - Renders poll as formatted text (no poll forwarding)
    - Shows vote result
    - Extracts and sends QuizBot links if present
    - Preserves existing QuizBot workflow
    
    Args:
        client: Bot client
        message: User's message
        poll: Poll object
        source_msg: Original message containing poll
        user_session: User's Pyrogram client for voting
        auto_vote: Whether to auto-vote (default True)
    """
    try:
        if not poll:
            return False
        
        is_quiz = hasattr(poll, 'type') and poll.type == PollType.QUIZ
        correct_id = poll.correct_option_id if is_quiz and hasattr(poll, 'correct_option_id') else None
        
        # Determine which option to vote for
        import random
        if is_quiz and correct_id is not None:
            # Quiz: vote for correct answer
            vote_option = correct_id
        else:
            # Regular poll: vote random option
            vote_option = random.randint(0, len(poll.options) - 1)
        
        vote_success = False
        vote_error = None
        
        # AUTO-VOTE using user session
        if auto_vote and user_session and source_msg:
            try:
                await user_session.vote_poll(
                    chat_id=source_msg.chat.id,
                    message_id=source_msg.id,
                    options=[vote_option]
                )
                vote_success = True
                logger.info(f"Auto-voted option {vote_option} in poll {source_msg.id}")
            except Exception as vote_err:
                vote_error = str(vote_err)
                error_upper = vote_error.upper()
                if "ALREADY" in error_upper:
                    vote_success = True  # Already voted is OK
                    logger.debug(f"Already voted in poll {source_msg.id}")
                elif "FLOOD" in error_upper:
                    logger.warning(f"FloodWait on vote_poll: {vote_err}")
                else:
                    logger.warning(f"vote_poll failed: {vote_err}")
        
        # Build text representation (Uzbek format as requested)
        option_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        poll_type_text = "Quiz" if is_quiz else "So'rovnoma"
        text = f"**📊 {poll_type_text}**\n\n"
        text += f"**Savol:**\n{poll.question}\n\n"
        text += "**Variantlar:**\n"
        
        for i, opt in enumerate(poll.options):
            letter = option_letters[i] if i < len(option_letters) else str(i + 1)
            marker = ""
            
            if is_quiz and i == correct_id:
                marker = " ✅"
            
            text += f"{letter}) {opt.text}{marker}\n"
        
        # Show user's vote
        vote_letter = option_letters[vote_option] if vote_option < len(option_letters) else str(vote_option + 1)
        vote_text = poll.options[vote_option].text if vote_option < len(poll.options) else "?"
        
        text += f"\n**Sizning ovozingiz:** {vote_letter}) {vote_text}"
        
        if vote_success:
            text += " ✓"
        elif vote_error and "ALREADY" not in vote_error.upper():
            text += " (ovoz berilmadi)"
        
        # Quiz result
        if is_quiz:
            correct_letter = option_letters[correct_id] if correct_id is not None and correct_id < len(option_letters) else "?"
            correct_text = poll.options[correct_id].text if correct_id is not None and correct_id < len(poll.options) else "?"
            
            text += f"\n**To'g'ri javob:** {correct_letter}) {correct_text}"
            
            if vote_option == correct_id:
                text += "\n\n**Natija:** ✅ TO'G'RI"
            else:
                text += "\n\n**Natija:** ❌ NOTO'G'RI"
        
        # Explanation
        if is_quiz and hasattr(poll, 'explanation') and poll.explanation:
            text += f"\n\n**💡 Izoh:** {poll.explanation}"
        
        # QuizBot links (preserve existing workflow)
        if source_msg:
            quizbot_links = extract_quizbot_links(source_msg.reply_markup)
            if quizbot_links:
                text += "\n\n🤖 **QuizBot Start:**"
                for link in quizbot_links:
                    text += f"\n{link}"
        
        # Preserve reply markup for QuizBot workflow
        reply_markup = source_msg.reply_markup if source_msg else None
        
        # Send text-only message with auto-splitting (no poll forwarding)
        await safe_send_message(
            client=client,
            chat_id=message.chat.id,
            text=text,
            **build_reply_kwargs_from_message(message),
            reply_markup=reply_markup,
            **build_link_preview_kwargs(is_disabled=True)
        )
        
        return True
    except Exception as e:
        logger.warning(f"handle_poll error: {e}")
        return False


async def handle_quizbot_post(client: Client, message: Message, source_msg) -> bool:
    """
    Handle QuizBot posts - sends ALL content as ONE single message.
    
    Combines:
    - Quiz text/description
    - Emoji and metadata
    - QuizBot start links
    - Inline buttons (preserved)
    
    All in ONE message, not split across multiple messages.
    """
    try:
        # Build combined message content
        parts = []
        
        # 1. Original message text (quiz description)
        if source_msg.text:
            parts.append(source_msg.text)
        elif source_msg.caption:
            parts.append(source_msg.caption)
        
        # 2. Extract and append QuizBot links
        quizbot_links = extract_quizbot_links(source_msg.reply_markup)
        if quizbot_links:
            parts.append("")  # Empty line separator
            parts.append("🤖 **QuizBot Start:**")
            for link in quizbot_links:
                parts.append(link)
        
        if not parts:
            return False
        
        # Combine all parts into single message
        combined_text = "\n".join(parts)
        
        # Preserve inline buttons (excluding QuizBot start buttons which we already extracted)
        reply_markup = None
        if source_msg.reply_markup and hasattr(source_msg.reply_markup, 'inline_keyboard'):
            # Keep non-QuizBot buttons
            filtered_rows = []
            for row in source_msg.reply_markup.inline_keyboard:
                filtered_buttons = []
                for button in row:
                    if hasattr(button, 'url') and button.url:
                        # Skip QuizBot start buttons (already in text)
                        if 'quizbot' in button.url.lower() and 'start=' in button.url.lower():
                            continue
                    filtered_buttons.append(button)
                if filtered_buttons:
                    filtered_rows.append(filtered_buttons)
            
            if filtered_rows:
                reply_markup = InlineKeyboardMarkup(filtered_rows)
        
        # Send as ONE single message
        await client.send_message(
            message.chat.id,
            combined_text,
            **build_reply_kwargs_from_message(message),
            reply_markup=reply_markup,
            **build_link_preview_kwargs(is_disabled=True)
        )
        
        return True
        
    except Exception as e:
        logger.warning(f"handle_quizbot_post error: {e}")
        return False


# Don't Remove Credit Tg - @VJ_Botz
