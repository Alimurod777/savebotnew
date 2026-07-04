"""Helpers for copying MTProto-uploaded messages with the bot client."""

from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional, Tuple

import asyncio
import logging

logger = logging.getLogger(__name__)
_ACTIVE_UPLOAD_WAITERS: dict[int, List["BotUploadUpdateWaiter"]] = {}

_MEDIA_ATTRS = (
    "photo",
    "video",
    "audio",
    "document",
    "voice",
    "video_note",
    "animation",
    "sticker",
)


class BotMessageResolutionError(RuntimeError):
    """Raised when a bot-side DM message id cannot be resolved safely."""


class BotUploadUpdateWaiter:
    """
    Wait for the bot-side update created by a user-session upload.

    Bot API/pyrofork bots can receive incoming private messages as updates even
    when ``get_chat_history`` is not allowed for bots. Pool delivery uses this
    waiter to resolve the real bot-side message id without polling history.
    """

    def __init__(self, bot_client: Any, chat_id: int, timeout: float = 20.0) -> None:
        self.bot_client = bot_client
        self.chat_id = int(chat_id)
        self.timeout = float(timeout)
        self._handler = None
        self._registered = None
        self._group = -997
        self._target_fp = None
        self._future: Optional[asyncio.Future] = None
        self._candidates: List[Any] = []
        self._started = False

    async def start(self) -> "BotUploadUpdateWaiter":
        if self._started:
            return self

        from pyrogram import filters
        from pyrogram.handlers import MessageHandler

        try:
            from pyrogram import StopPropagation
        except Exception:  # pragma: no cover - depends on pyrogram flavor
            StopPropagation = None

        async def _handler(_, message):
            if not self._is_candidate(message):
                return

            for waiter in list(_ACTIVE_UPLOAD_WAITERS.get(self.chat_id, [])):
                waiter._remember_candidate(message)

            # Pool uploads are internal transport messages. Stop unrelated bot
            # handlers from processing them as user-submitted media.
            if StopPropagation is not None:
                raise StopPropagation

        self._handler = MessageHandler(
            _handler,
            filters.private & filters.chat(self.chat_id),
        )
        _ACTIVE_UPLOAD_WAITERS.setdefault(self.chat_id, []).append(self)
        try:
            registered = self.bot_client.add_handler(self._handler, group=self._group)
            if asyncio.iscoroutine(registered):
                registered = await registered
            self._registered = registered
            self._started = True
        except Exception:
            waiters = _ACTIVE_UPLOAD_WAITERS.get(self.chat_id, [])
            if self in waiters:
                waiters.remove(self)
            if not waiters:
                _ACTIVE_UPLOAD_WAITERS.pop(self.chat_id, None)
            raise
        return self

    async def wait_for(self, sent_msg: Any) -> int:
        if not self._started:
            await self.start()

        target_fp = _media_fingerprint(sent_msg)
        if not target_fp:
            raise BotMessageResolutionError("sent message has no stable media fingerprint")

        loop = asyncio.get_running_loop()
        self._target_fp = target_fp
        self._future = loop.create_future()

        for candidate in list(self._candidates):
            if _media_fingerprint(candidate) == target_fp:
                msg_id = getattr(candidate, "id", None)
                if msg_id is None:
                    raise BotMessageResolutionError("bot-side upload update has no message id")
                return int(msg_id)

        try:
            message = await asyncio.wait_for(self._future, timeout=self.timeout)
        except asyncio.TimeoutError as exc:
            raise BotMessageResolutionError(
                f"timed out waiting for bot-side upload update from chat {self.chat_id}"
            ) from exc

        msg_id = getattr(message, "id", None)
        if msg_id is None:
            raise BotMessageResolutionError("bot-side upload update has no message id")
        return int(msg_id)

    async def close(self) -> None:
        if not self._started:
            return
        try:
            if isinstance(self._registered, tuple):
                result = self.bot_client.remove_handler(*self._registered)
            elif self._handler is not None:
                result = self.bot_client.remove_handler(self._handler, self._group)
            else:
                result = None
            if asyncio.iscoroutine(result):
                await result
        except Exception as err:
            logger.debug("BotUploadUpdateWaiter cleanup failed: %s", err)
        finally:
            self._started = False
            waiters = _ACTIVE_UPLOAD_WAITERS.get(self.chat_id, [])
            if self in waiters:
                waiters.remove(self)
            if not waiters:
                _ACTIVE_UPLOAD_WAITERS.pop(self.chat_id, None)
            if self._future is not None and not self._future.done():
                self._future.cancel()

    def _is_candidate(self, message: Any) -> bool:
        chat = getattr(message, "chat", None)
        chat_id = getattr(chat, "id", None)
        if chat_id is not None and int(chat_id) != self.chat_id:
            return False

        from_user = getattr(message, "from_user", None)
        from_user_id = getattr(from_user, "id", None)
        if from_user_id is not None and int(from_user_id) != self.chat_id:
            return False

        return _media_fingerprint(message) is not None

    def _remember(self, message: Any) -> None:
        msg_id = getattr(message, "id", None)
        if msg_id is not None:
            for candidate in self._candidates:
                if getattr(candidate, "id", None) == msg_id:
                    return
        self._candidates.append(message)
        if len(self._candidates) > 20:
            self._candidates = self._candidates[-20:]

    def _remember_candidate(self, message: Any) -> None:
        self._remember(message)
        matched = self._target_fp is not None and _media_fingerprint(message) == self._target_fp
        if matched and self._future is not None and not self._future.done():
            self._future.set_result(message)


async def start_bot_upload_update_waiter(
    bot_client: Any,
    chat_id: int,
    timeout: float = 20.0,
) -> BotUploadUpdateWaiter:
    waiter = BotUploadUpdateWaiter(bot_client, chat_id, timeout=timeout)
    return await waiter.start()


def _media_fingerprint(message: Any) -> Optional[Tuple[str, str, Optional[int], Optional[str], str]]:
    """
    Return a stable media fingerprint for matching sender-side and bot-side DMs.

    file_unique_id is Telegram-global for the same media. Extra fields reduce
    false matches when a user sends the same file with different metadata.
    """
    if not message:
        return None

    caption = ""
    if getattr(message, "caption", None) is not None:
        caption = str(message.caption)

    for media_type in _MEDIA_ATTRS:
        media = getattr(message, media_type, None)
        if not media:
            continue
        file_unique_id = getattr(media, "file_unique_id", None)
        if not file_unique_id:
            continue
        file_size = getattr(media, "file_size", None)
        file_name = getattr(media, "file_name", None)
        return (
            media_type,
            str(file_unique_id),
            int(file_size) if file_size is not None else None,
            str(file_name) if file_name else None,
            caption,
        )

    venue = getattr(message, "venue", None)
    if venue:
        location = getattr(venue, "location", None)
        latitude = getattr(location, "latitude", None)
        longitude = getattr(location, "longitude", None)
        if latitude is not None and longitude is not None:
            return (
                "venue",
                f"{float(latitude):.7f},{float(longitude):.7f}",
                None,
                f"{getattr(venue, 'title', '')}|{getattr(venue, 'address', '')}",
                caption,
            )

    location = getattr(message, "location", None)
    if location:
        latitude = getattr(location, "latitude", None)
        longitude = getattr(location, "longitude", None)
        if latitude is not None and longitude is not None:
            return (
                "location",
                f"{float(latitude):.7f},{float(longitude):.7f}",
                None,
                None,
                caption,
            )
    return None


def _date_distance_seconds(left: Any, right: Any) -> float:
    if not isinstance(left, datetime) or not isinstance(right, datetime):
        return 0.0
    try:
        return abs((left - right).total_seconds())
    except Exception:
        return 0.0



def get_bot_copy_source_chat_id(sent_message, fallback_chat_id: Optional[int] = None) -> Optional[int]:
    """
    Return the chat id the bot client should use as copy/delete source.

    A user session that uploads to the bot sees the peer as the bot chat, so
    ``sent_message.chat.id`` is often the bot id. The bot, however, sees the
    same private dialog under the sender user's id. Prefer ``from_user.id`` for
    MTProto user-session uploads and fall back to the message chat id.
    """
    from_user = getattr(sent_message, "from_user", None)
    from_user_id = getattr(from_user, "id", None)
    if from_user_id is not None:
        return int(from_user_id)

    sender_chat = getattr(sent_message, "sender_chat", None)
    sender_chat_id = getattr(sender_chat, "id", None)
    if sender_chat_id is not None:
        return int(sender_chat_id)

    if fallback_chat_id is not None:
        return int(fallback_chat_id)

    chat = getattr(sent_message, "chat", None)
    chat_id = getattr(chat, "id", None)
    if chat_id is not None:
        return int(chat_id)

    return None


async def get_bot_latest_message_id(bot_client, chat_id: int) -> Optional[int]:
    """
    Return the newest bot-side message id in a private dialog.

    0 means the dialog exists but has no messages. None means the watermark
    could not be read, so callers should avoid unsafe copy/delete operations.
    """
    try:
        async for msg in bot_client.get_chat_history(chat_id, limit=1):
            msg_id = getattr(msg, "id", None)
            return int(msg_id) if msg_id is not None else 0
        return 0
    except Exception as err:
        logger.warning("Could not read bot DM watermark for chat %s: %s", chat_id, err)
        return None


async def get_bot_real_message_id(
    bot_client,
    chat_id: int,
    sent_msg,
    *,
    min_message_id: Optional[int] = None,
    max_retries: int = 5,
    history_limit: int = 80,
) -> int:
    """
    Find the real message ID from the bot's perspective using a strict media
    fingerprint and an optional pre-upload watermark.

    In Telegram private chats, sender and receiver message IDs are independent.
    When a user session sends a message to the bot, sent_msg.id is the sender's ID.
    We must find the bot's corresponding message ID to copy/delete it. If it
    cannot be resolved safely, this function raises instead of falling back to
    sent_msg.id, because that fallback can copy/delete unrelated old messages.
    """
    if getattr(sent_msg, "empty", False):
        raise BotMessageResolutionError("empty sent message cannot be resolved")

    target_fp = _media_fingerprint(sent_msg)
    if not target_fp:
        raise BotMessageResolutionError("sent message has no stable media fingerprint")

    min_id = int(min_message_id or 0)
    sent_date = getattr(sent_msg, "date", None)
    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            candidates = []
            async for bot_msg in bot_client.get_chat_history(chat_id, limit=history_limit):
                bot_msg_id = getattr(bot_msg, "id", None)
                if bot_msg_id is None:
                    continue
                bot_msg_id = int(bot_msg_id)
                if min_id and bot_msg_id <= min_id:
                    break

                from_user = getattr(bot_msg, "from_user", None)
                from_user_id = getattr(from_user, "id", None)
                if from_user_id is not None and int(from_user_id) != int(chat_id):
                    continue

                if _media_fingerprint(bot_msg) == target_fp:
                    candidates.append(bot_msg)

            if candidates:
                candidates.sort(
                    key=lambda msg: (
                        _date_distance_seconds(getattr(msg, "date", None), sent_date),
                        -int(getattr(msg, "id", 0)),
                    )
                )
                return int(candidates[0].id)
        except Exception as err:
            last_error = err
            logger.warning("get_bot_real_message_id history fetch error: %s", err)

        if attempt < max_retries - 1:
            await asyncio.sleep(0.7 * (attempt + 1))

    detail = f" after bot message {min_id}" if min_id else ""
    if last_error:
        detail += f"; last error: {last_error}"
    raise BotMessageResolutionError(
        f"could not resolve bot-side message id for chat {chat_id}{detail}"
    )
