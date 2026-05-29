"""Helpers for copying MTProto-uploaded messages with the bot client."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Tuple

import asyncio
import logging

logger = logging.getLogger(__name__)

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

    chat = getattr(sent_message, "chat", None)
    chat_id = getattr(chat, "id", None)
    if chat_id is not None:
        return int(chat_id)

    return int(fallback_chat_id) if fallback_chat_id is not None else None


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
