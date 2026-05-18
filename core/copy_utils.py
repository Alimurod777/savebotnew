"""Helpers for copying MTProto-uploaded messages with the bot client."""

from __future__ import annotations

from typing import Optional


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
