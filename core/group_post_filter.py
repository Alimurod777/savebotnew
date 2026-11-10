"""Select group-authored messages and automatic posts from its linked channel."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class GroupPostFilter:
    group_id: int
    linked_channel_id: Optional[int] = None

    def accepts(self, message: Any) -> bool:
        if not message or getattr(message, "empty", False):
            return False
        # Never use names, signatures or the anonymous-admin pseudo-user as
        # proof of authorship. Only Telegram's peer IDs establish identity.
        chat_id = getattr(getattr(message, "chat", None), "id", None)
        if chat_id != self.group_id:
            return False
        sender_id = getattr(getattr(message, "sender_chat", None), "id", None)
        automatic = (
            getattr(message, "automatic_forward", False)
            or getattr(message, "is_automatic_forward", False)
        )
        if not automatic:
            return sender_id == self.group_id
        if self.linked_channel_id is None:
            return False
        origin = getattr(message, "forward_origin", None)
        origin_chat = getattr(origin, "chat", None)
        # Older cached messages may expose the legacy forward_from_chat field.
        origin_id = getattr(origin_chat, "id", None)
        if origin_id is None:
            origin_id = getattr(getattr(message, "forward_from_chat", None), "id", None)
        return (
            origin_id == self.linked_channel_id
            or (origin_id is None and sender_id == self.linked_channel_id)
        )
