from __future__ import annotations

import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

RESTRICTED_CHANNEL_MESSAGE = (
    "Bu kanaldan kontent olish owner tomonidan taqiqlangan.\n"
    "Iltimos boshqa havola yuboring yoki owner bilan bog'laning."
)


def _same_user(left: int, right: int) -> bool:
    try:
        return int(left) == int(right)
    except Exception:
        return False


def validate_restricted_channel_id(
    channel_id: Optional[int],
    *,
    user_id: int,
    owner_id: int,
) -> Tuple[bool, str]:
    """
    Reject enabled ChannelMonitor sources for non-owner users.

    Returns (allowed, message). Missing/unknown channel IDs are allowed.
    """
    if channel_id is None:
        return True, ""

    if _same_user(user_id, owner_id):
        return True, ""

    try:
        source_id = int(channel_id)
    except Exception:
        return True, ""

    try:
        from core.channel_monitor import channel_monitor
    except Exception as err:
        logger.debug("Restricted channel guard unavailable: %s", err)
        return True, ""

    try:
        if channel_monitor.is_monitored(source_id):
            return False, RESTRICTED_CHANNEL_MESSAGE
    except Exception as err:
        logger.debug("Restricted channel guard skipped for %s: %s", source_id, err)

    return True, ""
