"""
core/bot_unblock.py — User sessiyasi orqali botni unblock qilish.

Foydalanuvchining o'z Telegram sessiyasi orqali botni unblock qilib,
/start yuboradi va chatni arxivlaydi.

Bu modul hech qachon exception raise qilmaydi — barcha xatolar
ichida catch qilinadi va False qaytaradi.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


async def try_unblock_bot(
    session_string: str,
    user_id: int,
    bot_username: str,
) -> bool:
    """
    User sessiyasi orqali botni unblock qiladi va /start yuboradi.

    Args:
        session_string: User ning Pyrogram session stringi
        user_id: User ning Telegram ID si
        bot_username: Bot username (@ siz ham bo'ladi)

    Returns:
        True  — muvaffaqiyatli unblock + /start yuborildi
        False — muvaffaqiyatsiz (session invalid, bot hali ham block, yoki boshqa xato)

    Hech qachon exception raise qilmaydi — barcha xatolar catch qilinadi.
    """
    if not session_string or not bot_username:
        return False

    uname = bot_username.lstrip("@")

    try:
        from TechVJ.session_handler import (
            create_user_session,
            SessionInvalidError,
            SessionConnectionError,
        )

        async with create_user_session(
            session_string, user_id,
            timeout=15.0,
            validate=False,  # tezroq — get_me() chaqirmaymiz
        ) as acc:
            # Step 1: botni unblock qilish
            try:
                await acc.unblock_user(uname)
                logger.debug(
                    "bot_unblock[%d]: unblocked @%s", user_id, uname
                )
            except Exception:
                pass  # bloklangan bo'lmasligi mumkin — skip

            # Step 2: /start yuborish (chat tiklash)
            try:
                await acc.send_message(uname, "/start")
                logger.info(
                    "bot_unblock[%d]: sent /start to @%s — success",
                    user_id, uname,
                )
            except Exception as e:
                logger.warning(
                    "bot_unblock[%d]: /start to @%s failed: %s",
                    user_id, uname, e,
                )
                return False

            # Step 3: chatni arxivlash (best-effort)
            try:
                chat = await acc.get_chat(uname)
                await acc.archive_chats([chat.id])
            except Exception:
                pass  # non-fatal

            return True

    except Exception as e:
        # SessionInvalidError, SessionConnectionError, yoki boshqa har qanday xato
        logger.warning(
            "bot_unblock[%d]: failed for @%s: %s: %s",
            user_id, uname, type(e).__name__, e,
        )
        return False
