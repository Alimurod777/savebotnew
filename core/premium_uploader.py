"""
core/premium_uploader.py - Premium-aware upload backend.

Two runtime strategies:
  BotUpload  — existing behavior (bot token, 2GB limit)
  PremiumUpload — uses Premium MTProto session (4GB limit)

Selection logic:
  IF user.is_premium -> use their own session when possible
  ELIF system premium exists -> use system premium uploader
  ELSE -> fallback to bot upload + splitter

If Premium upload fails (session revoked, expired, FloodWait, RPC, DC migration),
system instantly falls back to BotUpload WITHOUT crashing.
"""

import os
import asyncio
import uuid
import logging
from typing import Optional, Callable

from pyrogram import Client
from pyrogram.errors import (
    FloodWait, AuthKeyUnregistered, AuthKeyInvalid,
    SessionRevoked, SessionExpired, UserDeactivated,
    RPCError, Timeout,
)

from config import API_ID, API_HASH, get_client_params
from core.premium_logic import get_system_session, SystemPremiumSession

logger = logging.getLogger(__name__)

# Fatal errors that mean the premium session is permanently unusable
_FATAL_ERRORS = (
    AuthKeyUnregistered, AuthKeyInvalid,
    SessionRevoked, SessionExpired, UserDeactivated,
)

# Max upload timeout (30 minutes for very large files)
_UPLOAD_TIMEOUT = 1800


async def premium_upload(
    bot_client: Client,
    chat_id: int,
    file_path: str,
    send_func_name: str,
    send_kwargs: dict,
    user_session_string: Optional[str] = None,
    progress: Optional[Callable] = None,
) -> bool:
    """
    Attempt upload via Premium session, fallback to bot on failure.

    Args:
        bot_client: The bot's Pyrogram Client (fallback)
        chat_id: Destination chat ID
        file_path: Path to file on disk
        send_func_name: Method name e.g. "send_video", "send_document"
        send_kwargs: kwargs for the send method (caption, entities, etc.)
            Must NOT include chat_id or the media argument — those are added here.
        user_session_string: User's own session (if they are Premium)
        progress: Progress callback

    Returns:
        True if upload succeeded (via either path), False on total failure.
    """
    # Determine which premium session to try
    session_string = user_session_string
    session_source = "user"

    if not session_string:
        sys_session = get_system_session()
        if sys_session:
            session_string = sys_session.session_string
            session_source = "system"

    # Try premium upload if session available
    if session_string:
        try:
            success = await _upload_via_session(
                session_string, chat_id, file_path,
                send_func_name, send_kwargs, progress, session_source
            )
            if success:
                return True
            # Non-fatal failure (e.g. network) — fall through to bot upload
        except _FATAL_ERRORS as e:
            logger.warning(
                f"Premium session ({session_source}) fatally failed: {type(e).__name__}. "
                f"Falling back to bot upload."
            )
        except Exception as e:
            logger.warning(
                f"Premium upload ({session_source}) failed: {e}. "
                f"Falling back to bot upload."
            )

    # Fallback: bot upload
    return await _upload_via_bot(
        bot_client, chat_id, file_path,
        send_func_name, send_kwargs, progress
    )


async def _upload_via_session(
    session_string: str,
    chat_id: int,
    file_path: str,
    send_func_name: str,
    send_kwargs: dict,
    progress: Optional[Callable],
    source: str,
) -> bool:
    """Upload using a Premium user session."""
    client_name = f"premium_upload_{uuid.uuid4().hex[:8]}"
    fp = get_client_params()
    client = Client(
        client_name,
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
        sleep_threshold=60,
        # CRITICAL: Keep low to prevent FILE_PART_INVALID on big file uploads
        max_concurrent_transmissions=2,
        device_model=fp['device_model'],
        system_version=fp['system_version'],
        app_version=fp['app_version'],
        lang_code=fp['lang_code'],
    )


    try:
        await asyncio.wait_for(client.start(), timeout=20.0)

        send_method = getattr(client, send_func_name, None)
        if send_method is None:
            logger.error(f"Unknown send method: {send_func_name}")
            return False

        kwargs = dict(send_kwargs)
        kwargs['chat_id'] = chat_id
        # The first positional arg name varies by method, but all accept the media
        # as the second positional arg via the media-type key
        media_key = send_func_name.replace("send_", "")
        # Pyrogram uses the media type name as kwarg: send_video(video=...), send_document(document=...)
        kwargs[media_key] = file_path
        if progress:
            kwargs['progress'] = progress

        await asyncio.wait_for(send_method(**kwargs), timeout=_UPLOAD_TIMEOUT)
        logger.info(f"Premium upload ({source}) succeeded for {os.path.basename(file_path)}")
        return True

    except FloodWait as e:
        wait = getattr(e, 'value', getattr(e, 'x', 30))
        logger.warning(f"Premium upload FloodWait: {wait}s")
        if wait <= 60:
            await asyncio.sleep(wait)
            try:
                send_method = getattr(client, send_func_name)
                kwargs = dict(send_kwargs)
                kwargs['chat_id'] = chat_id
                media_key = send_func_name.replace("send_", "")
                kwargs[media_key] = file_path
                if progress:
                    kwargs['progress'] = progress
                await asyncio.wait_for(send_method(**kwargs), timeout=_UPLOAD_TIMEOUT)
                return True
            except Exception:
                pass
        return False

    except asyncio.TimeoutError:
        logger.warning(f"Premium upload ({source}) timed out")
        return False

    except _FATAL_ERRORS:
        raise  # Let caller handle and mark session as dead

    except Exception as e:
        logger.warning(f"Premium upload ({source}) error: {e}")
        return False

    finally:
        try:
            if client.is_connected:
                await asyncio.wait_for(client.stop(), timeout=5.0)
        except Exception:
            pass


async def _upload_via_bot(
    bot_client: Client,
    chat_id: int,
    file_path: str,
    send_func_name: str,
    send_kwargs: dict,
    progress: Optional[Callable],
) -> bool:
    """Upload using the bot client (standard path)."""
    try:
        send_method = getattr(bot_client, send_func_name, None)
        if send_method is None:
            logger.error(f"Unknown send method on bot: {send_func_name}")
            return False

        kwargs = dict(send_kwargs)
        kwargs['chat_id'] = chat_id
        media_key = send_func_name.replace("send_", "")
        kwargs[media_key] = file_path
        if progress:
            kwargs['progress'] = progress

        await send_method(**kwargs)
        return True

    except FloodWait as e:
        wait = getattr(e, 'value', getattr(e, 'x', 30))
        logger.warning(f"Bot upload FloodWait: {wait}s")
        await asyncio.sleep(min(wait, 60))
        try:
            await send_method(**kwargs)
            return True
        except Exception:
            return False

    except Exception as e:
        logger.error(f"Bot upload failed: {e}")
        return False
