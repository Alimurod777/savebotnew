"""
core/premium_logic.py - Telegram Premium detection and splitting decision engine.

This module decides whether to split files based on:
1. Whether the CURRENT USER is Telegram Premium
2. Whether a SYSTEM Telegram Premium session is configured
3. A USER-LEVEL preference (Premium users can choose behavior)

IMPORTANT: "Premium" = Telegram Premium subscription, NOT bot privileges.
Detection uses user.is_premium from MTProto user object.
"""

import os
import json
import asyncio
import logging
from typing import Optional, Tuple
from dataclasses import dataclass, asdict

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

from config import API_ID, API_HASH, get_client_params

logger = logging.getLogger(__name__)

# Per-user upload setting options
UPLOAD_AUTO = "auto"
UPLOAD_FORCE_SPLIT = "force_split"
UPLOAD_NO_SPLIT = "no_split"
VALID_SETTINGS = {UPLOAD_AUTO, UPLOAD_FORCE_SPLIT, UPLOAD_NO_SPLIT}

# Telegram Premium allows 4GB uploads (vs 2GB for non-premium)
PREMIUM_FILE_SIZE_LIMIT = 4000 * 1024 * 1024  # 4GB
STANDARD_FILE_SIZE_LIMIT = 2000 * 1024 * 1024  # 2GB

# Premium vs standard caption/message limits (UTF-16 code units)
PREMIUM_CAPTION_LIMIT = 2048  # Premium users can send 2048 UTF-16 captions
STANDARD_CAPTION_LIMIT = 1024  # Standard 1024 UTF-16 caption limit
PREMIUM_MESSAGE_LIMIT = 4096  # Same for both

# Storage paths — environment-aware (Colab, Kaggle, local)
try:
    from core.environment import get_base_path
    _BASE_DIR = get_base_path()
except ImportError:
    _BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_PREMIUM_DATA_DIR = os.path.join(_BASE_DIR, "data", "premium")
_SYSTEM_SESSION_FILE = os.path.join(_PREMIUM_DATA_DIR, "system_session.json")
_USER_PREFS_DIR = os.path.join(_PREMIUM_DATA_DIR, "user_prefs")


def _ensure_dirs():
    os.makedirs(_PREMIUM_DATA_DIR, exist_ok=True)
    os.makedirs(_USER_PREFS_DIR, exist_ok=True)


def get_caption_limit(is_premium: bool = False) -> int:
    """Get the caption limit in UTF-16 code units based on premium status."""
    return PREMIUM_CAPTION_LIMIT if is_premium else STANDARD_CAPTION_LIMIT


def get_file_size_limit(is_premium: bool = False) -> int:
    """Get the maximum file upload size based on premium status."""
    return PREMIUM_FILE_SIZE_LIMIT if is_premium else STANDARD_FILE_SIZE_LIMIT


# ==================== PREMIUM DETECTION ====================

async def check_user_premium(client: Client, user_id: int) -> bool:
    """
    Check if a user has Telegram Premium by querying Telegram live.
    Uses MTProto user.is_premium — NOT database flags.
    """
    try:
        users = await client.get_users(user_id)
        if users:
            user = users if not isinstance(users, list) else users[0]
            return getattr(user, 'is_premium', False) or False
    except FloodWait as e:
        wait = getattr(e, 'value', getattr(e, 'x', 5))
        logger.warning(f"FloodWait {wait}s checking premium for {user_id}")
        await asyncio.sleep(min(wait, 10))
        return False
    except Exception as e:
        logger.warning(f"Premium check failed for {user_id}: {e}")
    return False


async def check_user_premium_via_session(session_string: str) -> Tuple[bool, Optional[dict]]:
    """
    Check Premium status using a session string (get_me).
    Returns (is_premium, user_info_dict_or_None).
    """
    import uuid
    client = Client(
        f"premium_check_{uuid.uuid4().hex[:8]}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
        **get_client_params()
    )
    try:
        await asyncio.wait_for(client.start(), timeout=15.0)
        me = await client.get_me()
        is_premium = getattr(me, 'is_premium', False) or False
        info = {
            "user_id": me.id,
            "first_name": me.first_name or "",
            "username": me.username or "",
            "is_premium": is_premium,
        }
        return is_premium, info
    except Exception as e:
        logger.warning(f"Premium session check failed: {e}")
        return False, None
    finally:
        try:
            if client.is_connected:
                await asyncio.wait_for(client.stop(), timeout=5.0)
        except Exception:
            pass


async def lookup_user_premium(client: Client, identifier) -> Tuple[bool, Optional[dict]]:
    """
    Look up any user's Premium status via MTProto.
    identifier can be user_id (int) or username (str).
    Returns (is_premium, user_info_dict_or_None).
    """
    try:
        users = await client.get_users(identifier)
        user = users if not isinstance(users, list) else users[0]
        is_premium = getattr(user, 'is_premium', False) or False
        info = {
            "user_id": user.id,
            "first_name": user.first_name or "",
            "username": user.username or "",
            "is_premium": is_premium,
        }
        return is_premium, info
    except Exception as e:
        logger.warning(f"User lookup failed for {identifier}: {e}")
        return False, None


# ==================== SYSTEM PREMIUM SESSION ====================

@dataclass
class SystemPremiumSession:
    """System-level Premium session configured by owner."""
    session_string: str
    user_id: int
    username: str = ""
    first_name: str = ""


# In-memory cache of system session (loaded from disk on first access)
_system_session: Optional[SystemPremiumSession] = None
_system_session_loaded: bool = False


def _load_system_session() -> Optional[SystemPremiumSession]:
    """Load system Premium session from disk."""
    global _system_session, _system_session_loaded
    _system_session_loaded = True
    try:
        if os.path.exists(_SYSTEM_SESSION_FILE):
            with open(_SYSTEM_SESSION_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            _system_session = SystemPremiumSession(
                session_string=data['session_string'],
                user_id=data['user_id'],
                username=data.get('username', ''),
                first_name=data.get('first_name', ''),
            )
            return _system_session
    except Exception as e:
        logger.warning(f"Failed to load system premium session: {e}")
    _system_session = None
    return None


def get_system_session() -> Optional[SystemPremiumSession]:
    """Get the system Premium session (cached)."""
    global _system_session_loaded
    if not _system_session_loaded:
        _load_system_session()
    return _system_session


def has_system_premium() -> bool:
    """Check if a system Premium session is configured."""
    return get_system_session() is not None


async def set_system_session(session_string: str) -> Tuple[bool, str]:
    """
    Set system Premium session after verifying it's valid and Premium.
    Returns (success, message).
    """
    global _system_session, _system_session_loaded
    _ensure_dirs()

    is_premium, info = await check_user_premium_via_session(session_string)
    if info is None:
        return False, "Session is invalid or could not connect."
    if not is_premium:
        return False, (
            f"Account @{info['username'] or info['user_id']} is NOT Telegram Premium.\n"
            "Only Telegram Premium sessions can be used as system uploader."
        )

    _system_session = SystemPremiumSession(
        session_string=session_string,
        user_id=info['user_id'],
        username=info.get('username', ''),
        first_name=info.get('first_name', ''),
    )
    _system_session_loaded = True

    data = {
        'session_string': session_string,
        'user_id': info['user_id'],
        'username': info.get('username', ''),
        'first_name': info.get('first_name', ''),
    }
    try:
        with open(_SYSTEM_SESSION_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Failed to save system session: {e}")
        return False, f"Session verified but save failed: {e}"

    logger.info(f"System Premium session set: @{info.get('username', info['user_id'])}")
    return True, (
        f"System Premium session set.\n"
        f"Account: {info.get('first_name', '')} @{info.get('username', '')}\n"
        f"User ID: `{info['user_id']}`"
    )


async def remove_system_session() -> Tuple[bool, str]:
    """Remove system Premium session."""
    global _system_session, _system_session_loaded
    _system_session = None
    _system_session_loaded = True

    try:
        if os.path.exists(_SYSTEM_SESSION_FILE):
            os.remove(_SYSTEM_SESSION_FILE)
    except Exception as e:
        logger.warning(f"Failed to remove system session file: {e}")
        return False, f"Error removing session file: {e}"

    logger.info("System Premium session removed")
    return True, "System Premium session removed."


def get_system_session_status() -> str:
    """Get human-readable system session status."""
    session = get_system_session()
    if session is None:
        return "**System Premium Session:** Not configured"
    return (
        f"**System Premium Session:** Active\n"
        f"**Account:** {session.first_name} @{session.username}\n"
        f"**User ID:** `{session.user_id}`"
    )


# ==================== USER UPLOAD PREFERENCE ====================

def get_user_upload_setting(user_id: int) -> str:
    """Get user's upload split preference. Default: auto."""
    _ensure_dirs()
    pref_file = os.path.join(_USER_PREFS_DIR, f"{user_id}.txt")
    try:
        if os.path.exists(pref_file):
            with open(pref_file, 'r') as f:
                val = f.read().strip()
                if val in VALID_SETTINGS:
                    return val
    except Exception:
        pass
    return UPLOAD_AUTO


def set_user_upload_setting(user_id: int, setting: str) -> bool:
    """Set user's upload split preference."""
    if setting not in VALID_SETTINGS:
        return False
    _ensure_dirs()
    pref_file = os.path.join(_USER_PREFS_DIR, f"{user_id}.txt")
    try:
        with open(pref_file, 'w') as f:
            f.write(setting)
        return True
    except Exception as e:
        logger.error(f"Failed to set upload setting for {user_id}: {e}")
        return False


# ==================== SPLITTING DECISION ENGINE ====================

def should_split(
    user_is_premium: bool,
    has_system_premium_session: bool,
    user_setting: str,
    file_size: int,
) -> bool:
    """
    Central decision: should a file be split before upload?

    Logic:
    - If user is Telegram Premium AND setting is force_split -> split
    - If user is Telegram Premium AND setting is not force_split -> no split
    - If system premium session exists -> no split
    - Otherwise -> split (standard bot upload limit)

    Args:
        user_is_premium: User's Telegram Premium status (live check)
        has_system_premium_session: Whether system Premium session is available
        user_setting: User's preference (auto / force_split / no_split)
        file_size: File size in bytes

    Returns:
        True if file should be split, False if Premium path can handle it
    """
    # Files within standard limit never need splitting
    if file_size <= STANDARD_FILE_SIZE_LIMIT:
        return False

    # Premium users can upload up to 4GB without splitting
    if user_is_premium:
        if user_setting == UPLOAD_FORCE_SPLIT:
            logger.info("User forced split mode")
            return True
        # auto or no_split: Premium handles it
        if file_size <= PREMIUM_FILE_SIZE_LIMIT:
            logger.info("Telegram Premium detected for user - bypassing split")
            return False
        # Even Premium can't handle >4GB
        return True

    # System premium session can upload up to 4GB
    if has_system_premium_session:
        if file_size <= PREMIUM_FILE_SIZE_LIMIT:
            logger.info("System Premium session in use")
            return False
        return True

    # No premium available — standard split
    logger.info("Premium unavailable - fallback engaged")
    return True


def get_upload_limit(user_is_premium: bool, has_system_premium_session: bool) -> int:
    """Get the effective upload size limit in bytes."""
    if user_is_premium or has_system_premium_session:
        return PREMIUM_FILE_SIZE_LIMIT
    return STANDARD_FILE_SIZE_LIMIT
