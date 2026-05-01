"""
Pyrogram Client Factory

Creates properly configured Pyrogram clients for:
- Main bot (with handlers)
- Download operations (optimized for file transfers)
- Session validation

Uses Kurigram 2.2.17 which installs under the "pyrogram" namespace.

SECURITY: All clients use platform-consistent fingerprints from config.py
to prevent Telegram security resets.
"""

import os
import logging
import asyncio
from typing import Optional, Tuple, Dict

# Pyrogram imports (provided by kurigram==2.2.17)
from pyrogram import Client
from pyrogram.errors import (
    AuthKeyUnregistered, SessionRevoked, UserDeactivated,
    AuthKeyDuplicated, FloodWait, BadRequest
)

from config import get_client_params, DEFAULT_CLIENT_FINGERPRINT

logger = logging.getLogger(__name__)

# Get validated fingerprint parameters
_fingerprint = DEFAULT_CLIENT_FINGERPRINT.to_dict()

# PATCH 1: Absolute sessions path — prevents sqlite3 errors in Colab/Kaggle
# Uses environment-aware path resolution for Colab/Kaggle/local
try:
    from core.environment import get_sessions_path
    _SESSION_DIR = get_sessions_path()
except ImportError:
    _SESSION_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sessions")
    os.makedirs(_SESSION_DIR, exist_ok=True)

# Default client configuration optimized for high-load downloads
DEFAULT_CONFIG = {
    "workdir": _SESSION_DIR,
    "device_model": _fingerprint['device_model'],
    "system_version": _fingerprint['system_version'],
    "app_version": _fingerprint['app_version'],
    "lang_code": _fingerprint['lang_code'],
    "workers": 32,
    "sleep_threshold": 60,
    "max_concurrent_transmissions": 50,
}


def create_bot_client(
    api_id: int,
    api_hash: str,
    bot_token: str,
    name: str = "media_downloader_bot",
    **kwargs
) -> Client:
    """
    Create a Pyrogram bot client for handling commands.
    
    Uses bot token authentication.
    
    Args:
        api_id: Telegram API ID from my.telegram.org
        api_hash: Telegram API Hash from my.telegram.org
        bot_token: Bot token from @BotFather
        name: Session name
        **kwargs: Additional client options
    
    Returns:
        Configured Pyrogram Client (not started)
    """
    workdir = kwargs.pop('workdir', DEFAULT_CONFIG['workdir'])
    os.makedirs(workdir, exist_ok=True)
    
    config = {
        **DEFAULT_CONFIG,
        'workdir': workdir,
        **kwargs
    }
    
    return Client(
        name=name,
        api_id=api_id,
        api_hash=api_hash,
        bot_token=bot_token,
        **config
    )


def create_user_client(
    api_id: int,
    api_hash: str,
    session_string: str,
    name: str = "user_session",
    **kwargs
) -> Client:
    """
    Create a Pyrogram client with user session authentication.
    
    Uses session string for in-memory session (no file I/O).
    SECURITY: Uses validated platform-consistent fingerprint.
    
    Args:
        api_id: Telegram API ID
        api_hash: Telegram API Hash
        session_string: Pyrogram session string
        name: Session name identifier
        **kwargs: Additional client options
    
    Returns:
        Configured Pyrogram Client (not started)
    """
    # Start with validated fingerprint params
    config = get_client_params()
    config.update(kwargs)
    
    return Client(
        name=name,
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        in_memory=True,
        **config
    )


def create_download_client(
    api_id: int,
    api_hash: str,
    session_string: str,
    name: str = "download_client"
) -> Client:
    """
    Create a client optimized for downloads.
    
    Uses:
    - in_memory session (no file I/O during operation)
    - no_updates (skip update processing overhead)
    - Minimal workers (we use our own queue)
    - High sleep_threshold (handle rate limits gracefully)
    
    SECURITY: Uses validated platform-consistent fingerprint.
    
    Args:
        api_id: Telegram API ID
        api_hash: Telegram API Hash
        session_string: Valid Pyrogram session string
        name: Client identifier
    
    Returns:
        Pyrogram Client optimized for file downloads
    """
    # Get validated fingerprint
    fp = get_client_params()
    
    return Client(
        name=name,
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
        workers=4,
        sleep_threshold=120,
        max_concurrent_transmissions=10,
        device_model=fp['device_model'],
        system_version=fp['system_version'],
        app_version=fp['app_version'],
        lang_code=fp['lang_code'],
    )


async def validate_session(
    api_id: int,
    api_hash: str,
    session_string: str
) -> Tuple[bool, str, Optional[dict]]:
    """
    Validate a session string by attempting to connect.
    
    Args:
        api_id: Telegram API ID
        api_hash: Telegram API Hash
        session_string: Session string to validate
    
    Returns:
        Tuple of (is_valid, message, user_info)
        user_info contains id, first_name, username if valid
    """
    client = None
    try:
        config = get_client_params()
        client = Client(
            name="session_validator",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_string,
            in_memory=True,
            no_updates=True,
            workers=1,
            **config,
        )
        
        await client.start()
        me = await client.get_me()
        
        user_info = {
            "id": me.id,
            "first_name": me.first_name,
            "username": me.username,
        }
        
        return True, f"Valid session for {me.first_name} (@{me.username})", user_info
        
    except AuthKeyUnregistered:
        return False, "Session expired or revoked. Please login again.", None
    except SessionRevoked:
        return False, "Session has been revoked. Please login again.", None
    except UserDeactivated:
        return False, "User account has been deactivated.", None
    except AuthKeyDuplicated:
        return False, "Session is being used elsewhere. Wait and try again.", None
    except FloodWait as e:
        wait = getattr(e, 'value', getattr(e, 'x', 30))
        return False, f"Too many attempts. Wait {wait} seconds.", None
    except Exception as e:
        return False, f"Validation error: {str(e)}", None
    finally:
        if client:
            try:
                if client.is_connected:
                    await client.stop()
            except Exception:
                pass


async def export_session_string(client: Client) -> Optional[str]:
    """
    Export session string from an active client.
    
    Call after successful login to persist the session.
    
    Args:
        client: Connected Pyrogram client
    
    Returns:
        Session string or None on failure
    """
    try:
        if not client.is_connected:
            logger.warning("Client not connected, cannot export session")
            return None
        
        return await client.export_session_string()
    except Exception as e:
        logger.error(f"Failed to export session: {e}")
        return None


class UserSessionPool:
    """
    Pool of user session clients for concurrent downloads.
    
    Manages lifecycle of multiple user sessions:
    - Connection pooling
    - Automatic reconnection
    - Clean shutdown
    """
    
    def __init__(self, api_id: int, api_hash: str):
        self.api_id = api_id
        self.api_hash = api_hash
        self._clients: Dict[int, Client] = {}
        self._locks: Dict[int, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()
    
    async def _get_user_lock(self, user_id: int) -> asyncio.Lock:
        """Get or create a lock for a specific user."""
        async with self._global_lock:
            if user_id not in self._locks:
                self._locks[user_id] = asyncio.Lock()
            return self._locks[user_id]
    
    async def get_client(
        self,
        user_id: int,
        session_string: str
    ) -> Optional[Client]:
        """
        Get or create a client for a user.
        
        Returns existing connected client or creates new one.
        """
        lock = await self._get_user_lock(user_id)
        
        async with lock:
            # Check existing client
            if user_id in self._clients:
                client = self._clients[user_id]
                if client.is_connected:
                    return client
                # Client disconnected - cleanup
                try:
                    await client.stop()
                except Exception:
                    pass
                del self._clients[user_id]
            
            # Create new client
            client = create_download_client(
                api_id=self.api_id,
                api_hash=self.api_hash,
                session_string=session_string,
                name=f"user_{user_id}"
            )
            
            try:
                await client.start()
                self._clients[user_id] = client
                logger.info(f"Created session client for user {user_id}")
                return client
            except Exception as e:
                logger.error(f"Failed to start client for user {user_id}: {e}")
                return None
    
    async def release_client(self, user_id: int) -> None:
        """Release and stop a user's client."""
        lock = await self._get_user_lock(user_id)
        
        async with lock:
            if user_id in self._clients:
                client = self._clients.pop(user_id)
                try:
                    await client.stop()
                    logger.info(f"Released session client for user {user_id}")
                except Exception as e:
                    logger.warning(f"Error stopping client for {user_id}: {e}")
    
    async def stop_all(self) -> None:
        """Stop all managed clients."""
        async with self._global_lock:
            for user_id, client in list(self._clients.items()):
                try:
                    await client.stop()
                except Exception as e:
                    logger.warning(f"Error stopping client {user_id}: {e}")
            self._clients.clear()
            logger.info("All session clients stopped")
    
    def get_active_count(self) -> int:
        """Get number of active session clients."""
        return len(self._clients)
