"""
Activity Tracker for User Sessions

Provides decorators and utilities to automatically track user activity
for the InactivityManager.

Usage in handlers:

    from TechVJ.activity_tracker import track_activity, with_user_session
    
    # Method 1: Decorator for activity tracking
    @app.on_message(filters.command("download"))
    @track_activity
    async def download_handler(client, message):
        ...
    
    # Method 2: Get client with automatic activity tracking
    @app.on_message(filters.command("save"))
    async def save_handler(bot, message):
        user_id = message.from_user.id
        session_str = await get_user_session_string(user_id)
        
        async with with_user_session(user_id, session_str) as user_client:
            if user_client:
                await do_download(user_client, ...)
"""

import asyncio
import functools
import logging
from typing import Optional, Callable, Any

from pyrogram import Client
from pyrogram.types import Message, CallbackQuery

logger = logging.getLogger(__name__)

# Import InactivityManager
try:
    from TechVJ.inactivity_manager import (
        get_inactivity_manager,
        get_user_client,
        touch_user_session,
        protect_download,
    )
    INACTIVITY_AVAILABLE = True
except ImportError:
    INACTIVITY_AVAILABLE = False
    logger.warning("InactivityManager not available - activity tracking disabled")


def track_activity(func: Callable) -> Callable:
    """
    Decorator that tracks user activity for inactivity management.
    
    Use this on message/callback handlers to keep user sessions alive
    while they're actively using the bot.
    
    Example:
        @app.on_message(filters.command("start"))
        @track_activity
        async def start_handler(client, message):
            await message.reply("Hello!")
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Extract user_id from message or callback
        user_id = None
        
        for arg in args:
            if isinstance(arg, Message):
                if arg.from_user:
                    user_id = arg.from_user.id
                break
            elif isinstance(arg, CallbackQuery):
                if arg.from_user:
                    user_id = arg.from_user.id
                break
        
        # Touch activity before handler
        if user_id and INACTIVITY_AVAILABLE:
            try:
                await touch_user_session(user_id)
            except Exception as e:
                logger.debug(f"Activity tracking error: {e}")
        
        # Run the handler
        result = await func(*args, **kwargs)
        
        # Touch activity after handler (extends idle timer)
        if user_id and INACTIVITY_AVAILABLE:
            try:
                await touch_user_session(user_id)
            except Exception as e:
                logger.debug(f"Activity tracking error: {e}")
        
        return result
    
    return wrapper


async def with_user_session(
    user_id: int,
    session_string: Optional[str] = None,
    task_name: Optional[str] = None
):
    """
    Context manager for using a user session with activity tracking.
    
    Features:
    - Automatic client start if stopped
    - Activity tracking on entry/exit
    - Optional task protection for downloads
    
    Args:
        user_id: User's Telegram ID
        session_string: Session string (required if not registered)
        task_name: If provided, protects session from timeout during task
    
    Usage:
        async with with_user_session(user_id, session_str) as client:
            if client:
                await client.get_messages(...)
        
        # With download protection:
        async with with_user_session(user_id, session_str, "download_xyz") as client:
            await do_long_download(client)
    """
    if not INACTIVITY_AVAILABLE:
        # Fallback: return None, let caller handle
        logger.warning("InactivityManager not available")
        yield None
        return
    
    manager = get_inactivity_manager()
    
    # Get or start client
    client = await manager.get_client(user_id, session_string)
    
    if task_name:
        # Use task protection
        async with manager.protect_task(user_id, task_name):
            try:
                yield client
            finally:
                await manager.touch(user_id)
    else:
        # Simple session access
        try:
            yield client
        finally:
            await manager.touch(user_id)


class ActivityMiddleware:
    """
    Middleware class for automatic activity tracking.
    
    Can be used to wrap a Pyrogram Client and automatically
    track activity on all message handlers.
    
    Example:
        bot = Client(...)
        activity = ActivityMiddleware(bot)
        
        # In handlers, call:
        await activity.track(user_id)
    """
    
    def __init__(self, bot: Client):
        self.bot = bot
        self._user_sessions: dict = {}
    
    async def track(self, user_id: int) -> None:
        """Track activity for a user."""
        if INACTIVITY_AVAILABLE:
            await touch_user_session(user_id)
    
    async def get_session(
        self,
        user_id: int,
        session_string: str
    ) -> Optional[Client]:
        """Get a user session client."""
        if INACTIVITY_AVAILABLE:
            return await get_user_client(user_id, session_string)
        return None
    
    def protect(self, user_id: int, task_id: Optional[str] = None):
        """Get a task guard for download protection."""
        if INACTIVITY_AVAILABLE:
            return protect_download(user_id, task_id)
        # Return a no-op context manager
        return _NoOpContext()


class _NoOpContext:
    """No-op async context manager for when InactivityManager is unavailable."""
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        return False


# Convenience function for handlers
async def on_user_activity(user_id: int) -> None:
    """
    Call this in handlers to track user activity.
    
    Example:
        @app.on_message(filters.command("help"))
        async def help_handler(client, message):
            await on_user_activity(message.from_user.id)
            await message.reply("Help text...")
    """
    if INACTIVITY_AVAILABLE:
        try:
            await touch_user_session(user_id)
        except Exception:
            pass


async def get_managed_client(
    user_id: int,
    session_string: str
) -> Optional[Client]:
    """
    Get a managed client for a user.
    
    This is the recommended way to get user session clients.
    The InactivityManager handles:
    - Starting stopped sessions
    - Activity tracking
    - Automatic stop after inactivity
    
    Args:
        user_id: User's Telegram ID
        session_string: User's session string
    
    Returns:
        Connected Pyrogram Client, or None on failure
    """
    if not INACTIVITY_AVAILABLE:
        logger.warning("InactivityManager not available")
        return None
    
    return await get_user_client(user_id, session_string)


def is_inactivity_manager_available() -> bool:
    """Check if InactivityManager is available."""
    return INACTIVITY_AVAILABLE
