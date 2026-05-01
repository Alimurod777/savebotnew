"""
Safe Auto-Inactivity Session Manager

CRITICAL: This implements SOFT SESSION STOP - NOT LOGOUT.

Design Goals:
- Stop idle Pyrogram client connections after 10-15 min inactivity
- Preserve session strings (no re-login required)
- Transparent auto-restart when user returns
- Protect active downloads from premature stop
- No AUTH_KEY_DUPLICATED, no session invalidation

FORBIDDEN:
- client.log_out()      # Invalidates session permanently
- client.terminate()    # Destroys session data
- Session deletion APIs # Triggers security alerts

ALLOWED:
- await client.stop()   # Safe connection close, session preserved

Author: WOODcraft Production-Grade Implementation
"""

import asyncio
import time
import uuid
import logging
from typing import Optional, Dict, Set, Callable, Any
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from enum import Enum, auto

from pyrogram import Client
from pyrogram.errors import (
    AuthKeyUnregistered,
    AuthKeyDuplicated,
    SessionRevoked,
    UserDeactivated,
    FloodWait,
)

from config import API_ID, API_HASH, get_client_params

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

# Inactivity threshold: 10 minutes (600 seconds)
# Can be set up to 15 minutes (900 seconds) for less aggressive cleanup
INACTIVITY_TIMEOUT = 600  # seconds

# How often to check for inactive sessions
CLEANUP_INTERVAL = 60  # seconds

# Grace period after activity before allowing stop
# Prevents stop during rapid command sequences
ACTIVITY_GRACE_PERIOD = 30  # seconds

# Maximum time to wait for client.stop()
STOP_TIMEOUT = 15.0  # seconds

# Delay before reconnect (prevents AUTH_KEY_DUPLICATED)
RECONNECT_DELAY = 5.0  # seconds

# Maximum start retries
MAX_START_RETRIES = 3


# ============================================================================
# SESSION STATE
# ============================================================================

class SessionState(Enum):
    """State of a managed session."""
    STOPPED = auto()      # Client not connected
    STARTING = auto()     # Client is starting
    ACTIVE = auto()       # Client connected and in use
    IDLE = auto()         # Client connected but idle
    STOPPING = auto()     # Client is stopping
    ERROR = auto()        # Session has errors


@dataclass
class ManagedSession:
    """
    A user session with activity tracking.
    
    INVARIANTS:
    - session_string is NEVER deleted (preserves login)
    - client may be None when stopped (normal)
    - Only client.stop() is called, NEVER log_out()
    """
    user_id: int
    session_string: str
    client: Optional[Client] = None
    state: SessionState = SessionState.STOPPED
    last_activity: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    start_count: int = 0
    stop_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    
    # Active task tracking - prevents stop during downloads
    active_tasks: Set[str] = field(default_factory=set)
    
    def touch(self) -> None:
        """Update last activity timestamp."""
        self.last_activity = time.time()
    
    def is_idle(self, timeout: float = INACTIVITY_TIMEOUT) -> bool:
        """Check if session has been idle beyond timeout."""
        if self.active_tasks:
            return False  # Has active tasks, not idle
        return time.time() - self.last_activity > timeout
    
    def can_stop(self) -> bool:
        """Check if session can be safely stopped."""
        if self.state not in (SessionState.ACTIVE, SessionState.IDLE):
            return False
        if self.active_tasks:
            return False  # Protect active downloads
        # Ensure grace period has passed
        return time.time() - self.last_activity > ACTIVITY_GRACE_PERIOD


@dataclass
class TaskGuard:
    """
    RAII guard that protects a session from being stopped during a task.
    
    Usage:
        async with manager.protect_task(user_id, "download_123"):
            # Session won't be stopped during this block
            await do_download(client)
    """
    manager: 'InactivityManager'
    user_id: int
    task_id: str
    
    async def __aenter__(self):
        await self.manager._register_task(self.user_id, self.task_id)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.manager._unregister_task(self.user_id, self.task_id)
        return False


# ============================================================================
# INACTIVITY MANAGER
# ============================================================================

class InactivityManager:
    """
    Production-grade auto-inactivity session manager.
    
    GUARANTEES:
    - Sessions are soft-stopped after inactivity (connection closed)
    - Session strings are NEVER deleted (no re-login)
    - Active downloads are protected from stop
    - Transparent restart when user returns
    - No AUTH_KEY_DUPLICATED errors
    - No race conditions (per-user locks)
    
    LIFECYCLE:
    1. User sends command -> get_client() called
    2. If stopped, client is started transparently
    3. Activity timestamp updated on each command
    4. After 10-15 min idle, client.stop() called
    5. Session string preserved for next use
    6. User returns -> client started again, same session
    """
    
    def __init__(
        self,
        inactivity_timeout: float = INACTIVITY_TIMEOUT,
        cleanup_interval: float = CLEANUP_INTERVAL,
    ):
        """
        Initialize the inactivity manager.
        
        Args:
            inactivity_timeout: Seconds of inactivity before soft-stop
            cleanup_interval: Seconds between cleanup checks
        """
        self._inactivity_timeout = inactivity_timeout
        self._cleanup_interval = cleanup_interval
        
        # User sessions: user_id -> ManagedSession
        self._sessions: Dict[int, ManagedSession] = {}
        
        # Per-user locks to prevent race conditions
        self._user_locks: Dict[int, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()
        
        # Background cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Event loop tracking (for lock safety)
        self._loop_id: Optional[int] = None
        
        # Callbacks for session events
        self._on_session_stopped: Optional[Callable[[int], Any]] = None
        self._on_session_started: Optional[Callable[[int], Any]] = None
    
    # ========================================================================
    # LIFECYCLE
    # ========================================================================
    
    async def start(self) -> None:
        """Start the inactivity manager and background cleanup."""
        if self._running:
            return
        
        self._running = True
        self._loop_id = id(asyncio.get_running_loop())
        self._cleanup_task = asyncio.create_task(
            self._cleanup_loop(),
            name="inactivity_cleanup"
        )
        logger.info(
            f"InactivityManager started (timeout={self._inactivity_timeout}s, "
            f"interval={self._cleanup_interval}s)"
        )
    
    async def stop(self) -> None:
        """Stop the manager and all managed sessions."""
        self._running = False
        
        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
        
        # Stop all sessions (but preserve session strings!)
        async with self._global_lock:
            user_ids = list(self._sessions.keys())
        
        for user_id in user_ids:
            try:
                await self._stop_session(user_id, force=True)
            except Exception as e:
                logger.warning(f"Error stopping session {user_id}: {e}")
        
        logger.info("InactivityManager stopped")
    
    # ========================================================================
    # PUBLIC API
    # ========================================================================
    
    async def register_session(
        self,
        user_id: int,
        session_string: str,
        start_immediately: bool = False
    ) -> None:
        """
        Register a user's session string for management.
        
        IMPORTANT: This does NOT start the client immediately.
        Client is started on first get_client() call (lazy start).
        
        Args:
            user_id: User's Telegram ID
            session_string: Valid Pyrogram session string
            start_immediately: If True, start client now
        """
        lock = await self._get_user_lock(user_id)
        async with lock:
            if user_id in self._sessions:
                # Update existing session string
                self._sessions[user_id].session_string = session_string
                self._sessions[user_id].touch()
                logger.debug(f"Updated session for user {user_id}")
            else:
                # Create new managed session
                self._sessions[user_id] = ManagedSession(
                    user_id=user_id,
                    session_string=session_string,
                )
                logger.debug(f"Registered session for user {user_id}")
            
            if start_immediately:
                await self._ensure_started(user_id)
    
    async def unregister_session(self, user_id: int) -> None:
        """
        Unregister and stop a user's session.
        
        NOTE: This only removes from management.
        Session string should be separately preserved if needed.
        """
        lock = await self._get_user_lock(user_id)
        async with lock:
            if user_id in self._sessions:
                await self._stop_session_internal(user_id, force=True)
                del self._sessions[user_id]
                logger.info(f"Unregistered session for user {user_id}")
    
    async def get_client(
        self,
        user_id: int,
        session_string: Optional[str] = None
    ) -> Optional[Client]:
        """
        Get an active client for a user, starting if needed.
        
        This is the MAIN entry point for using sessions.
        Automatically:
        - Registers session if not registered
        - Starts client if stopped
        - Updates activity timestamp
        
        Args:
            user_id: User's Telegram ID
            session_string: Session string (required if not registered)
        
        Returns:
            Connected Pyrogram Client, or None on failure
        """
        lock = await self._get_user_lock(user_id)
        async with lock:
            # Register if needed
            if user_id not in self._sessions:
                if not session_string:
                    logger.warning(f"No session for user {user_id}")
                    return None
                self._sessions[user_id] = ManagedSession(
                    user_id=user_id,
                    session_string=session_string,
                )
            elif session_string:
                # Update session string if provided
                self._sessions[user_id].session_string = session_string
            
            # Ensure client is started
            client = await self._ensure_started(user_id)
            
            # Update activity
            if client:
                self._sessions[user_id].touch()
            
            return client
    
    @asynccontextmanager
    async def session(
        self,
        user_id: int,
        session_string: Optional[str] = None
    ):
        """
        Context manager for using a session with automatic activity tracking.
        
        Usage:
            async with manager.session(user_id, session_str) as client:
                if client:
                    await client.get_messages(...)
        """
        client = await self.get_client(user_id, session_string)
        try:
            yield client
        finally:
            # Touch on exit to extend idle timer
            await self.touch(user_id)
    
    def protect_task(self, user_id: int, task_id: Optional[str] = None) -> TaskGuard:
        """
        Create a guard that prevents session stop during a task.
        
        CRITICAL: Use this for downloads and long operations!
        
        Usage:
            async with manager.protect_task(user_id, "download_xyz"):
                await do_long_download(client)
            # Session can now be stopped if idle
        """
        if task_id is None:
            task_id = f"task_{uuid.uuid4().hex[:8]}"
        return TaskGuard(self, user_id, task_id)
    
    async def touch(self, user_id: int) -> None:
        """Update activity timestamp for a user."""
        lock = await self._get_user_lock(user_id)
        async with lock:
            if user_id in self._sessions:
                self._sessions[user_id].touch()
    
    async def force_stop(self, user_id: int) -> bool:
        """
        Force stop a user's session immediately.
        
        SAFE: Uses client.stop(), NOT log_out().
        Session string is preserved.
        
        Returns:
            True if stopped, False if no session or already stopped
        """
        return await self._stop_session(user_id, force=True)
    
    def get_session_info(self, user_id: int) -> Optional[Dict]:
        """Get information about a user's session."""
        session = self._sessions.get(user_id)
        if not session:
            return None
        
        return {
            "user_id": session.user_id,
            "state": session.state.name,
            "last_activity": session.last_activity,
            "idle_seconds": time.time() - session.last_activity,
            "active_tasks": len(session.active_tasks),
            "start_count": session.start_count,
            "stop_count": session.stop_count,
            "is_connected": session.client.is_connected if session.client else False,
        }
    
    def get_all_sessions_info(self) -> Dict[int, Dict]:
        """Get info for all managed sessions."""
        return {
            user_id: self.get_session_info(user_id)
            for user_id in self._sessions
        }
    
    def get_active_count(self) -> int:
        """Get count of currently connected sessions."""
        return sum(
            1 for s in self._sessions.values()
            if s.client and s.client.is_connected
        )
    
    def get_idle_count(self) -> int:
        """Get count of idle sessions."""
        return sum(
            1 for s in self._sessions.values()
            if s.is_idle(self._inactivity_timeout)
        )
    
    # ========================================================================
    # INTERNAL: SESSION LIFECYCLE
    # ========================================================================
    
    async def _ensure_started(self, user_id: int) -> Optional[Client]:
        """
        Ensure session is started, starting if needed.
        
        MUST be called with user lock held.
        """
        session = self._sessions.get(user_id)
        if not session:
            return None
        
        # Already connected?
        if session.client and session.client.is_connected:
            session.state = SessionState.ACTIVE
            return session.client
        
        # Need to start
        session.state = SessionState.STARTING
        
        try:
            client = await self._create_and_start_client(session)
            session.client = client
            session.state = SessionState.ACTIVE
            session.start_count += 1
            session.touch()
            
            logger.info(f"Session started for user {user_id} (starts: {session.start_count})")
            
            if self._on_session_started:
                try:
                    result = self._on_session_started(user_id)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.warning(f"Session started callback error: {e}")
            
            return client
            
        except Exception as e:
            session.state = SessionState.ERROR
            session.error_count += 1
            session.last_error = str(e)
            logger.error(f"Failed to start session for user {user_id}: {e}")
            return None
    
    async def _create_and_start_client(self, session: ManagedSession) -> Client:
        """Create and start a Pyrogram client for a session."""
        # Get validated platform-consistent fingerprint
        fp = get_client_params(session.user_id)
        
        client = Client(
            name=f"user_{session.user_id}_{uuid.uuid4().hex[:8]}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session.session_string,
            in_memory=True,
            no_updates=True,
            workers=4,
            sleep_threshold=60,
            max_concurrent_transmissions=8,
            device_model=fp['device_model'],
            system_version=fp['system_version'],
            app_version=fp['app_version'],
            lang_code=fp['lang_code'],
        )
        
        last_error = None
        for attempt in range(MAX_START_RETRIES):
            try:
                await asyncio.wait_for(client.start(), timeout=30.0)
                return client
                
            except (AuthKeyUnregistered, SessionRevoked, UserDeactivated) as e:
                # Fatal: Session is invalid, don't retry
                logger.error(f"Session invalid for user {session.user_id}: {e}")
                raise
                
            except AuthKeyDuplicated as e:
                # Wait and retry - connection wasn't fully closed
                logger.warning(f"AUTH_KEY_DUPLICATED for user {session.user_id}, waiting...")
                await asyncio.sleep(RECONNECT_DELAY * (attempt + 1))
                last_error = e
                continue
                
            except FloodWait as e:
                wait = min(getattr(e, 'value', 30), 60)
                logger.warning(f"FloodWait({wait}s) for user {session.user_id}")
                if attempt < MAX_START_RETRIES - 1:
                    await asyncio.sleep(wait)
                    continue
                raise
                
            except asyncio.TimeoutError:
                last_error = "Connection timeout"
                if attempt < MAX_START_RETRIES - 1:
                    await asyncio.sleep(RECONNECT_DELAY)
                    continue
                raise Exception(f"Start timeout after {MAX_START_RETRIES} attempts")
                
            except Exception as e:
                last_error = e
                if attempt < MAX_START_RETRIES - 1:
                    await asyncio.sleep(RECONNECT_DELAY)
                    continue
                raise
        
        raise Exception(f"Failed to start after {MAX_START_RETRIES} retries: {last_error}")
    
    async def _stop_session(self, user_id: int, force: bool = False) -> bool:
        """Stop a session with proper locking."""
        lock = await self._get_user_lock(user_id)
        async with lock:
            return await self._stop_session_internal(user_id, force)
    
    async def _stop_session_internal(self, user_id: int, force: bool = False) -> bool:
        """
        Stop a session's client connection.
        
        CRITICAL: Uses client.stop() ONLY - NEVER log_out()!
        Session string is preserved for later restart.
        
        MUST be called with user lock held.
        """
        session = self._sessions.get(user_id)
        if not session:
            return False
        
        if not session.client:
            session.state = SessionState.STOPPED
            return False
        
        # Check if can stop (unless forced)
        if not force and not session.can_stop():
            logger.debug(f"Cannot stop session {user_id}: active tasks or grace period")
            return False
        
        session.state = SessionState.STOPPING
        client = session.client
        session.client = None
        
        try:
            if client.is_connected:
                # SAFE STOP - preserves session, just closes connection
                await asyncio.wait_for(client.stop(), timeout=STOP_TIMEOUT)
                
                # Wait before allowing restart (prevents AUTH_KEY_DUPLICATED)
                await asyncio.sleep(RECONNECT_DELAY)
            
            session.state = SessionState.STOPPED
            session.stop_count += 1
            
            logger.info(
                f"Session stopped for user {user_id} "
                f"(stops: {session.stop_count}, idle: {time.time() - session.last_activity:.0f}s)"
            )
            
            if self._on_session_stopped:
                try:
                    result = self._on_session_stopped(user_id)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.warning(f"Session stopped callback error: {e}")
            
            return True
            
        except asyncio.TimeoutError:
            logger.warning(f"Stop timeout for user {user_id}")
            session.state = SessionState.STOPPED
            return True
            
        except Exception as e:
            logger.error(f"Error stopping session {user_id}: {e}")
            session.state = SessionState.ERROR
            session.last_error = str(e)
            return False
    
    # ========================================================================
    # INTERNAL: TASK PROTECTION
    # ========================================================================
    
    async def _register_task(self, user_id: int, task_id: str) -> None:
        """Register an active task that protects session from stop."""
        lock = await self._get_user_lock(user_id)
        async with lock:
            session = self._sessions.get(user_id)
            if session:
                session.active_tasks.add(task_id)
                session.touch()
                logger.debug(f"Task {task_id} registered for user {user_id}")
    
    async def _unregister_task(self, user_id: int, task_id: str) -> None:
        """Unregister a completed task."""
        lock = await self._get_user_lock(user_id)
        async with lock:
            session = self._sessions.get(user_id)
            if session:
                session.active_tasks.discard(task_id)
                session.touch()
                logger.debug(f"Task {task_id} unregistered for user {user_id}")
    
    # ========================================================================
    # INTERNAL: CLEANUP
    # ========================================================================
    
    async def _cleanup_loop(self) -> None:
        """Background task that stops idle sessions."""
        logger.debug("Cleanup loop started")
        
        while self._running:
            try:
                await asyncio.sleep(self._cleanup_interval)
                
                if not self._running:
                    break
                
                await self._cleanup_idle_sessions()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")
        
        logger.debug("Cleanup loop stopped")
    
    async def _cleanup_idle_sessions(self) -> None:
        """Find and stop all idle sessions."""
        # Get list of potentially idle sessions
        async with self._global_lock:
            candidates = [
                user_id for user_id, session in self._sessions.items()
                if session.is_idle(self._inactivity_timeout) and session.can_stop()
            ]
        
        if not candidates:
            return
        
        logger.debug(f"Found {len(candidates)} idle sessions to stop")
        
        # Stop each idle session
        stopped = 0
        for user_id in candidates:
            try:
                if await self._stop_session(user_id, force=False):
                    stopped += 1
            except Exception as e:
                logger.warning(f"Error stopping idle session {user_id}: {e}")
        
        if stopped:
            logger.info(f"Stopped {stopped} idle sessions")
    
    # ========================================================================
    # INTERNAL: LOCKING
    # ========================================================================
    
    async def _get_user_lock(self, user_id: int) -> asyncio.Lock:
        """Get or create a lock for a specific user."""
        async with self._global_lock:
            if user_id not in self._user_locks:
                self._user_locks[user_id] = asyncio.Lock()
            return self._user_locks[user_id]
    
    # ========================================================================
    # CALLBACKS
    # ========================================================================
    
    def on_session_stopped(self, callback: Callable[[int], Any]) -> None:
        """Register callback for when a session is stopped."""
        self._on_session_stopped = callback
    
    def on_session_started(self, callback: Callable[[int], Any]) -> None:
        """Register callback for when a session is started."""
        self._on_session_started = callback


# ============================================================================
# GLOBAL INSTANCE
# ============================================================================

# Global manager instance
_inactivity_manager: Optional[InactivityManager] = None


def get_inactivity_manager() -> InactivityManager:
    """Get or create the global InactivityManager instance."""
    global _inactivity_manager
    if _inactivity_manager is None:
        _inactivity_manager = InactivityManager()
    return _inactivity_manager


async def init_inactivity_manager(
    inactivity_timeout: float = INACTIVITY_TIMEOUT,
    cleanup_interval: float = CLEANUP_INTERVAL,
) -> InactivityManager:
    """Initialize and start the global InactivityManager."""
    global _inactivity_manager
    _inactivity_manager = InactivityManager(
        inactivity_timeout=inactivity_timeout,
        cleanup_interval=cleanup_interval,
    )
    await _inactivity_manager.start()
    return _inactivity_manager


async def shutdown_inactivity_manager() -> None:
    """Shutdown the global InactivityManager."""
    global _inactivity_manager
    if _inactivity_manager:
        await _inactivity_manager.stop()
        _inactivity_manager = None


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def get_user_client(
    user_id: int,
    session_string: Optional[str] = None
) -> Optional[Client]:
    """
    Convenience function to get a client for a user.
    
    This is the recommended way to get a client for operations.
    Handles session registration, starting, and activity tracking.
    """
    manager = get_inactivity_manager()
    return await manager.get_client(user_id, session_string)


async def touch_user_session(user_id: int) -> None:
    """Update activity timestamp for a user."""
    manager = get_inactivity_manager()
    await manager.touch(user_id)


def protect_download(user_id: int, task_id: Optional[str] = None) -> TaskGuard:
    """
    Protect a download from session timeout.
    
    Usage:
        async with protect_download(user_id, "download_123"):
            await perform_download(client)
    """
    manager = get_inactivity_manager()
    return manager.protect_task(user_id, task_id)
