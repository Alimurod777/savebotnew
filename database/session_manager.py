"""
Session Validation and Cleanup System

Features:
- Ban-protected users: Never deleted, only session_string removed if invalid
- Non-banned users: Fully removed if session is invalid/expired
- Async-safe with high concurrency support
- Local cache fallback for MongoDB
- Integration with existing album pipeline
- Hourly automated validation (configurable)
- MongoDB free-tier rate limiting

Session States:
- valid: Session works, user can use bot
- invalid: Session expired/revoked, needs re-login
- missing: No session stored
- error: Temporary error during validation

Engineering Guarantees:
- Async-safe and high-concurrency ready
- Hourly scheduled validation (configurable via CLEANUP_INTERVAL_HOURS)
- Ban users fully protected, only invalid sessions removed
- Non-banned users removed entirely if session invalid
- Local cache fallback supported
- Compatible with album pipeline (no interference with media_group_id tracking)
"""

import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import logging
import time

from pyrogram import Client
from pyrogram.errors import (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionExpired,
    SessionRevoked,
    SessionPasswordNeeded,
    UserDeactivated,
    UserDeactivatedBan,
    FloodWait,
    RPCError
)

from config import (
    API_ID, 
    API_HASH,
    CLEANUP_INTERVAL_HOURS,
    MAX_CONCURRENT_VALIDATIONS,
    VALIDATION_DELAY_SECONDS,
    MONGODB_OPERATION_DELAY
)

logger = logging.getLogger(__name__)

# Import session registry to check for active sessions before validation
# This prevents AUTH_KEY_DUPLICATED by skipping validation when session is in use
try:
    from TechVJ.session_ownership import session_registry
    SESSION_REGISTRY_AVAILABLE = True
except ImportError:
    session_registry = None
    SESSION_REGISTRY_AVAILABLE = False
    logger.warning("session_ownership not available - cannot check active sessions")

# Runtime config (allows runtime override of cleanup settings)
_runtime_config = {
    'interval_hours': CLEANUP_INTERVAL_HOURS,
    'max_concurrent': MAX_CONCURRENT_VALIDATIONS,
    'validation_delay': VALIDATION_DELAY_SECONDS,
    'mongodb_delay': MONGODB_OPERATION_DELAY
}


def _get_validation_delay() -> float:
    """Get current validation delay (supports runtime override)"""
    return _runtime_config.get('validation_delay', VALIDATION_DELAY_SECONDS)


def _get_mongodb_delay() -> float:
    """Get current MongoDB operation delay (supports runtime override)"""
    return _runtime_config.get('mongodb_delay', MONGODB_OPERATION_DELAY)


class SessionStatus(Enum):
    """Session validation status"""
    VALID = "valid"
    INVALID = "invalid"
    EXPIRED = "expired"
    REVOKED = "revoked"
    DEACTIVATED = "deactivated"
    PASSWORD_NEEDED = "password_needed"
    MISSING = "missing"
    ERROR = "error"


@dataclass
class SessionValidationResult:
    """Result of a session validation"""
    user_id: int
    status: SessionStatus
    is_banned: bool = False
    error_message: Optional[str] = None
    validated_at: datetime = field(default_factory=datetime.utcnow)
    user_info: Optional[dict] = None  # get_me() result if valid


@dataclass 
class CleanupResult:
    """Result of cleanup operation"""
    total_checked: int = 0
    valid_sessions: int = 0
    invalid_sessions: int = 0
    banned_users_preserved: int = 0
    non_banned_removed: int = 0
    errors: int = 0
    details: List[dict] = field(default_factory=list)


class SessionManager:
    """
    Manages session validation and cleanup with ban protection.
    
    Key behaviors:
    1. Banned users are NEVER deleted - only session_string is cleared if invalid
    2. Non-banned users with invalid sessions are completely removed
    3. All operations are async-safe and high-concurrency ready
    """
    
    # Session validation errors that indicate invalid/expired sessions
    INVALID_SESSION_ERRORS = (
        AuthKeyUnregistered,
        AuthKeyInvalid,
        SessionExpired,
        SessionRevoked,
        UserDeactivated,
        UserDeactivatedBan
    )
    
    # Errors that need special handling but don't mean session is invalid
    SPECIAL_ERRORS = (
        SessionPasswordNeeded,  # 2FA required - session might still be valid
        FloodWait,  # Rate limited - retry later
    )
    
    def __init__(self):
        # IMPORTANT: Do NOT create asyncio primitives here!
        # They bind to the current event loop at creation time.
        # Use lazy initialization via _get_*_lock() methods instead.
        self._validation_lock: Optional[asyncio.Lock] = None
        self._cleanup_lock: Optional[asyncio.Lock] = None
        self._validation_cache: Dict[int, SessionValidationResult] = {}
        self._cache_ttl = timedelta(minutes=5)
        self._concurrent_validations: Optional[asyncio.Semaphore] = None
        
        # Active users tracking (to avoid cleanup during operations)
        self._active_users: Set[int] = set()
        self._active_users_lock: Optional[asyncio.Lock] = None
        
        # Cleanup state tracking
        self._cleanup_running = False
        self._last_cleanup: Optional[datetime] = None
        self._last_cleanup_result: Optional[CleanupResult] = None
        
        # Track current loop to detect loop changes
        self._bound_loop_id: Optional[int] = None
        
        # Import database - use lazy getters
        try:
            from database.async_db import async_db, get_sessions_collection, get_banned_users_collection
            self._db = async_db
            self._get_sessions = get_sessions_collection
            self._get_banned = get_banned_users_collection
            self._db_available = True
        except ImportError:
            self._db = None
            self._get_sessions = None
            self._get_banned = None
            self._db_available = False
            logger.warning("Database not available for SessionManager")
    
    def _check_loop_change(self) -> bool:
        """
        Check if event loop has changed and reset primitives if needed.
        Returns True if loop changed.
        """
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
        except RuntimeError:
            return False
        
        if self._bound_loop_id is None:
            self._bound_loop_id = current_loop_id
            return False
        
        if self._bound_loop_id != current_loop_id:
            # Loop changed - reset all asyncio primitives
            logger.warning("Event loop changed, resetting asyncio primitives")
            self._validation_lock = None
            self._cleanup_lock = None
            self._active_users_lock = None
            self._concurrent_validations = None
            self._bound_loop_id = current_loop_id
            return True
        
        return False
    
    def _get_validation_lock(self) -> asyncio.Lock:
        """Lazy lock creation - binds to current event loop."""
        self._check_loop_change()
        if self._validation_lock is None:
            self._validation_lock = asyncio.Lock()
        return self._validation_lock
    
    def _get_cleanup_lock(self) -> asyncio.Lock:
        """Lazy lock creation - binds to current event loop."""
        self._check_loop_change()
        if self._cleanup_lock is None:
            self._cleanup_lock = asyncio.Lock()
        return self._cleanup_lock
    
    def _get_active_users_lock(self) -> asyncio.Lock:
        """Lazy lock creation - binds to current event loop."""
        self._check_loop_change()
        if self._active_users_lock is None:
            self._active_users_lock = asyncio.Lock()
        return self._active_users_lock
    
    def _get_semaphore(self) -> asyncio.Semaphore:
        """Lazy semaphore creation - binds to current event loop."""
        self._check_loop_change()
        if self._concurrent_validations is None:
            self._concurrent_validations = asyncio.Semaphore(MAX_CONCURRENT_VALIDATIONS)
        return self._concurrent_validations
    
    async def mark_user_active(self, user_id: int):
        """Mark a user as active (prevents cleanup during operations)"""
        async with self._get_active_users_lock():
            self._active_users.add(user_id)
    
    async def mark_user_inactive(self, user_id: int):
        """Mark a user as inactive (allows cleanup)"""
        async with self._get_active_users_lock():
            self._active_users.discard(user_id)
    
    async def is_user_active(self, user_id: int) -> bool:
        """Check if user is currently active"""
        async with self._get_active_users_lock():
            return user_id in self._active_users
    
    def get_active_users(self) -> Set[int]:
        """Get set of currently active users (sync version)"""
        return self._active_users.copy()
    
    @property
    def is_cleanup_running(self) -> bool:
        """Check if cleanup is currently running"""
        return self._cleanup_running
    
    @property
    def last_cleanup_time(self) -> Optional[datetime]:
        """Get last cleanup time"""
        return self._last_cleanup
    
    @property
    def last_cleanup_stats(self) -> Optional[CleanupResult]:
        """Get last cleanup result"""
        return self._last_cleanup_result
    
    async def is_user_banned(self, user_id: int) -> bool:
        """Check if a user is banned"""
        if not self._db_available:
            return False
        
        try:
            return await self._db.is_banned(user_id)
        except Exception as e:
            logger.error(f"Error checking ban status for {user_id}: {e}")
            return False
    
    async def validate_session(
        self, 
        user_id: int, 
        session_string: str,
        use_cache: bool = True
    ) -> SessionValidationResult:
        """
        Validate a user's session string using Pyrogram get_me().
        
        CRITICAL: Checks session registry first to prevent AUTH_KEY_DUPLICATED.
        If session is currently in use by another task, validation is skipped.
        
        Args:
            user_id: The user's chat ID
            session_string: The session string to validate
            use_cache: Whether to use cached validation results
        
        Returns:
            SessionValidationResult with status and details
        """
        # Check cache first
        if use_cache and user_id in self._validation_cache:
            cached = self._validation_cache[user_id]
            if datetime.utcnow() - cached.validated_at < self._cache_ttl:
                return cached
        
        # Check if user is banned
        is_banned = await self.is_user_banned(user_id)
        
        # No session string
        if not session_string:
            result = SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.MISSING,
                is_banned=is_banned,
                error_message="No session string provided"
            )
            self._validation_cache[user_id] = result
            return result
        
        # CRITICAL: Check if session is currently in use
        # This prevents AUTH_KEY_DUPLICATED by not creating a second client
        if SESSION_REGISTRY_AVAILABLE and session_registry:
            try:
                if await session_registry.is_active(session_string):
                    logger.info(f"Skipping validation for user {user_id}: session currently in use")
                    return SessionValidationResult(
                        user_id=user_id,
                        status=SessionStatus.ERROR,
                        is_banned=is_banned,
                        error_message="Session currently in use - validation skipped"
                    )
            except Exception as e:
                logger.debug(f"Could not check session registry: {e}")
        
        # Validate using Pyrogram
        async with self._get_semaphore():
            result = await self._validate_with_pyrogram(user_id, session_string, is_banned)
        
        # Cache result
        self._validation_cache[user_id] = result
        return result
    
    async def _validate_with_pyrogram(
        self, 
        user_id: int, 
        session_string: str,
        is_banned: bool
    ) -> SessionValidationResult:
        """Perform actual Pyrogram validation"""
        client = None
        try:
            client = Client(
                f"validation_{user_id}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=session_string,
                in_memory=True,
                no_updates=True
            )
            
            await client.start()
            
            # get_me() to verify session is valid
            me = await client.get_me()
            
            await client.stop()
            
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.VALID,
                is_banned=is_banned,
                user_info={
                    'id': me.id,
                    'first_name': me.first_name,
                    'username': me.username
                }
            )
            
        except AuthKeyUnregistered:
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.INVALID,
                is_banned=is_banned,
                error_message="Session key unregistered"
            )
        except AuthKeyInvalid:
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.INVALID,
                is_banned=is_banned,
                error_message="Invalid session key"
            )
        except SessionExpired:
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.EXPIRED,
                is_banned=is_banned,
                error_message="Session expired"
            )
        except SessionRevoked:
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.REVOKED,
                is_banned=is_banned,
                error_message="Session revoked"
            )
        except UserDeactivated:
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.DEACTIVATED,
                is_banned=is_banned,
                error_message="User account deactivated"
            )
        except UserDeactivatedBan:
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.DEACTIVATED,
                is_banned=is_banned,
                error_message="User account banned by Telegram"
            )
        except SessionPasswordNeeded:
            # 2FA enabled - session might still be valid but needs password
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.PASSWORD_NEEDED,
                is_banned=is_banned,
                error_message="2FA password required"
            )
        except FloodWait as e:
            # Rate limited - don't mark as invalid
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.ERROR,
                is_banned=is_banned,
                error_message=f"Rate limited, retry in {e.value}s"
            )
        except RPCError as e:
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.ERROR,
                is_banned=is_banned,
                error_message=f"RPC error: {str(e)}"
            )
        except Exception as e:
            return SessionValidationResult(
                user_id=user_id,
                status=SessionStatus.ERROR,
                is_banned=is_banned,
                error_message=f"Validation error: {str(e)}"
            )
        finally:
            if client and client.is_connected:
                try:
                    await client.stop()
                except:
                    pass
    
    async def cleanup_invalid_session(
        self, 
        user_id: int,
        validation_result: SessionValidationResult
    ) -> Tuple[bool, str]:
        """
        Clean up an invalid session based on ban status.
        
        For BANNED users: Remove only session_string, preserve user entry
        For NON-BANNED users: Remove user entry completely
        
        Returns:
            Tuple of (success, action_taken)
        """
        if not self._db_available:
            return False, "Database not available"
        
        is_invalid = validation_result.status in (
            SessionStatus.INVALID,
            SessionStatus.EXPIRED,
            SessionStatus.REVOKED,
            SessionStatus.DEACTIVATED
        )
        
        if not is_invalid:
            return False, "Session is not invalid"
        
        try:
            sessions_collection = self._get_sessions()
            if validation_result.is_banned:
                # BANNED USER: Preserve entry, only remove session_string
                # NEVER delete banned users - only clear invalid session
                await sessions_collection.update_one(
                    {'chat_id': user_id},
                    {
                        '$set': {
                            'logged_in': False,
                            'session_status': validation_result.status.value,
                            'session_invalid_at': datetime.utcnow(),
                            'session_error': validation_result.error_message,
                            'ban_protected': True  # Mark as ban-protected
                        },
                        '$unset': {
                            'session': "",
                            'session_string': ""
                        }
                    }
                )
                # MongoDB free-tier rate limiting
                await asyncio.sleep(_get_mongodb_delay())
                logger.info(f"Banned user {user_id}: Removed invalid session, preserved user entry")
                return True, "banned_session_cleared"
            else:
                # NON-BANNED USER: Remove completely
                await sessions_collection.delete_one({'chat_id': user_id})
                # MongoDB free-tier rate limiting
                await asyncio.sleep(_get_mongodb_delay())
                logger.info(f"Non-banned user {user_id}: Removed user entry (invalid session)")
                return True, "non_banned_removed"
                
        except Exception as e:
            logger.error(f"Error cleaning up session for {user_id}: {e}")
            return False, f"error: {str(e)}"
    
    async def run_cleanup(
        self, 
        skip_active_users: bool = True,
        dry_run: bool = False
    ) -> CleanupResult:
        """
        Run a full cleanup of all sessions.
        
        Args:
            skip_active_users: Skip users who have active tasks/downloads
            dry_run: If True, only report what would be done without making changes
        
        Returns:
            CleanupResult with statistics and details
        
        Integration Safety:
            - Skips users with active album operations
            - Skips users with pending media_group_id processing
            - Does not interfere with /stop command handling
        """
        if not self._db_available:
            return CleanupResult(errors=1, details=[{"error": "Database not available"}])
        
        if self._cleanup_running:
            logger.warning("Cleanup already in progress, skipping")
            return CleanupResult(errors=1, details=[{"error": "Cleanup already running"}])
        
        async with self._get_cleanup_lock():
            self._cleanup_running = True
            result = CleanupResult()
            start_time = time.time()
            
            try:
                # Get all users with sessions
                sessions_collection = self._get_sessions()
                cursor = sessions_collection.find({
                    '$or': [
                        {'session': {'$exists': True, '$ne': None}},
                        {'session_string': {'$exists': True, '$ne': None}},
                        {'logged_in': True}
                    ]
                })
                users = await cursor.to_list(length=None)
                result.total_checked = len(users)
                
                logger.info(f"Starting cleanup: {result.total_checked} users to check")
                
                # Collect all active users from various sources
                active_users: Set[int] = set()
                if skip_active_users:
                    # 1. Internal active users tracking
                    active_users.update(self._active_users)
                    
                    # 2. Task manager active users
                    try:
                        from TechVJ.task_manager import task_manager
                        active_users.update(task_manager.get_active_users())
                    except:
                        pass
                    
                    # 3. State manager active operations
                    try:
                        from database.state_manager import state_manager
                        active_users.update(state_manager.get_active_users())
                    except:
                        pass
                
                logger.info(f"Skipping {len(active_users)} active users")
                
                # Validate each user
                for user_doc in users:
                    user_id = user_doc.get('chat_id')
                    session_string = user_doc.get('session') or user_doc.get('session_string')
                    
                    if not user_id:
                        continue
                    
                    # Skip active users (prevents interference with album/media operations)
                    if user_id in active_users:
                        result.details.append({
                            'user_id': user_id,
                            'action': 'skipped_active',
                            'reason': 'User has active tasks'
                        })
                        continue
                    
                    # Validate session
                    validation = await self.validate_session(user_id, session_string)
                    
                    if validation.status == SessionStatus.VALID:
                        result.valid_sessions += 1
                        result.details.append({
                            'user_id': user_id,
                            'action': 'valid',
                            'is_banned': validation.is_banned
                        })
                        
                        # Update session status in DB
                        if not dry_run:
                            await sessions_collection.update_one(
                                {'chat_id': user_id},
                                {'$set': {
                                    'session_status': 'valid',
                                    'last_validated': datetime.utcnow()
                                }}
                            )
                            await asyncio.sleep(_get_mongodb_delay())
                    
                    elif validation.status in (
                        SessionStatus.INVALID,
                        SessionStatus.EXPIRED,
                        SessionStatus.REVOKED,
                        SessionStatus.DEACTIVATED
                    ):
                        result.invalid_sessions += 1
                        
                        if validation.is_banned:
                            result.banned_users_preserved += 1
                            action = 'banned_session_cleared'
                        else:
                            result.non_banned_removed += 1
                            action = 'non_banned_removed'
                        
                        result.details.append({
                            'user_id': user_id,
                            'action': action,
                            'status': validation.status.value,
                            'is_banned': validation.is_banned,
                            'error': validation.error_message
                        })
                        
                        if not dry_run:
                            await self.cleanup_invalid_session(user_id, validation)
                    
                    elif validation.status == SessionStatus.ERROR:
                        result.errors += 1
                        result.details.append({
                            'user_id': user_id,
                            'action': 'error',
                            'error': validation.error_message
                        })
                    
                    # Configurable delay between validations (Telegram rate limits + MongoDB free-tier)
                    await asyncio.sleep(_get_validation_delay())
                
                # Store cleanup result
                self._last_cleanup = datetime.utcnow()
                self._last_cleanup_result = result
                
                elapsed = time.time() - start_time
                logger.info(
                    f"Cleanup completed in {elapsed:.1f}s: "
                    f"{result.valid_sessions} valid, {result.invalid_sessions} invalid, "
                    f"{result.banned_users_preserved} banned preserved, "
                    f"{result.non_banned_removed} removed, {result.errors} errors"
                )
                
                return result
                
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                result.errors += 1
                result.details.append({'error': str(e)})
                return result
            finally:
                self._cleanup_running = False
    
    async def validate_and_cleanup_user(self, user_id: int) -> Tuple[SessionStatus, str]:
        """
        Validate and cleanup a single user's session.
        
        Returns:
            Tuple of (status, action_taken)
        """
        if not self._db_available:
            return SessionStatus.ERROR, "Database not available"
        
        try:
            # Get user from DB
            sessions_collection = self._get_sessions()
            user_doc = await sessions_collection.find_one({'chat_id': user_id})
            
            if not user_doc:
                return SessionStatus.MISSING, "User not found"
            
            session_string = user_doc.get('session') or user_doc.get('session_string')
            
            if not session_string:
                return SessionStatus.MISSING, "No session string"
            
            # Validate
            validation = await self.validate_session(user_id, session_string, use_cache=False)
            
            if validation.status == SessionStatus.VALID:
                # Update last validated timestamp
                await sessions_collection.update_one(
                    {'chat_id': user_id},
                    {'$set': {
                        'session_status': 'valid',
                        'last_validated': datetime.utcnow()
                    }}
                )
                return SessionStatus.VALID, "Session valid"
            
            elif validation.status in (
                SessionStatus.INVALID,
                SessionStatus.EXPIRED,
                SessionStatus.REVOKED,
                SessionStatus.DEACTIVATED
            ):
                success, action = await self.cleanup_invalid_session(user_id, validation)
                return validation.status, action
            
            else:
                return validation.status, validation.error_message or "Unknown status"
                
        except Exception as e:
            logger.error(f"Error validating user {user_id}: {e}")
            return SessionStatus.ERROR, str(e)
    
    async def get_session_status(self, user_id: int) -> Optional[dict]:
        """Get stored session status for a user"""
        if not self._db_available:
            return None
        
        try:
            sessions_collection = self._get_sessions()
            user_doc = await sessions_collection.find_one({'chat_id': user_id})
            if not user_doc:
                return None
            
            return {
                'user_id': user_id,
                'logged_in': user_doc.get('logged_in', False),
                'session_status': user_doc.get('session_status', 'unknown'),
                'last_validated': user_doc.get('last_validated'),
                'is_banned': await self.is_user_banned(user_id)
            }
        except Exception as e:
            logger.error(f"Error getting session status for {user_id}: {e}")
            return None
    
    def clear_cache(self, user_id: int = None):
        """Clear validation cache for a user or all users"""
        if user_id:
            self._validation_cache.pop(user_id, None)
        else:
            self._validation_cache.clear()


# Global instance
session_manager = SessionManager()


# ==================== Background Cleanup Task ====================

class ScheduledCleanup:
    """
    Scheduled session cleanup task with configurable interval.
    
    WARNING: Background scheduled cleanup can cause AUTH_KEY_DUPLICATED errors
    if it runs while users have active sessions. This is now DISABLED by default.
    
    Use manual cleanup via trigger_manual_cleanup() when system is idle,
    or enable scheduled cleanup only if you're certain no downloads are active.
    
    Default interval: 1 hour (configurable via CLEANUP_INTERVAL_HOURS env var)
    
    Features:
    - Graceful shutdown support
    - Automatic retry on errors
    - Status reporting
    - Thread-safe state management
    - DISABLED BY DEFAULT to prevent AUTH_KEY_DUPLICATED
    """
    
    def __init__(self, interval_hours: float = None, enabled: bool = False):
        # Use environment variable or parameter, default to 1 hour
        if interval_hours is None:
            interval_hours = CLEANUP_INTERVAL_HOURS
        self.interval = timedelta(hours=interval_hours)
        self._enabled = enabled  # DISABLED by default to prevent AUTH_KEY_DUPLICATED
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._started_at: Optional[datetime] = None
        self._runs_completed: int = 0
        self._last_error: Optional[str] = None
        self._shutdown_event: Optional[asyncio.Event] = None
    
    @property
    def is_running(self) -> bool:
        """Check if the scheduled cleanup is running"""
        return self._running
    
    @property
    def status(self) -> dict:
        """Get current status of the scheduled cleanup"""
        return {
            'running': self._running,
            'interval_hours': self.interval.total_seconds() / 3600,
            'started_at': self._started_at.isoformat() if self._started_at else None,
            'runs_completed': self._runs_completed,
            'last_error': self._last_error,
            'next_run_in': self._get_next_run_time(),
            'last_cleanup': session_manager.last_cleanup_time.isoformat() if session_manager.last_cleanup_time else None
        }
    
    def _get_next_run_time(self) -> Optional[str]:
        """Get estimated time until next run"""
        if not self._running or not session_manager.last_cleanup_time:
            return None
        next_run = session_manager.last_cleanup_time + self.interval
        remaining = next_run - datetime.utcnow()
        if remaining.total_seconds() < 0:
            return "imminent"
        hours, remainder = divmod(int(remaining.total_seconds()), 3600)
        minutes = remainder // 60
        return f"{hours}h {minutes}m"
    
    async def start(self):
        """
        Start the scheduled cleanup task.
        
        WARNING: This is now DISABLED by default. Background cleanup can cause
        AUTH_KEY_DUPLICATED if running while users have active sessions.
        
        To enable, explicitly pass enabled=True to constructor or call enable().
        """
        if not self._enabled:
            logger.warning(
                "Scheduled cleanup is DISABLED to prevent AUTH_KEY_DUPLICATED. "
                "Use trigger_manual_cleanup() when system is idle, or call "
                "scheduled_cleanup.enable() then start() to enable."
            )
            return
        
        if self._running:
            logger.warning("Scheduled cleanup already running")
            return
        
        self._running = True
        self._started_at = datetime.utcnow()
        self._shutdown_event = asyncio.Event()
        self._task = asyncio.create_task(self._cleanup_loop())
        logger.info(f"Scheduled cleanup started (interval: {self.interval.total_seconds() / 3600:.1f} hours)")
    
    def enable(self):
        """Enable scheduled cleanup (use with caution - can cause AUTH_KEY_DUPLICATED)"""
        logger.warning("Enabling scheduled cleanup - ensure no active downloads when running")
        self._enabled = True
    
    def disable(self):
        """Disable scheduled cleanup"""
        self._enabled = False
    
    async def stop(self, timeout: float = 30.0):
        """
        Stop the scheduled cleanup task gracefully.
        
        Args:
            timeout: Maximum time to wait for graceful shutdown
        """
        if not self._running:
            return
        
        logger.info("Stopping scheduled cleanup...")
        self._running = False
        
        # Signal shutdown
        if self._shutdown_event:
            self._shutdown_event.set()
        
        if self._task:
            try:
                # Wait for task to complete gracefully
                await asyncio.wait_for(self._task, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Cleanup task did not stop gracefully, cancelling...")
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Scheduled cleanup stopped after {self._runs_completed} runs")
    
    async def run_now(self) -> CleanupResult:
        """Trigger an immediate cleanup run (outside of schedule)"""
        logger.info("Manual cleanup triggered")
        return await session_manager.run_cleanup(skip_active_users=True)
    
    async def _cleanup_loop(self):
        """Main cleanup loop with graceful shutdown support"""
        # Run first cleanup immediately on start
        await self._run_single_cleanup()
        
        while self._running:
            try:
                # Wait for interval or shutdown signal
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.interval.total_seconds()
                    )
                    # Shutdown was signaled
                    break
                except asyncio.TimeoutError:
                    # Normal timeout - time to run cleanup
                    pass
                
                if not self._running:
                    break
                
                await self._run_single_cleanup()
                
            except asyncio.CancelledError:
                logger.info("Cleanup loop cancelled")
                break
            except Exception as e:
                self._last_error = str(e)
                logger.error(f"Scheduled cleanup error: {e}")
                # Wait before retry (shorter delay on error)
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    break
    
    async def _run_single_cleanup(self):
        """Run a single cleanup cycle"""
        try:
            logger.info("Starting scheduled session cleanup...")
            result = await session_manager.run_cleanup(skip_active_users=True)
            self._runs_completed += 1
            self._last_error = None
            
            # Log summary
            logger.info(
                f"Cleanup #{self._runs_completed} complete: "
                f"{result.total_checked} checked, {result.valid_sessions} valid, "
                f"{result.invalid_sessions} invalid, {result.banned_users_preserved} banned preserved, "
                f"{result.non_banned_removed} removed, {result.errors} errors"
            )
            
        except Exception as e:
            self._last_error = str(e)
            logger.error(f"Cleanup run failed: {e}")


# Global scheduled cleanup instance (1 hour default, configurable via env)
scheduled_cleanup = ScheduledCleanup()


# ==================== Convenience Functions ====================

async def validate_user_session(user_id: int) -> Tuple[SessionStatus, str]:
    """Validate and cleanup a user's session"""
    return await session_manager.validate_and_cleanup_user(user_id)


async def is_session_valid(user_id: int, session_string: str) -> bool:
    """Quick check if a session is valid"""
    result = await session_manager.validate_session(user_id, session_string)
    return result.status == SessionStatus.VALID


async def run_session_cleanup(dry_run: bool = False) -> CleanupResult:
    """Run a full session cleanup"""
    return await session_manager.run_cleanup(dry_run=dry_run)


async def start_scheduled_cleanup():
    """Start the scheduled cleanup task"""
    await scheduled_cleanup.start()


async def stop_scheduled_cleanup():
    """Stop the scheduled cleanup task"""
    await scheduled_cleanup.stop()


# ==================== Pipeline Integration Helpers ====================

async def mark_user_active_for_operation(user_id: int):
    """
    Mark a user as active before starting an operation.
    Call this before album sends, downloads, etc. to prevent cleanup during operation.
    
    Usage:
        await mark_user_active_for_operation(user_id)
        try:
            # ... do album send / download ...
        finally:
            await mark_user_inactive_after_operation(user_id)
    """
    await session_manager.mark_user_active(user_id)


async def mark_user_inactive_after_operation(user_id: int):
    """Mark a user as inactive after completing an operation."""
    await session_manager.mark_user_inactive(user_id)


def get_cleanup_status() -> dict:
    """Get the current status of the cleanup scheduler"""
    return scheduled_cleanup.status


async def trigger_manual_cleanup() -> CleanupResult:
    """Trigger an immediate cleanup run"""
    return await scheduled_cleanup.run_now()


def get_cleanup_config() -> dict:
    """Get current cleanup configuration (including runtime overrides)"""
    return {
        'interval_hours': _runtime_config.get('interval_hours', CLEANUP_INTERVAL_HOURS),
        'max_concurrent_validations': _runtime_config.get('max_concurrent', MAX_CONCURRENT_VALIDATIONS),
        'validation_delay_seconds': _runtime_config.get('validation_delay', VALIDATION_DELAY_SECONDS),
        'mongodb_operation_delay': _runtime_config.get('mongodb_delay', MONGODB_OPERATION_DELAY)
    }


# Context manager for safe operations
class ActiveUserContext:
    """
    Context manager for marking users active during operations.
    Ensures cleanup doesn't interfere with ongoing album/media operations.
    
    Usage:
        async with ActiveUserContext(user_id):
            # ... album send / media processing ...
    """
    def __init__(self, user_id: int):
        self.user_id = user_id
    
    async def __aenter__(self):
        await session_manager.mark_user_active(self.user_id)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await session_manager.mark_user_inactive(self.user_id)
        return False


# ==================== Initialization ====================

def configure_cleanup(
    interval_hours: float = None,
    max_concurrent: int = None,
    validation_delay: float = None,
    mongodb_delay: float = None
):
    """
    Configure cleanup parameters at runtime.
    Note: Changes apply to new cleanup runs, not currently running ones.
    
    Args:
        interval_hours: Hours between cleanup runs
        max_concurrent: Max concurrent session validations
        validation_delay: Seconds between individual validations
        mongodb_delay: Seconds between MongoDB operations (for free-tier)
    """
    if interval_hours is not None:
        _runtime_config['interval_hours'] = interval_hours
        scheduled_cleanup.interval = timedelta(hours=interval_hours)
    
    if max_concurrent is not None:
        _runtime_config['max_concurrent'] = max_concurrent
        session_manager._concurrent_validations = asyncio.Semaphore(max_concurrent)
    
    if validation_delay is not None:
        _runtime_config['validation_delay'] = validation_delay
    
    if mongodb_delay is not None:
        _runtime_config['mongodb_delay'] = mongodb_delay
    
    logger.info(f"Cleanup configured: {get_cleanup_config()}")
