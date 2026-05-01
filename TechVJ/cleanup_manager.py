"""
Event-Loop-Safe Cleanup Manager

DESIGN PRINCIPLES:
1. ALL cleanup runs in the SAME event loop as the bot
2. NO threads, NO asyncio.run(), NO secondary loops
3. NO background schedulers started at import time
4. NO MongoDB or persistent state
5. Cleanup is DETERMINISTIC and SYNCHRONOUS with task lifecycle

CLEANUP TRIGGERS:
- Task completion (success/error/cancel)
- User /stop command
- Bot shutdown
- Resource timeout (configurable)

This module provides cleanup that is:
- Lazily initialized (only when first used in a running loop)
- Bound to the current event loop
- Safe to call from any coroutine in that loop
"""

import asyncio
import os
import shutil
import weakref
import logging
from typing import Dict, Set, Optional, Callable, Any
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# =============================================================================
# RESOURCE TRACKING (No Persistence)
# =============================================================================

@dataclass
class TrackedResource:
    """A resource that needs cleanup."""
    resource_id: str
    resource_type: str  # "temp_dir", "session", "download"
    path: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    user_id: Optional[int] = None
    cleanup_callback: Optional[Callable] = None
    
    async def cleanup(self) -> bool:
        """Execute cleanup for this resource."""
        try:
            if self.resource_type == "temp_dir" and self.path:
                if os.path.exists(self.path):
                    shutil.rmtree(self.path)
                    return True
            elif self.resource_type == "download" and self.path:
                if os.path.exists(self.path):
                    os.remove(self.path)
                    return True
            elif self.cleanup_callback:
                if asyncio.iscoroutinefunction(self.cleanup_callback):
                    await self.cleanup_callback()
                else:
                    self.cleanup_callback()
                return True
            return False
        except Exception as e:
            logger.warning(f"Cleanup failed for {self.resource_type}/{self.resource_id}: {e}")
            return False


# =============================================================================
# EVENT-LOOP-BOUND CLEANUP MANAGER
# =============================================================================

class CleanupManager:
    """
    Event-loop-safe cleanup manager.
    
    CRITICAL: This class is designed to be used within a SINGLE event loop.
    All operations are async and must be called from coroutines.
    
    NO background tasks are started at instantiation.
    NO threads are used.
    NO secondary event loops are created.
    """
    
    def __init__(self):
        # Resources tracked per user
        self._user_resources: Dict[int, Dict[str, TrackedResource]] = {}
        
        # Global resources (not tied to a user)
        self._global_resources: Dict[str, TrackedResource] = {}
        
        # Lock - created lazily to bind to correct loop
        self._lock: Optional[asyncio.Lock] = None
        
        # Weak references to tasks for cleanup on completion
        self._task_resources: weakref.WeakValueDictionary = weakref.WeakValueDictionary()
        
        # Cleanup timeout (resources older than this are auto-cleaned)
        self.resource_timeout = timedelta(hours=1)
        
        # Flag to track if we've been initialized in a loop
        self._initialized = False
        
        # Track bound loop to detect changes
        self._bound_loop_id: Optional[int] = None
    
    def _get_lock(self) -> asyncio.Lock:
        """
        Get or create lock bound to CURRENT event loop.
        This prevents "Future attached to different loop" errors.
        Automatically resets if event loop changes.
        """
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
        except RuntimeError:
            # No running loop - return fresh lock (not stored)
            return asyncio.Lock()
        
        # Reset lock if loop changed
        if self._bound_loop_id is not None and self._bound_loop_id != current_loop_id:
            logger.warning("CleanupManager: Event loop changed, resetting lock")
            self._lock = None
            self._initialized = False
        
        self._bound_loop_id = current_loop_id
        
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
    
    async def _ensure_initialized(self) -> None:
        """Lazy initialization - only runs once per loop."""
        if not self._initialized:
            self._initialized = True
            logger.debug("CleanupManager initialized in current event loop")
    
    # =========================================================================
    # RESOURCE REGISTRATION
    # =========================================================================
    
    async def register_temp_dir(
        self, 
        user_id: int, 
        path: str, 
        resource_id: Optional[str] = None
    ) -> str:
        """Register a temporary directory for cleanup."""
        await self._ensure_initialized()
        
        if resource_id is None:
            resource_id = f"temp_{user_id}_{id(path)}"
        
        resource = TrackedResource(
            resource_id=resource_id,
            resource_type="temp_dir",
            path=path,
            user_id=user_id
        )
        
        lock = self._get_lock()
        async with lock:
            if user_id not in self._user_resources:
                self._user_resources[user_id] = {}
            self._user_resources[user_id][resource_id] = resource
        
        return resource_id
    
    async def register_download(
        self, 
        user_id: int, 
        path: str,
        resource_id: Optional[str] = None
    ) -> str:
        """Register a downloaded file for cleanup."""
        await self._ensure_initialized()
        
        if resource_id is None:
            resource_id = f"download_{user_id}_{id(path)}"
        
        resource = TrackedResource(
            resource_id=resource_id,
            resource_type="download",
            path=path,
            user_id=user_id
        )
        
        lock = self._get_lock()
        async with lock:
            if user_id not in self._user_resources:
                self._user_resources[user_id] = {}
            self._user_resources[user_id][resource_id] = resource
        
        return resource_id
    
    async def register_callback(
        self,
        user_id: int,
        callback: Callable,
        resource_id: str
    ) -> str:
        """Register a cleanup callback."""
        await self._ensure_initialized()
        
        resource = TrackedResource(
            resource_id=resource_id,
            resource_type="callback",
            user_id=user_id,
            cleanup_callback=callback
        )
        
        lock = self._get_lock()
        async with lock:
            if user_id not in self._user_resources:
                self._user_resources[user_id] = {}
            self._user_resources[user_id][resource_id] = resource
        
        return resource_id
    
    # =========================================================================
    # CLEANUP OPERATIONS
    # =========================================================================
    
    async def cleanup_resource(self, user_id: int, resource_id: str) -> bool:
        """Clean up a specific resource."""
        lock = self._get_lock()
        async with lock:
            if user_id not in self._user_resources:
                return False
            if resource_id not in self._user_resources[user_id]:
                return False
            
            resource = self._user_resources[user_id].pop(resource_id)
        
        return await resource.cleanup()
    
    async def cleanup_user(self, user_id: int) -> int:
        """
        Clean up ALL resources for a user.
        Called on /stop or task completion.
        
        Returns number of resources cleaned.
        """
        lock = self._get_lock()
        async with lock:
            if user_id not in self._user_resources:
                return 0
            resources = list(self._user_resources[user_id].values())
            self._user_resources[user_id].clear()
        
        cleaned = 0
        for resource in resources:
            if await resource.cleanup():
                cleaned += 1
        
        logger.debug(f"Cleaned {cleaned} resources for user {user_id}")
        return cleaned
    
    async def cleanup_expired(self) -> int:
        """
        Clean up resources older than timeout.
        Call this explicitly - NOT from a background task.
        
        Example: Call at the start of a new operation.
        """
        now = datetime.utcnow()
        expired: list = []
        
        lock = self._get_lock()
        async with lock:
            for user_id, resources in self._user_resources.items():
                for resource_id, resource in list(resources.items()):
                    if now - resource.created_at > self.resource_timeout:
                        expired.append((user_id, resource_id, resource))
                        del resources[resource_id]
        
        cleaned = 0
        for user_id, resource_id, resource in expired:
            if await resource.cleanup():
                cleaned += 1
        
        if cleaned > 0:
            logger.info(f"Cleaned {cleaned} expired resources")
        
        return cleaned
    
    async def cleanup_all(self) -> int:
        """
        Clean up ALL resources.
        Called on bot shutdown.
        """
        lock = self._get_lock()
        async with lock:
            all_resources = []
            for user_id, resources in self._user_resources.items():
                all_resources.extend(resources.values())
            all_resources.extend(self._global_resources.values())
            
            self._user_resources.clear()
            self._global_resources.clear()
        
        cleaned = 0
        for resource in all_resources:
            if await resource.cleanup():
                cleaned += 1
        
        logger.info(f"Cleaned {cleaned} resources on shutdown")
        return cleaned
    
    # =========================================================================
    # CONTEXT MANAGERS
    # =========================================================================
    
    @asynccontextmanager
    async def temp_directory(self, user_id: int, base_path: str = "downloads/temp"):
        """
        Context manager for temporary directories.
        Guarantees cleanup on exit.
        
        Usage:
            async with cleanup_manager.temp_directory(user_id) as temp_dir:
                # use temp_dir
            # temp_dir is cleaned up here
        """
        import uuid
        
        unique_id = uuid.uuid4().hex[:8]
        temp_dir = os.path.join(base_path, f"user_{user_id}_{unique_id}")
        os.makedirs(temp_dir, exist_ok=True)
        
        resource_id = await self.register_temp_dir(user_id, temp_dir)
        
        try:
            yield temp_dir
        finally:
            await self.cleanup_resource(user_id, resource_id)
    
    @asynccontextmanager
    async def tracked_download(self, user_id: int, file_path: str):
        """
        Context manager for downloaded files.
        Guarantees cleanup on exit.
        """
        resource_id = await self.register_download(user_id, file_path)
        
        try:
            yield file_path
        finally:
            await self.cleanup_resource(user_id, resource_id)
    
    # =========================================================================
    # STATISTICS
    # =========================================================================
    
    async def get_stats(self) -> dict:
        """Get current resource statistics."""
        lock = self._get_lock()
        async with lock:
            user_count = len(self._user_resources)
            resource_count = sum(
                len(resources) for resources in self._user_resources.values()
            )
            
            by_type = {}
            for resources in self._user_resources.values():
                for resource in resources.values():
                    by_type[resource.resource_type] = by_type.get(resource.resource_type, 0) + 1
        
        return {
            "users_with_resources": user_count,
            "total_resources": resource_count,
            "by_type": by_type,
            "initialized": self._initialized
        }


# =============================================================================
# TASK-BOUND CLEANUP
# =============================================================================

class TaskCleanupContext:
    """
    Cleanup context bound to a specific asyncio Task.
    
    When the task completes (success, error, or cancel),
    all registered resources are automatically cleaned up.
    
    Usage:
        async with TaskCleanupContext(user_id) as ctx:
            temp_dir = await ctx.create_temp_dir()
            # ... do work ...
        # All resources cleaned up here
    """
    
    def __init__(self, user_id: int, manager: CleanupManager):
        self.user_id = user_id
        self.manager = manager
        self._resources: list = []
        self._task: Optional[asyncio.Task] = None
    
    async def __aenter__(self) -> 'TaskCleanupContext':
        self._task = asyncio.current_task()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        # Clean up all resources registered in this context
        for resource_id in self._resources:
            await self.manager.cleanup_resource(self.user_id, resource_id)
        return False  # Don't suppress exceptions
    
    async def create_temp_dir(self, base_path: str = "downloads/temp") -> str:
        """Create and track a temporary directory."""
        import uuid
        
        unique_id = uuid.uuid4().hex[:8]
        temp_dir = os.path.join(base_path, f"task_{self.user_id}_{unique_id}")
        os.makedirs(temp_dir, exist_ok=True)
        
        resource_id = await self.manager.register_temp_dir(self.user_id, temp_dir)
        self._resources.append(resource_id)
        
        return temp_dir
    
    async def track_download(self, file_path: str) -> str:
        """Track a downloaded file for cleanup."""
        resource_id = await self.manager.register_download(self.user_id, file_path)
        self._resources.append(resource_id)
        return file_path
    
    async def register_cleanup(self, callback: Callable, name: str) -> None:
        """Register a custom cleanup callback."""
        resource_id = await self.manager.register_callback(
            self.user_id, callback, f"callback_{name}"
        )
        self._resources.append(resource_id)


# =============================================================================
# GLOBAL INSTANCE (Lazily Bound)
# =============================================================================

# This is safe because it doesn't start any background tasks
# All async operations bind to the current loop when called
cleanup_manager = CleanupManager()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

async def cleanup_user_resources(user_id: int) -> int:
    """Clean up all resources for a user."""
    return await cleanup_manager.cleanup_user(user_id)


async def cleanup_on_shutdown() -> int:
    """Clean up all resources on bot shutdown."""
    return await cleanup_manager.cleanup_all()


def task_cleanup(user_id: int) -> TaskCleanupContext:
    """Create a task-bound cleanup context."""
    return TaskCleanupContext(user_id, cleanup_manager)


# =============================================================================
# INTEGRATION HOOK FOR BOT SHUTDOWN
# =============================================================================

async def register_shutdown_cleanup(app) -> None:
    """
    Register cleanup to run on bot shutdown.
    
    Call this in your bot's start() method:
        await register_shutdown_cleanup(self)
    
    This uses Pyrogram's built-in shutdown mechanism,
    NOT a separate event loop or thread.
    """
    # Store reference for shutdown
    app._cleanup_manager = cleanup_manager
    
    # The actual cleanup happens when stop() is called
    logger.info("Cleanup manager registered for shutdown")


async def on_bot_stop() -> int:
    """
    Call this in your bot's stop() method:
        await on_bot_stop()
    
    Returns:
        Number of resources cleaned up
    """
    cleaned = await cleanup_manager.cleanup_all()
    logger.info("Cleanup completed on bot stop")
    return cleaned
