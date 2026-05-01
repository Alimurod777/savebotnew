"""
DEPRECATED: Use TechVJ/progress_controller.py instead.

This module has been superseded by progress_controller.py which provides
the same functionality with better thread-safety using call_soon_threadsafe.

Do not use this module for new code.
"""

import warnings
warnings.warn(
    "TechVJ.async_progress is deprecated. Use TechVJ.progress_controller instead.",
    DeprecationWarning,
    stacklevel=2
)

# Original: Async-Safe Progress System for Telegram Media Transfers

import asyncio
import time
import logging
from typing import Optional, Dict, Callable, Any
from dataclasses import dataclass, field
from enum import Enum, auto

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified, MessageIdInvalid, RPCError

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS - Tuned for production stability
# ============================================================================

# Minimum interval between progress message edits (Telegram rate limit protection)
PROGRESS_UPDATE_INTERVAL = 1.5  # seconds

# Maximum consecutive errors before giving up on progress updates
MAX_CONSECUTIVE_ERRORS = 5

# Progress bar visual configuration
PROGRESS_BAR_LENGTH = 12
PROGRESS_FILLED = "█"
PROGRESS_EMPTY = "░"

# Speed smoothing window (for calculating average speed)
SPEED_SAMPLES = 5


# ============================================================================
# TYPE DEFINITIONS
# ============================================================================

class TransferType(Enum):
    """Transfer direction type."""
    DOWNLOAD = auto()
    UPLOAD = auto()


@dataclass
class SpeedTracker:
    """
    Track transfer speed with smoothing.
    
    Uses a rolling window of samples to smooth out speed fluctuations.
    """
    samples: list = field(default_factory=list)
    last_bytes: int = 0
    last_time: float = 0.0
    
    def update(self, current_bytes: int) -> float:
        """
        Update speed tracker and return smoothed speed.
        
        Args:
            current_bytes: Current total bytes transferred
            
        Returns:
            Smoothed speed in bytes/second
        """
        now = time.time()
        
        if self.last_time > 0:
            elapsed = now - self.last_time
            if elapsed > 0:
                bytes_diff = current_bytes - self.last_bytes
                instant_speed = bytes_diff / elapsed
                
                # Add to samples
                self.samples.append(instant_speed)
                if len(self.samples) > SPEED_SAMPLES:
                    self.samples.pop(0)
        
        self.last_bytes = current_bytes
        self.last_time = now
        
        # Return average of samples
        if self.samples:
            return sum(self.samples) / len(self.samples)
        return 0.0


@dataclass
class ProgressState:
    """
    State for a single transfer's progress tracking.
    
    All state is in-memory only, scoped to transfer lifetime.
    """
    transfer_id: str
    transfer_type: TransferType
    
    # Pyrogram objects for message editing
    client: Optional[Client] = None
    status_message: Optional[Message] = None
    
    # Transfer progress
    total_bytes: int = 0
    current_bytes: int = 0
    
    # Timing
    start_time: float = field(default_factory=time.time)
    last_update_time: float = 0.0
    last_message_text: str = ""
    
    # Speed tracking
    speed_tracker: SpeedTracker = field(default_factory=SpeedTracker)
    
    # Error tracking
    consecutive_errors: int = 0
    is_active: bool = True
    is_cancelled: bool = False
    
    # File info (optional)
    file_name: Optional[str] = None
    
    def get_percentage(self) -> float:
        """Calculate current completion percentage."""
        if self.total_bytes <= 0:
            return 0.0
        return (self.current_bytes / self.total_bytes) * 100
    
    def get_eta_seconds(self, speed: float) -> float:
        """Calculate estimated time remaining in seconds."""
        if speed <= 0:
            return 0.0
        remaining_bytes = self.total_bytes - self.current_bytes
        return remaining_bytes / speed


# ============================================================================
# PROGRESS FORMATTER
# ============================================================================

class ProgressFormatter:
    """
    Format progress information into user-friendly messages.
    
    Separated from the controller for testability and customization.
    """
    
    @staticmethod
    def format_size(size_bytes: int) -> str:
        """Format bytes into human-readable size."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    
    @staticmethod
    def format_speed(bytes_per_second: float) -> str:
        """Format speed into human-readable string."""
        if bytes_per_second < 1024:
            return f"{bytes_per_second:.0f} B/s"
        elif bytes_per_second < 1024 * 1024:
            return f"{bytes_per_second / 1024:.1f} KB/s"
        else:
            return f"{bytes_per_second / (1024 * 1024):.1f} MB/s"
    
    @staticmethod
    def format_time(seconds: float) -> str:
        """Format seconds into human-readable time."""
        if seconds < 0:
            return "calculating..."
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            mins = int(seconds / 60)
            secs = int(seconds % 60)
            return f"{mins}m {secs}s"
        else:
            hours = int(seconds / 3600)
            mins = int((seconds % 3600) / 60)
            return f"{hours}h {mins}m"
    
    @staticmethod
    def format_progress_bar(percentage: float) -> str:
        """Create a visual progress bar."""
        filled_count = int(percentage / 100 * PROGRESS_BAR_LENGTH)
        filled_count = min(filled_count, PROGRESS_BAR_LENGTH)
        empty_count = PROGRESS_BAR_LENGTH - filled_count
        return PROGRESS_FILLED * filled_count + PROGRESS_EMPTY * empty_count
    
    @classmethod
    def format_message(
        cls,
        state: ProgressState,
        speed: float
    ) -> str:
        """
        Format a complete progress message.
        
        Args:
            state: Current progress state
            speed: Current speed in bytes/second
            
        Returns:
            Formatted message string
        """
        percentage = state.get_percentage()
        eta = state.get_eta_seconds(speed)
        
        # Determine emoji based on transfer type
        if state.transfer_type == TransferType.DOWNLOAD:
            icon = "⬇️"
            action = "Downloading"
        else:
            icon = "⬆️"
            action = "Uploading"
        
        # Build progress bar
        bar = cls.format_progress_bar(percentage)
        
        # Build message
        lines = [
            f"{icon} **{action}**",
            "",
            f"`{bar}` {percentage:.1f}%",
            "",
            f"📦 {cls.format_size(state.current_bytes)} / {cls.format_size(state.total_bytes)}",
            f"⚡ {cls.format_speed(speed)}",
            f"⏱ ETA: {cls.format_time(eta)}",
        ]
        
        # Add filename if available
        if state.file_name:
            # Truncate long filenames
            name = state.file_name
            if len(name) > 30:
                name = name[:27] + "..."
            lines.insert(1, f"📄 `{name}`")
        
        return "\n".join(lines)
    
    @classmethod
    def format_completion(
        cls,
        state: ProgressState,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> str:
        """Format a completion message."""
        elapsed = time.time() - state.start_time
        avg_speed = state.total_bytes / elapsed if elapsed > 0 else 0
        
        if state.transfer_type == TransferType.DOWNLOAD:
            action = "Download"
        else:
            action = "Upload"
        
        if success:
            return (
                f"✅ **{action} Complete**\n\n"
                f"📦 Size: {cls.format_size(state.total_bytes)}\n"
                f"⏱ Time: {cls.format_time(elapsed)}\n"
                f"⚡ Avg Speed: {cls.format_speed(avg_speed)}"
            )
        else:
            return (
                f"❌ **{action} Failed**\n\n"
                f"{error_message or 'Unknown error'}"
            )


# ============================================================================
# ASYNC-SAFE PROGRESS CONTROLLER
# ============================================================================

class AsyncProgressController:
    """
    Production-grade async-safe progress controller.
    
    This replaces the previous implementation with improvements:
    - Better lock handling for Windows
    - More robust error recovery
    - Cleaner task scheduling
    
    THREAD SAFETY: This class is NOT thread-safe. All operations MUST
    occur on the same asyncio event loop.
    
    Usage:
        controller = AsyncProgressController()
        
        # Create callback for Pyrogram
        callback = controller.create_callback(
            transfer_id="unique_id",
            client=bot_client,
            status_message=status_msg,
            transfer_type=TransferType.DOWNLOAD,
            file_name="video.mp4"
        )
        
        # Use with Pyrogram
        await client.download_media(message, progress=callback)
        
        # Finalize and cleanup
        await controller.finalize(transfer_id, success=True)
        controller.cleanup(transfer_id)
    """
    
    def __init__(self):
        """Initialize the progress controller."""
        self._states: Dict[str, ProgressState] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._current_loop: Optional[asyncio.AbstractEventLoop] = None
        self._pending_updates: Dict[str, asyncio.Task] = {}
    
    def _ensure_lock(self) -> asyncio.Lock:
        """
        Get or create a lock bound to the current event loop.
        
        This pattern prevents "Future attached to a different loop" errors
        by detecting when the event loop has changed and recreating the lock.
        """
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop - create one for the lock
            current_loop = None
        
        if self._lock is None or self._current_loop is not current_loop:
            self._lock = asyncio.Lock()
            self._current_loop = current_loop
            logger.debug("Created new lock for event loop")
        
        return self._lock
    
    def create_callback(
        self,
        transfer_id: str,
        client: Client,
        status_message: Message,
        transfer_type: TransferType,
        file_name: Optional[str] = None
    ) -> Callable[[int, int], None]:
        """
        Create a Pyrogram-compatible progress callback.
        
        IMPORTANT: This returns a SYNCHRONOUS callback because that's what
        Pyrogram expects. The callback schedules async work on the event loop.
        
        Args:
            transfer_id: Unique identifier for this transfer
            client: Pyrogram client for editing messages
            status_message: Message to update with progress
            transfer_type: Download or upload
            file_name: Optional filename to display
            
        Returns:
            Synchronous callback with signature (current, total) -> None
        """
        # Create state for this transfer
        state = ProgressState(
            transfer_id=transfer_id,
            transfer_type=transfer_type,
            client=client,
            status_message=status_message,
            file_name=file_name,
            start_time=time.time()
        )
        self._states[transfer_id] = state
        
        logger.debug(f"Created progress callback for {transfer_id}")
        
        # Capture the loop at callback creation time to detect loop changes
        try:
            creation_loop = asyncio.get_running_loop()
            creation_loop_id = id(creation_loop)
        except RuntimeError:
            creation_loop = None
            creation_loop_id = None
        
        def progress_callback(current: int, total: int) -> None:
            """
            Pyrogram progress callback (SYNCHRONOUS).
            
            Pyrogram calls this from within the download/upload coroutine.
            We update state and optionally schedule an async message edit.
            
            SAFETY: Checks if event loop has changed to prevent
            "Future attached to different loop" errors.
            """
            # Update state (synchronous, safe)
            state.current_bytes = current
            state.total_bytes = total
            
            # Check if we should skip this update (throttling)
            now = time.time()
            time_since_last = now - state.last_update_time
            if time_since_last < PROGRESS_UPDATE_INTERVAL:
                return
            
            # Check if we should stop updates
            if not state.is_active or state.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                return
            
            # Mark that we're processing this update time
            state.last_update_time = now
            
            # Schedule async message edit on the event loop
            try:
                loop = asyncio.get_running_loop()
                
                # SAFETY: Check if loop has changed
                # This prevents "Future attached to different loop" errors
                if creation_loop_id is not None and id(loop) != creation_loop_id:
                    logger.debug(f"Event loop changed for {transfer_id}, skipping progress update")
                    return
                
                # Cancel any pending update task for this transfer
                if transfer_id in self._pending_updates:
                    old_task = self._pending_updates.pop(transfer_id)
                    if not old_task.done():
                        old_task.cancel()
                
                # Schedule new update
                task = loop.create_task(
                    self._do_progress_update(transfer_id),
                    name=f"progress_update_{transfer_id}"
                )
                self._pending_updates[transfer_id] = task
                
            except RuntimeError:
                # No running loop - can't schedule update
                logger.debug("No running loop for progress update")
        
        return progress_callback
    
    async def _do_progress_update(self, transfer_id: str) -> None:
        """
        Perform the actual message edit.
        
        This is the async function that edits the Telegram message.
        It runs on the main event loop.
        """
        async with self._ensure_lock():
            state = self._states.get(transfer_id)
            if not state or not state.is_active:
                return
            
            # Calculate current speed
            speed = state.speed_tracker.update(state.current_bytes)
            
            # Format the message
            message_text = ProgressFormatter.format_message(state, speed)
            
            # Skip if message hasn't changed
            if message_text == state.last_message_text:
                return
            
            # Attempt to edit message
            try:
                if state.client and state.status_message:
                    await state.client.edit_message_text(
                        chat_id=state.status_message.chat.id,
                        message_id=state.status_message.id,
                        text=message_text
                    )
                    state.last_message_text = message_text
                    state.consecutive_errors = 0
                    
            except MessageNotModified:
                # Message content is the same - not an error
                state.last_message_text = message_text
                
            except FloodWait as e:
                # Rate limited - extend the throttle window
                wait_time = getattr(e, 'value', getattr(e, 'x', 5))
                logger.warning(f"FloodWait({wait_time}s) on progress edit")
                state.last_update_time = time.time() + wait_time
                state.consecutive_errors += 1
                
            except MessageIdInvalid:
                # Message was deleted - stop updating
                logger.info(f"Progress message deleted for {transfer_id}")
                state.is_active = False
                
            except RPCError as e:
                # Other Telegram errors
                state.consecutive_errors += 1
                if state.consecutive_errors < MAX_CONSECUTIVE_ERRORS:
                    logger.debug(f"Progress RPC error: {e}")
                else:
                    logger.warning(f"Too many progress errors for {transfer_id}, stopping updates")
                    state.is_active = False
                    
            except Exception as e:
                # Unexpected errors
                state.consecutive_errors += 1
                logger.debug(f"Progress update error: {e}")
        
        # Remove from pending updates
        self._pending_updates.pop(transfer_id, None)
    
    async def finalize(
        self,
        transfer_id: str,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> None:
        """
        Finalize a transfer and show completion message.
        
        Args:
            transfer_id: The transfer to finalize
            success: Whether the transfer completed successfully
            error_message: Optional error message to display
        """
        async with self._ensure_lock():
            state = self._states.get(transfer_id)
            if not state:
                return
            
            state.is_active = False
            
            # Format completion message
            message_text = ProgressFormatter.format_completion(
                state, 
                success=success, 
                error_message=error_message
            )
            
            # Update message
            try:
                if state.client and state.status_message:
                    await state.client.edit_message_text(
                        chat_id=state.status_message.chat.id,
                        message_id=state.status_message.id,
                        text=message_text
                    )
            except Exception as e:
                logger.debug(f"Could not update completion message: {e}")
    
    def cleanup(self, transfer_id: str) -> None:
        """
        Remove transfer state from memory.
        
        Call this after finalize() to free resources.
        """
        # Cancel any pending update task
        if transfer_id in self._pending_updates:
            task = self._pending_updates.pop(transfer_id)
            if not task.done():
                task.cancel()
        
        # Remove state
        self._states.pop(transfer_id, None)
        logger.debug(f"Cleaned up progress state for {transfer_id}")
    
    def cancel(self, transfer_id: str) -> bool:
        """
        Mark a transfer as cancelled.
        
        Returns:
            True if transfer was found and cancelled
        """
        state = self._states.get(transfer_id)
        if state:
            state.is_active = False
            state.is_cancelled = True
            return True
        return False
    
    def get_active_count(self) -> int:
        """Get count of active transfers."""
        return sum(1 for s in self._states.values() if s.is_active)
    
    def cleanup_all(self) -> int:
        """
        Clean up all transfer states.
        
        Returns:
            Number of states cleaned up
        """
        count = len(self._states)
        
        # Cancel all pending tasks
        for task in self._pending_updates.values():
            if not task.done():
                task.cancel()
        self._pending_updates.clear()
        
        # Clear all states
        self._states.clear()
        
        return count


# ============================================================================
# GLOBAL SINGLETON
# ============================================================================

# Single global instance for use throughout the application
async_progress_controller = AsyncProgressController()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_download_progress(
    transfer_id: str,
    client: Client,
    status_message: Message,
    file_name: Optional[str] = None
) -> Callable[[int, int], None]:
    """
    Create a download progress callback.
    
    Convenience wrapper around the controller.
    """
    return async_progress_controller.create_callback(
        transfer_id=transfer_id,
        client=client,
        status_message=status_message,
        transfer_type=TransferType.DOWNLOAD,
        file_name=file_name
    )


def create_upload_progress(
    transfer_id: str,
    client: Client,
    status_message: Message,
    file_name: Optional[str] = None
) -> Callable[[int, int], None]:
    """
    Create an upload progress callback.
    
    Convenience wrapper around the controller.
    """
    return async_progress_controller.create_callback(
        transfer_id=transfer_id,
        client=client,
        status_message=status_message,
        transfer_type=TransferType.UPLOAD,
        file_name=file_name
    )
