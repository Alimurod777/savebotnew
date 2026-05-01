"""
Async-Safe Progress Controller for Telegram Media Transfers

Provides throttled, in-memory progress tracking that runs entirely on
the main asyncio event loop. Designed to avoid:
- File-based polling (old system)
- Event loop conflicts
- FloodWait errors from too-frequent edits

Design Decisions:
- All state is in-memory (no files, no database)
- Minimum 1.5s between progress edits (Telegram rate limit protection)
- Lock is created lazily in current event loop (avoids cross-loop issues)
- FloodWait and edit failures are caught, not raised

Author: WOODcraft Refactored
"""

import asyncio
import time
import logging
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass, field
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified, MessageIdInvalid

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# Minimum seconds between progress updates (Telegram rate limit protection)
MIN_UPDATE_INTERVAL = 1.5

# Maximum retries for message edits after FloodWait
MAX_EDIT_RETRIES = 2

# Progress bar configuration
PROGRESS_BAR_LENGTH = 10
PROGRESS_BAR_FILLED = "█"
PROGRESS_BAR_EMPTY = "░"

# ── Progress filtering (FloodWait prevention) ───────────────────
# Only show progress for files >= 100MB
FILE_SIZE_PROGRESS_THRESHOLD = 100 * 1024 * 1024  # 100MB

# Milestone-based updates — edit only at these percentages
PROGRESS_MILESTONES = [10, 25, 50, 75, 100]


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class TransferState:
    """
    State for a single transfer (download or upload).
    
    All timing and progress data stored here - no external files.
    """
    transfer_id: str
    transfer_type: str  # "download" or "upload"
    status_message: Optional[Message] = None
    client: Optional[Client] = None
    
    # Progress tracking
    total_bytes: int = 0
    current_bytes: int = 0
    
    # Timing
    start_time: float = field(default_factory=time.time)
    last_update_time: float = 0.0
    last_edit_text: str = ""
    
    # Error tracking
    consecutive_errors: int = 0
    is_active: bool = True

    # Milestone tracking (FloodWait prevention)
    last_reported_milestone: float = 0.0


# ============================================================================
# PROGRESS CONTROLLER
# ============================================================================

class ProgressController:
    """
    Async-safe, in-memory progress controller for Telegram media transfers.
    
    Features:
    - Throttled updates (minimum 1.5s between edits)
    - FloodWait handling with exponential backoff
    - Clean cancellation support
    - Event-loop-safe lock management
    
    Usage:
        controller = ProgressController()
        
        # Create a progress callback for Pyrogram
        callback = controller.create_callback(
            transfer_id="unique_id",
            client=bot_client,
            status_message=status_msg,
            transfer_type="download"
        )
        
        # Use with Pyrogram
        await client.download_media(message, progress=callback)
        
        # Cleanup
        controller.cleanup(transfer_id)
    """
    
    def __init__(self):
        """Initialize the progress controller."""
        self._transfers: Dict[str, TransferState] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._lock_loop: Optional[asyncio.AbstractEventLoop] = None
    
    def _get_lock(self) -> asyncio.Lock:
        """
        Get or create lock bound to current event loop.
        
        This pattern prevents "Future attached to a different loop" errors
        by recreating the lock if the event loop changes.
        """
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        
        # Create new lock if needed
        if self._lock is None or self._lock_loop is not current_loop:
            self._lock = asyncio.Lock()
            self._lock_loop = current_loop
        
        return self._lock
    
    def create_callback(
        self,
        transfer_id: str,
        client: Client,
        status_message: Message,
        transfer_type: str = "download"
    ) -> Callable[[int, int], None]:
        """
        Create a Pyrogram-compatible progress callback.
        
        The callback is a sync function that schedules async updates.
        This matches Pyrogram's expected signature for progress callbacks.
        
        Args:
            transfer_id: Unique identifier for this transfer
            client: Pyrogram client for editing messages
            status_message: Message to update with progress
            transfer_type: "download" or "upload"
        
        Returns:
            Callback function with signature (current, total) -> None
        """
        # Initialize transfer state
        state = TransferState(
            transfer_id=transfer_id,
            transfer_type=transfer_type,
            status_message=status_message,
            client=client,
            start_time=time.time()
        )
        self._transfers[transfer_id] = state
        
        # Capture the event loop at callback creation time (in async context)
        try:
            captured_loop = asyncio.get_running_loop()
        except RuntimeError:
            captured_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(captured_loop)
        
        def progress_callback(current: int, total: int) -> None:
            """
            Pyrogram progress callback (sync).

            This is called by Pyrogram during download/upload.
            We schedule the async update on the captured event loop.

            CRITICAL: Pyrogram may call this from a different thread,
            so we use call_soon_threadsafe to schedule the coroutine.

            FloodWait prevention:
            - Files < 100MB: no progress shown at all
            - Files >= 100MB: milestone-based (10/25/50/75/100%)
            """
            # Update bytes in state
            state.current_bytes = current
            state.total_bytes = total

            # Skip progress for small files (< 100MB)
            if total < FILE_SIZE_PROGRESS_THRESHOLD:
                return

            # Milestone-based filtering
            if total > 0:
                current_pct = (current / total) * 100
                hit_milestone = False
                for milestone in PROGRESS_MILESTONES:
                    if state.last_reported_milestone < milestone <= current_pct:
                        hit_milestone = True
                        break
                if not hit_milestone:
                    return
                state.last_reported_milestone = current_pct

            # Check if we should update (time throttling)
            now = time.time()
            if now - state.last_update_time < MIN_UPDATE_INTERVAL:
                return

            # Mark that we're attempting an update
            state.last_update_time = now
            
            # Schedule async update - use call_soon_threadsafe for thread safety
            try:
                if captured_loop.is_running():
                    # Thread-safe way to schedule coroutine from any thread
                    captured_loop.call_soon_threadsafe(
                        lambda: captured_loop.create_task(
                            self._update_progress_message(transfer_id)
                        )
                    )
            except Exception as e:
                logger.debug(f"Progress schedule failed: {e}")
        
        return progress_callback
    
    async def _update_progress_message(self, transfer_id: str) -> None:
        """
        Update the status message with current progress.
        
        This is the async function that actually edits the Telegram message.
        Handles throttling, FloodWait, and edit failures gracefully.
        """
        async with self._get_lock():
            state = self._transfers.get(transfer_id)
            if not state or not state.is_active:
                return
            
            # Double-check throttling (in case multiple updates were scheduled)
            now = time.time()
            if now - state.last_update_time < MIN_UPDATE_INTERVAL:
                return
            
            # Calculate progress
            if state.total_bytes <= 0:
                return
            
            percentage = (state.current_bytes / state.total_bytes) * 100
            elapsed = now - state.start_time
            
            # Calculate speed and ETA
            if elapsed > 0:
                speed = state.current_bytes / elapsed
                remaining_bytes = state.total_bytes - state.current_bytes
                if speed > 0:
                    eta_seconds = remaining_bytes / speed
                else:
                    eta_seconds = 0
            else:
                speed = 0
                eta_seconds = 0
            
            # Format the progress message
            text = self._format_progress_text(
                transfer_type=state.transfer_type,
                percentage=percentage,
                current_bytes=state.current_bytes,
                total_bytes=state.total_bytes,
                speed=speed,
                eta_seconds=eta_seconds
            )
            
            # Skip if text hasn't changed
            if text == state.last_edit_text:
                return
            
            # Try to edit the message
            try:
                if state.status_message and state.client:
                    await state.client.edit_message_text(
                        chat_id=state.status_message.chat.id,
                        message_id=state.status_message.id,
                        text=text
                    )
                    state.last_update_time = now
                    state.last_edit_text = text
                    state.consecutive_errors = 0
                    
            except MessageNotModified:
                # Message content is the same - not an error
                state.last_update_time = now
                
            except FloodWait as e:
                # Rate limited - wait and don't retry immediately
                wait_time = getattr(e, 'value', getattr(e, 'x', 5))
                logger.warning(f"FloodWait({wait_time}s) on progress update")
                state.last_update_time = now + wait_time
                state.consecutive_errors += 1
                
            except MessageIdInvalid:
                # Message was deleted - stop updating
                logger.warning(f"Status message deleted for transfer {transfer_id}")
                state.is_active = False
                
            except Exception as e:
                # Other errors - log and continue
                state.consecutive_errors += 1
                if state.consecutive_errors <= MAX_EDIT_RETRIES:
                    logger.debug(f"Progress update error: {e}")
                else:
                    logger.warning(f"Multiple progress errors for {transfer_id}: {e}")
    
    def _format_progress_text(
        self,
        transfer_type: str,
        percentage: float,
        current_bytes: int,
        total_bytes: int,
        speed: float,
        eta_seconds: float
    ) -> str:
        """
        Format the progress message text.
        
        Returns a nicely formatted string with:
        - Progress bar
        - Percentage
        - Speed in MB/s
        - ETA
        """
        # Progress bar
        filled = int(percentage / 100 * PROGRESS_BAR_LENGTH)
        bar = PROGRESS_BAR_FILLED * filled + PROGRESS_BAR_EMPTY * (PROGRESS_BAR_LENGTH - filled)
        
        # Size formatting
        current_mb = current_bytes / (1024 * 1024)
        total_mb = total_bytes / (1024 * 1024)
        speed_mbs = speed / (1024 * 1024)
        
        # ETA formatting
        if eta_seconds < 60:
            eta_str = f"{int(eta_seconds)}s"
        elif eta_seconds < 3600:
            eta_str = f"{int(eta_seconds / 60)}m {int(eta_seconds % 60)}s"
        else:
            eta_str = f"{int(eta_seconds / 3600)}h {int((eta_seconds % 3600) / 60)}m"
        
        # Build message
        action = "⬇️ Downloading" if transfer_type == "download" else "⬆️ Uploading"
        
        return (
            f"{action}\n\n"
            f"{bar} {percentage:.1f}%\n\n"
            f"📦 {current_mb:.1f} / {total_mb:.1f} MB\n"
            f"⚡ {speed_mbs:.1f} MB/s\n"
            f"⏱ ETA: {eta_str}"
        )
    
    async def finalize_progress(
        self,
        transfer_id: str,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> None:
        """
        Finalize a transfer and update the status message.
        
        Args:
            transfer_id: The transfer to finalize
            success: Whether the transfer completed successfully
            error_message: Optional error message to display
        """
        async with self._get_lock():
            state = self._transfers.get(transfer_id)
            if not state:
                return
            
            state.is_active = False
            
            # Calculate final stats
            elapsed = time.time() - state.start_time
            total_mb = state.total_bytes / (1024 * 1024) if state.total_bytes > 0 else 0
            avg_speed = (state.total_bytes / elapsed / (1024 * 1024)) if elapsed > 0 else 0
            
            # Format completion message
            if success:
                text = (
                    f"✅ {'Download' if state.transfer_type == 'download' else 'Upload'} Complete\n\n"
                    f"📦 Size: {total_mb:.1f} MB\n"
                    f"⏱ Time: {elapsed:.1f}s\n"
                    f"⚡ Avg Speed: {avg_speed:.1f} MB/s"
                )
            else:
                text = (
                    f"❌ {'Download' if state.transfer_type == 'download' else 'Upload'} Failed\n\n"
                    f"{error_message or 'Unknown error'}"
                )
            
            # Update message
            try:
                if state.status_message and state.client:
                    await state.client.edit_message_text(
                        chat_id=state.status_message.chat.id,
                        message_id=state.status_message.id,
                        text=text
                    )
            except Exception as e:
                logger.debug(f"Could not update final status: {e}")
    
    def cleanup(self, transfer_id: str) -> None:
        """
        Remove transfer state from memory.
        
        Call this after the transfer is complete (success or failure).
        """
        self._transfers.pop(transfer_id, None)
    
    def cleanup_all(self) -> int:
        """
        Remove all transfer states.
        
        Returns:
            Number of transfers cleaned up
        """
        count = len(self._transfers)
        self._transfers.clear()
        return count
    
    def get_active_transfers(self) -> List[str]:
        """Get list of active transfer IDs."""
        return [
            tid for tid, state in self._transfers.items()
            if state.is_active
        ]
    
    def cancel_transfer(self, transfer_id: str) -> bool:
        """
        Mark a transfer as cancelled.
        
        Returns:
            True if transfer was found and cancelled
        """
        state = self._transfers.get(transfer_id)
        if state:
            state.is_active = False
            return True
        return False


# ============================================================================
# GLOBAL INSTANCE
# ============================================================================

# Singleton instance for easy import
progress_controller = ProgressController()
