"""
Loop-Safe Progress Handler for Telegram Media Transfers

KEY FIXES:
- Lock is recreated if event loop changes (prevents "Future attached to different loop")
- Sync callback schedules async work via create_task (Pyrogram-compatible)
- In-memory state only - no files, no database
- Throttled updates (1.5s minimum) to avoid FloodWait

Author: Production-Grade Fix
Python: 3.10+
"""

import asyncio
import time
import logging
from typing import Optional, Dict, Callable
from dataclasses import dataclass, field

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified, MessageIdInvalid, RPCError

logger = logging.getLogger(__name__)

# Constants
PROGRESS_THROTTLE = 1.5  # Minimum seconds between updates
MAX_CONSECUTIVE_ERRORS = 5
SPEED_SAMPLES = 5
PROGRESS_BAR_LENGTH = 12


@dataclass
class ProgressState:
    """Per-transfer state - lives only for transfer duration"""
    transfer_id: str
    client: Client
    message: Message
    is_upload: bool
    filename: Optional[str] = None
    
    # Progress tracking
    current: int = 0
    total: int = 0
    start_time: float = field(default_factory=time.time)
    last_update: float = 0.0
    last_text: str = ""
    errors: int = 0
    active: bool = True
    
    # Speed calculation (rolling average)
    _speed_samples: list = field(default_factory=list)
    _last_bytes: int = 0
    _last_time: float = 0.0
    
    def get_percentage(self) -> float:
        if self.total <= 0:
            return 0.0
        return (self.current / self.total) * 100


class ProgressHandler:
    """
    Loop-safe progress handler for Pyrogram media transfers.
    
    CRITICAL FIX: Lock is recreated when event loop changes.
    This prevents "Future attached to a different loop" errors
    that occur after bot restarts or event loop recreation.
    
    Usage:
        handler = ProgressHandler()
        
        callback = handler.create_callback(
            transfer_id="unique_id",
            client=bot,
            message=status_msg,
            is_upload=False
        )
        
        await client.download_media(msg, progress=callback)
        await handler.finalize(transfer_id, success=True)
        handler.cleanup(transfer_id)
    """
    
    def __init__(self):
        self._states: Dict[str, ProgressState] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._loop_id: Optional[int] = None
        self._pending_tasks: Dict[str, asyncio.Task] = {}
    
    def _get_lock(self) -> asyncio.Lock:
        """
        Get lock bound to current event loop.
        
        THIS IS THE KEY FIX: Creates a new lock if the event loop has changed.
        Prevents "Future attached to a different loop" errors.
        """
        try:
            current_loop = asyncio.get_running_loop()
            current_id = id(current_loop)
        except RuntimeError:
            # No running loop - return a fresh lock
            return asyncio.Lock()
        
        if self._loop_id != current_id:
            # Loop changed (bot restart, etc.) - create new lock
            self._lock = asyncio.Lock()
            self._loop_id = current_id
            logger.debug(f"Created new progress lock for loop {current_id}")
        
        return self._lock
    
    def create_callback(
        self,
        transfer_id: str,
        client: Client,
        message: Message,
        is_upload: bool = False,
        filename: Optional[str] = None
    ) -> Callable[[int, int], None]:
        """
        Create a Pyrogram-compatible progress callback.
        
        IMPORTANT: Returns a SYNC function because Pyrogram progress
        callbacks must be synchronous. Async work is scheduled via
        loop.create_task().
        
        Args:
            transfer_id: Unique identifier for this transfer
            client: Pyrogram client for editing messages
            message: Status message to update
            is_upload: True for upload, False for download
            filename: Optional filename to display
        
        Returns:
            Sync callback with signature (current, total) -> None
        """
        state = ProgressState(
            transfer_id=transfer_id,
            client=client,
            message=message,
            is_upload=is_upload,
            filename=filename
        )
        self._states[transfer_id] = state
        
        def sync_callback(current: int, total: int) -> None:
            """
            SYNC progress callback - Pyrogram calls this during transfer.
            
            DO NOT AWAIT HERE. Only update state and schedule async work.
            """
            if not state.active:
                return
            
            # Update state (fast, sync operation)
            state.current = current
            state.total = total
            
            # Throttle check
            now = time.time()
            if now - state.last_update < PROGRESS_THROTTLE:
                return
            
            # Too many errors - stop updating
            if state.errors >= MAX_CONSECUTIVE_ERRORS:
                state.active = False
                return
            
            state.last_update = now
            
            # Schedule async message edit on the running loop
            try:
                loop = asyncio.get_running_loop()
                
                # Cancel previous pending update for this transfer
                if transfer_id in self._pending_tasks:
                    old_task = self._pending_tasks.pop(transfer_id)
                    if not old_task.done():
                        old_task.cancel()
                
                # Schedule new update
                task = loop.create_task(
                    self._do_update(transfer_id),
                    name=f"progress_{transfer_id}"
                )
                self._pending_tasks[transfer_id] = task
                
            except RuntimeError:
                # No running loop - skip update silently
                pass
        
        return sync_callback
    
    async def _do_update(self, transfer_id: str) -> None:
        """
        Perform the actual message edit (async).
        Runs on the main event loop.
        """
        lock = self._get_lock()
        
        async with lock:
            state = self._states.get(transfer_id)
            if not state or not state.active:
                return
            
            # Calculate speed
            speed = self._calc_speed(state)
            
            # Format message
            text = self._format_message(state, speed)
            
            # Skip if unchanged
            if text == state.last_text:
                return
            
            try:
                await state.client.edit_message_text(
                    chat_id=state.message.chat.id,
                    message_id=state.message.id,
                    text=text
                )
                state.last_text = text
                state.errors = 0
                
            except MessageNotModified:
                # Content unchanged - not an error
                state.last_text = text
                
            except FloodWait as e:
                wait = getattr(e, 'value', getattr(e, 'x', 5))
                logger.warning(f"FloodWait({wait}s) on progress update")
                state.last_update = time.time() + wait
                state.errors += 1
                
            except MessageIdInvalid:
                # Message was deleted - stop updating
                logger.info(f"Progress message deleted for {transfer_id}")
                state.active = False
                
            except RPCError as e:
                state.errors += 1
                if state.errors >= MAX_CONSECUTIVE_ERRORS:
                    logger.warning(f"Too many progress errors for {transfer_id}")
                    state.active = False
                else:
                    logger.debug(f"Progress RPC error: {e}")
                    
            except Exception as e:
                state.errors += 1
                logger.debug(f"Progress update error: {e}")
        
        # Remove from pending
        self._pending_tasks.pop(transfer_id, None)
    
    def _calc_speed(self, state: ProgressState) -> float:
        """Calculate smoothed speed in bytes/sec using rolling average."""
        now = time.time()
        
        if state._last_time > 0:
            elapsed = now - state._last_time
            if elapsed > 0:
                bytes_diff = state.current - state._last_bytes
                instant_speed = bytes_diff / elapsed
                
                state._speed_samples.append(instant_speed)
                if len(state._speed_samples) > SPEED_SAMPLES:
                    state._speed_samples.pop(0)
        
        state._last_bytes = state.current
        state._last_time = now
        
        if state._speed_samples:
            return sum(state._speed_samples) / len(state._speed_samples)
        return 0.0
    
    def _format_message(self, state: ProgressState, speed: float) -> str:
        """Format progress message with bar, percentage, speed, ETA."""
        if state.total <= 0:
            return "📊 Processing..."
        
        pct = state.get_percentage()
        filled = int(pct / 100 * PROGRESS_BAR_LENGTH)
        bar = "█" * filled + "░" * (PROGRESS_BAR_LENGTH - filled)
        
        # Size formatting
        def fmt_size(b: int) -> str:
            if b < 1024:
                return f"{b} B"
            if b < 1024 ** 2:
                return f"{b / 1024:.1f} KB"
            if b < 1024 ** 3:
                return f"{b / 1024 ** 2:.1f} MB"
            return f"{b / 1024 ** 3:.2f} GB"
        
        def fmt_speed(s: float) -> str:
            if s < 1024:
                return f"{s:.0f} B/s"
            if s < 1024 ** 2:
                return f"{s / 1024:.1f} KB/s"
            return f"{s / 1024 ** 2:.1f} MB/s"
        
        # ETA calculation
        if speed > 0:
            eta_sec = (state.total - state.current) / speed
            if eta_sec < 60:
                eta = f"{int(eta_sec)}s"
            elif eta_sec < 3600:
                eta = f"{int(eta_sec / 60)}m {int(eta_sec % 60)}s"
            else:
                eta = f"{int(eta_sec / 3600)}h {int((eta_sec % 3600) / 60)}m"
        else:
            eta = "calculating..."
        
        icon = "⬆️" if state.is_upload else "⬇️"
        action = "Uploading" if state.is_upload else "Downloading"
        
        lines = [
            f"{icon} **{action}**",
            "",
            f"`{bar}` {pct:.1f}%",
            "",
            f"📦 {fmt_size(state.current)} / {fmt_size(state.total)}",
            f"⚡ {fmt_speed(speed)}",
            f"⏱ ETA: {eta}",
        ]
        
        if state.filename:
            name = state.filename
            if len(name) > 30:
                name = name[:27] + "..."
            lines.insert(1, f"📄 `{name}`")
        
        return "\n".join(lines)
    
    async def finalize(
        self,
        transfer_id: str,
        success: bool = True,
        error: Optional[str] = None
    ) -> None:
        """
        Finalize transfer and show completion message.
        
        Args:
            transfer_id: The transfer to finalize
            success: Whether transfer completed successfully
            error: Optional error message
        """
        lock = self._get_lock()
        
        async with lock:
            state = self._states.get(transfer_id)
            if not state:
                return
            
            state.active = False
            elapsed = time.time() - state.start_time
            
            def fmt_size(b: int) -> str:
                if b < 1024 ** 2:
                    return f"{b / 1024:.1f} KB"
                if b < 1024 ** 3:
                    return f"{b / 1024 ** 2:.1f} MB"
                return f"{b / 1024 ** 3:.2f} GB"
            
            if success:
                avg_speed = state.total / elapsed if elapsed > 0 else 0
                text = (
                    f"✅ **{'Upload' if state.is_upload else 'Download'} Complete**\n\n"
                    f"📦 Size: {fmt_size(state.total)}\n"
                    f"⏱ Time: {elapsed:.1f}s\n"
                    f"⚡ Avg Speed: {avg_speed / 1024 ** 2:.1f} MB/s"
                )
            else:
                text = f"❌ **{'Upload' if state.is_upload else 'Download'} Failed**\n\n{error or 'Unknown error'}"
            
            try:
                await state.client.edit_message_text(
                    chat_id=state.message.chat.id,
                    message_id=state.message.id,
                    text=text
                )
            except Exception as e:
                logger.debug(f"Could not update completion message: {e}")
    
    def cleanup(self, transfer_id: str) -> None:
        """Remove transfer state from memory."""
        # Cancel pending task
        if transfer_id in self._pending_tasks:
            task = self._pending_tasks.pop(transfer_id)
            if not task.done():
                task.cancel()
        
        self._states.pop(transfer_id, None)
    
    def cleanup_all(self) -> int:
        """Clean all states (for shutdown). Returns count cleaned."""
        # Cancel all pending tasks
        for task in self._pending_tasks.values():
            if not task.done():
                task.cancel()
        self._pending_tasks.clear()
        
        count = len(self._states)
        self._states.clear()
        return count
    
    def cancel(self, transfer_id: str) -> bool:
        """Mark a transfer as cancelled. Returns True if found."""
        state = self._states.get(transfer_id)
        if state:
            state.active = False
            return True
        return False
    
    def get_active_count(self) -> int:
        """Get count of active transfers."""
        return sum(1 for s in self._states.values() if s.active)


# Global singleton instance
progress_handler = ProgressHandler()


# Convenience functions
def create_download_progress(
    transfer_id: str,
    client: Client,
    message: Message,
    filename: Optional[str] = None
) -> Callable[[int, int], None]:
    """Create download progress callback."""
    return progress_handler.create_callback(
        transfer_id=transfer_id,
        client=client,
        message=message,
        is_upload=False,
        filename=filename
    )


def create_upload_progress(
    transfer_id: str,
    client: Client,
    message: Message,
    filename: Optional[str] = None
) -> Callable[[int, int], None]:
    """Create upload progress callback."""
    return progress_handler.create_callback(
        transfer_id=transfer_id,
        client=client,
        message=message,
        is_upload=True,
        filename=filename
    )
