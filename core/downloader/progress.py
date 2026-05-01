"""
core/downloader/progress.py - Async-safe progress tracking for Pyrogram transfers.

Features:
- Thread-safe callback scheduling via call_soon_threadsafe
- Throttled updates (5s minimum interval)
- FloodWait handling
- Speed and ETA calculation
- Clean cancellation
"""

import asyncio
import time
import logging
from typing import Optional, Dict, Callable, Any
from dataclasses import dataclass, field

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified, MessageIdInvalid, MessageEditTimeExpired

logger = logging.getLogger(__name__)

# Constants
MIN_UPDATE_INTERVAL = 5.0  # Safer for Telegram edit limits on long Kaggle uploads
SPEED_SAMPLES = 10
PROGRESS_BAR_LENGTH = 12
PROGRESS_FILLED = "█"
PROGRESS_EMPTY = "░"


@dataclass
class TransferState:
    """State for a single transfer."""
    transfer_id: str
    transfer_type: str  # "download" or "upload"
    client: Optional[Client] = None
    message: Optional[Message] = None
    
    total_bytes: int = 0
    current_bytes: int = 0
    
    speed_samples: list = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    last_update_time: float = 0.0
    last_text: str = ""
    
    is_active: bool = True
    is_cancelled: bool = False
    error_count: int = 0


class ProgressTracker:
    """
    Thread-safe progress tracker for Pyrogram downloads/uploads.
    
    Usage:
        tracker = ProgressTracker()
        callback = tracker.create_callback(client, status_msg, "download")
        await client.download_media(msg, progress=callback)
        tracker.cleanup(transfer_id)
    """
    
    def __init__(self):
        self._transfers: Dict[str, TransferState] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
    
    def create_callback(
        self,
        client: Client,
        status_message: Message,
        transfer_type: str = "download",
        transfer_id: str = None
    ) -> Callable[[int, int], None]:
        """
        Create a Pyrogram-compatible progress callback.
        
        CRITICAL: Captures the event loop at creation time and uses
        call_soon_threadsafe to schedule updates from any thread.
        """
        if transfer_id is None:
            transfer_id = f"{status_message.chat.id}_{status_message.id}_{time.time()}"
        
        # Capture current event loop
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        
        # Create state
        state = TransferState(
            transfer_id=transfer_id,
            transfer_type=transfer_type,
            client=client,
            message=status_message
        )
        self._transfers[transfer_id] = state
        
        captured_loop = self._loop
        
        def callback(current: int, total: int) -> None:
            """Sync callback called by Pyrogram from potentially different thread."""
            if state.is_cancelled or not state.is_active:
                return
            
            state.current_bytes = current
            state.total_bytes = total
            
            # Add speed sample
            now = time.time()
            state.speed_samples.append((now, current))
            if len(state.speed_samples) > SPEED_SAMPLES:
                state.speed_samples.pop(0)
            
            # Throttle updates
            if now - state.last_update_time < MIN_UPDATE_INTERVAL:
                return
            
            state.last_update_time = now
            
            # Schedule async update on captured loop
            try:
                if captured_loop.is_running():
                    captured_loop.call_soon_threadsafe(
                        lambda: captured_loop.create_task(
                            self._update_message(transfer_id)
                        )
                    )
            except Exception as e:
                logger.debug(f"Progress schedule error: {e}")
        
        return callback
    
    async def _update_message(self, transfer_id: str) -> None:
        """Update the Telegram message with progress."""
        state = self._transfers.get(transfer_id)
        if not state or not state.is_active or state.is_cancelled:
            return
        
        if state.total_bytes <= 0:
            return
        
        # Calculate progress
        percent = (state.current_bytes / state.total_bytes) * 100
        speed = self._calculate_speed(state)
        eta = self._calculate_eta(state, speed)
        
        # Format message
        text = self._format_progress(
            state.transfer_type,
            percent,
            state.current_bytes,
            state.total_bytes,
            speed,
            eta
        )
        
        if text == state.last_text:
            return
        
        # Edit message
        try:
            if state.client and state.message:
                await state.client.edit_message_text(
                    chat_id=state.message.chat.id,
                    message_id=state.message.id,
                    text=text
                )
                state.last_text = text
                state.error_count = 0
        except MessageNotModified:
            pass
        except FloodWait as e:
            wait = getattr(e, 'value', getattr(e, 'x', 5))
            state.last_update_time = time.time() + wait
            logger.debug(f"FloodWait on progress: {wait}s")
        except (MessageIdInvalid, MessageEditTimeExpired):
            # Message deleted or too old to edit - stop trying
            state.is_active = False
            logger.debug(f"Message {transfer_id} no longer editable, stopping progress updates")
        except Exception as e:
            state.error_count += 1
            if state.error_count > 5:
                state.is_active = False
            logger.debug(f"Progress update error: {e}")
    
    def _calculate_speed(self, state: TransferState) -> float:
        """Calculate current speed in bytes/sec."""
        if len(state.speed_samples) < 2:
            return 0.0
        
        oldest = state.speed_samples[0]
        newest = state.speed_samples[-1]
        
        time_diff = newest[0] - oldest[0]
        bytes_diff = newest[1] - oldest[1]
        
        return bytes_diff / time_diff if time_diff > 0 else 0.0
    
    def _calculate_eta(self, state: TransferState, speed: float) -> float:
        """Calculate ETA in seconds."""
        if speed <= 0:
            return 0.0
        remaining = state.total_bytes - state.current_bytes
        return remaining / speed if remaining > 0 else 0.0
    
    def _format_progress(
        self,
        transfer_type: str,
        percent: float,
        current: int,
        total: int,
        speed: float,
        eta: float
    ) -> str:
        """Format progress message with live speed and ETA."""
        # Progress bar with percentage
        filled = int(PROGRESS_BAR_LENGTH * percent / 100)
        bar = PROGRESS_FILLED * filled + PROGRESS_EMPTY * (PROGRESS_BAR_LENGTH - filled)
        
        # Speed string with smart units
        speed_str = self._format_speed(speed)
        
        # ETA string
        eta_str = self._format_eta(eta)
        
        # Size strings with smart units
        current_str = self._format_size(current)
        total_str = self._format_size(total)
        
        # Icons for visual clarity
        icon = "📥" if transfer_type == "download" else "📤"
        action = "Downloading" if transfer_type == "download" else "Uploading"
        
        return (
            f"{icon} **{action}**\n\n"
            f"`{bar}` **{percent:.1f}%**\n\n"
            f"📊 {current_str} / {total_str}\n"
            f"⚡ {speed_str}\n"
            f"⏱ ETA: {eta_str}"
        )
    
    def _format_speed(self, speed: float) -> str:
        """Format speed with appropriate units."""
        if speed <= 0:
            return "calculating..."
        elif speed < 1024:
            return f"{speed:.0f} B/s"
        elif speed < 1024 * 1024:
            return f"{speed / 1024:.1f} KB/s"
        else:
            return f"{speed / 1024 / 1024:.2f} MB/s"
    
    def _format_eta(self, eta: float) -> str:
        """Format ETA in human-readable format."""
        if eta <= 0:
            return "calculating..."
        elif eta < 60:
            return f"{int(eta)}s"
        elif eta < 3600:
            mins, secs = divmod(int(eta), 60)
            return f"{mins}m {secs}s"
        else:
            hours, remainder = divmod(int(eta), 3600)
            mins = remainder // 60
            return f"{hours}h {mins}m"
    
    def _format_size(self, size: int) -> str:
        """Format size with appropriate units."""
        if size < 1024:
            return f"{size} B"
        elif size < 1024 * 1024:
            return f"{size / 1024:.1f} KB"
        elif size < 1024 * 1024 * 1024:
            return f"{size / 1024 / 1024:.1f} MB"
        else:
            return f"{size / 1024 / 1024 / 1024:.2f} GB"
    
    def cancel(self, transfer_id: str) -> None:
        """Cancel a transfer."""
        if transfer_id in self._transfers:
            self._transfers[transfer_id].is_cancelled = True
            self._transfers[transfer_id].is_active = False
    
    def cleanup(self, transfer_id: str) -> None:
        """Cleanup transfer state."""
        self._transfers.pop(transfer_id, None)
    
    def cleanup_all(self) -> None:
        """Cleanup all transfers."""
        for state in self._transfers.values():
            state.is_cancelled = True
            state.is_active = False
        self._transfers.clear()


# Global instance
progress_tracker = ProgressTracker()
