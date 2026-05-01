"""
core/downloader/progress_v2.py - Production-grade progress tracking with architectural separation.

Architecture:
- ProgressState: Thread-safe state container (workers write, publisher reads)
- ProgressPublisher: Dedicated async task for Telegram API calls
- Workers NEVER call Telegram API directly

Features:
- Thread-safe progress updates via threading.Lock
- Dedicated UI publisher task (no API calls from workers)
- 2-second update interval (safe for Telegram limits)
- FloodWait handling with exponential backoff
- Reconnect-safe (no duplicate tasks)
- Speed and ETA calculation
"""

import asyncio
import threading
import time
import logging
from typing import Optional, Dict, Callable, Any, Set
from dataclasses import dataclass, field
from enum import Enum

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified, MessageIdInvalid, MessageEditTimeExpired

logger = logging.getLogger(__name__)

# Constants
PROGRESS_UPDATE_INTERVAL = 2.0  # Safe interval for Telegram
SPEED_SAMPLE_COUNT = 10
PROGRESS_BAR_LENGTH = 12
PROGRESS_FILLED = "█"
PROGRESS_EMPTY = "░"
MAX_CONSECUTIVE_ERRORS = 5
FLOODWAIT_BACKOFF_MULTIPLIER = 1.5


class TransferStatus(Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProgressState:
    """
    Thread-safe progress state container.
    
    Workers update this state via update() method.
    Publisher reads this state to update Telegram UI.
    
    Thread safety: All mutations protected by _lock.
    """
    transfer_id: str
    transfer_type: str  # "download" or "upload"
    
    # Progress data (updated by workers)
    total_bytes: int = 0
    current_bytes: int = 0
    status: TransferStatus = TransferStatus.PENDING
    error_message: Optional[str] = None
    
    # Speed calculation
    speed_samples: list = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    
    # UI state (managed by publisher)
    last_ui_update: float = 0.0
    last_ui_text: str = ""
    ui_error_count: int = 0
    
    # Thread safety
    _lock: threading.Lock = field(default_factory=threading.Lock)
    
    def update(self, current: int, total: int) -> None:
        """Thread-safe progress update (called by workers)."""
        with self._lock:
            self.current_bytes = current
            self.total_bytes = total
            
            # Add speed sample
            now = time.time()
            self.speed_samples.append((now, current))
            if len(self.speed_samples) > SPEED_SAMPLE_COUNT:
                self.speed_samples.pop(0)
            
            if self.status == TransferStatus.PENDING:
                self.status = TransferStatus.DOWNLOADING if self.transfer_type == "download" else TransferStatus.UPLOADING
    
    def complete(self) -> None:
        """Mark transfer as completed."""
        with self._lock:
            self.status = TransferStatus.COMPLETED
    
    def fail(self, error: str) -> None:
        """Mark transfer as failed."""
        with self._lock:
            self.status = TransferStatus.FAILED
            self.error_message = error
    
    def cancel(self) -> None:
        """Mark transfer as cancelled."""
        with self._lock:
            self.status = TransferStatus.CANCELLED
    
    @property
    def is_active(self) -> bool:
        """Check if transfer is still active."""
        with self._lock:
            return self.status in (TransferStatus.PENDING, TransferStatus.DOWNLOADING, TransferStatus.UPLOADING)
    
    @property
    def percent(self) -> float:
        """Calculate progress percentage."""
        with self._lock:
            if self.total_bytes <= 0:
                return 0.0
            return (self.current_bytes / self.total_bytes) * 100
    
    @property
    def speed(self) -> float:
        """Calculate current speed in bytes/sec."""
        with self._lock:
            if len(self.speed_samples) < 2:
                return 0.0
            oldest = self.speed_samples[0]
            newest = self.speed_samples[-1]
            time_diff = newest[0] - oldest[0]
            bytes_diff = newest[1] - oldest[1]
            return bytes_diff / time_diff if time_diff > 0 else 0.0
    
    @property
    def eta_seconds(self) -> float:
        """Calculate ETA in seconds."""
        spd = self.speed
        if spd <= 0:
            return 0.0
        with self._lock:
            remaining = self.total_bytes - self.current_bytes
        return remaining / spd if remaining > 0 else 0.0
    
    def get_snapshot(self) -> dict:
        """Get thread-safe snapshot of current state."""
        with self._lock:
            return {
                'transfer_id': self.transfer_id,
                'transfer_type': self.transfer_type,
                'current_bytes': self.current_bytes,
                'total_bytes': self.total_bytes,
                'status': self.status,
                'error_message': self.error_message,
                'percent': (self.current_bytes / self.total_bytes * 100) if self.total_bytes > 0 else 0,
                'speed_samples': list(self.speed_samples),
            }


class ProgressPublisher:
    """
    Dedicated async task for publishing progress to Telegram.
    
    CRITICAL: This is the ONLY component that calls Telegram API for progress.
    Workers NEVER call Telegram API directly.
    
    Features:
    - Single publisher task per transfer (no duplicates)
    - 2-second update interval
    - FloodWait handling with backoff
    - Reconnect-safe
    - Clean shutdown
    """
    
    def __init__(
        self,
        client: Client,
        chat_id: int,
        message_id: int,
        state: ProgressState
    ):
        self.client = client
        self.chat_id = chat_id
        self.message_id = message_id
        self.state = state
        
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._floodwait_until = 0.0
    
    def start(self) -> None:
        """Start the publisher task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._publish_loop())
        logger.debug(f"ProgressPublisher started for {self.state.transfer_id}")
    
    def stop(self) -> None:
        """Stop the publisher task."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        logger.debug(f"ProgressPublisher stopped for {self.state.transfer_id}")
    
    async def _publish_loop(self) -> None:
        """Main publish loop - runs every 2 seconds."""
        while self._running and self.state.is_active:
            try:
                # Respect FloodWait
                now = time.time()
                if now < self._floodwait_until:
                    await asyncio.sleep(self._floodwait_until - now)
                    continue
                
                # Update UI
                await self._update_ui()
                
                # Wait for next interval
                await asyncio.sleep(PROGRESS_UPDATE_INTERVAL)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Publisher error: {e}")
                await asyncio.sleep(PROGRESS_UPDATE_INTERVAL)
        
        # Final update on completion
        if not self.state.is_active:
            await self._update_ui_final()
    
    async def _update_ui(self) -> None:
        """Update Telegram message with current progress."""
        snapshot = self.state.get_snapshot()
        
        # Format message
        text = self._format_progress(snapshot)
        
        # Skip if unchanged
        if text == self.state.last_ui_text:
            return
        
        try:
            await self.client.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=text
            )
            self.state.last_ui_text = text
            self.state.last_ui_update = time.time()
            self.state.ui_error_count = 0
            
        except MessageNotModified:
            pass
        except FloodWait as e:
            wait = getattr(e, 'value', getattr(e, 'x', 5))
            self._floodwait_until = time.time() + wait * FLOODWAIT_BACKOFF_MULTIPLIER
            logger.debug(f"FloodWait: {wait}s, backing off until {self._floodwait_until}")
        except (MessageIdInvalid, MessageEditTimeExpired):
            # Message deleted or too old to edit (48h limit)
            logger.debug(f"Message {self.message_id} no longer editable, stopping publisher")
            self._running = False
        except Exception as e:
            self.state.ui_error_count += 1
            if self.state.ui_error_count >= MAX_CONSECUTIVE_ERRORS:
                logger.warning(f"Too many UI errors, stopping publisher: {e}")
                self._running = False
    
    async def _update_ui_final(self) -> None:
        """Update UI with final status."""
        status = self.state.status
        
        if status == TransferStatus.COMPLETED:
            text = "✅ **Download completed**"
        elif status == TransferStatus.FAILED:
            text = f"❌ **Download failed**\n{self.state.error_message or 'Unknown error'}"
        elif status == TransferStatus.CANCELLED:
            text = "⚠️ **Download cancelled**"
        else:
            return
        
        try:
            await self.client.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.message_id,
                text=text
            )
        except Exception as e:
            logger.debug(f"Final UI update error: {e}")
    
    def _format_progress(self, snapshot: dict) -> str:
        """Format progress message."""
        percent = snapshot['percent']
        current = snapshot['current_bytes']
        total = snapshot['total_bytes']
        transfer_type = snapshot['transfer_type']
        
        # Calculate speed from samples
        samples = snapshot['speed_samples']
        if len(samples) >= 2:
            oldest, newest = samples[0], samples[-1]
            time_diff = newest[0] - oldest[0]
            bytes_diff = newest[1] - oldest[1]
            speed = bytes_diff / time_diff if time_diff > 0 else 0.0
        else:
            speed = 0.0
        
        # Progress bar
        filled = int(PROGRESS_BAR_LENGTH * percent / 100)
        bar = PROGRESS_FILLED * filled + PROGRESS_EMPTY * (PROGRESS_BAR_LENGTH - filled)
        
        # Speed string
        if speed < 1024:
            speed_str = f"{speed:.0f} B/s"
        elif speed < 1024 * 1024:
            speed_str = f"{speed / 1024:.1f} KB/s"
        else:
            speed_str = f"{speed / 1024 / 1024:.1f} MB/s"
        
        # ETA
        if speed > 0:
            remaining = total - current
            eta_sec = remaining / speed
            if eta_sec < 60:
                eta_str = f"{int(eta_sec)}s"
            elif eta_sec < 3600:
                eta_str = f"{int(eta_sec // 60)}m {int(eta_sec % 60)}s"
            else:
                eta_str = f"{int(eta_sec // 3600)}h {int((eta_sec % 3600) // 60)}m"
        else:
            eta_str = "..."
        
        # Size
        current_mb = current / 1024 / 1024
        total_mb = total / 1024 / 1024
        
        action = "Downloading" if transfer_type == "download" else "Uploading"
        
        return (
            f"**{action}**\n"
            f"{bar} {percent:.1f}%\n"
            f"{current_mb:.1f} / {total_mb:.1f} MB\n"
            f"Speed: {speed_str} | ETA: {eta_str}"
        )


class ProgressManager:
    """
    Centralized progress management with architectural separation.
    
    Usage:
        manager = ProgressManager()
        
        # Create progress (returns callback for workers)
        callback, state = await manager.create_progress(
            client, chat_id, message_id, "download"
        )
        
        # Workers use callback to update progress
        await client.download_media(msg, progress=callback)
        
        # Mark complete when done
        state.complete()
        
        # Cleanup
        manager.cleanup(state.transfer_id)
    """
    
    def __init__(self):
        self._states: Dict[str, ProgressState] = {}
        self._publishers: Dict[str, ProgressPublisher] = {}
        self._lock = threading.Lock()
    
    async def create_progress(
        self,
        client: Client,
        chat_id: int,
        message_id: int,
        transfer_type: str = "download",
        transfer_id: str = None
    ) -> tuple:
        """
        Create a new progress tracker.
        
        Returns:
            (callback, state) - callback for workers, state for completion tracking
        """
        if transfer_id is None:
            transfer_id = f"{chat_id}_{message_id}_{time.time()}"
        
        # Create state
        state = ProgressState(
            transfer_id=transfer_id,
            transfer_type=transfer_type
        )
        
        with self._lock:
            # Check for existing (reconnect scenario)
            if transfer_id in self._states:
                # Reuse existing state
                state = self._states[transfer_id]
                logger.debug(f"Reusing existing progress state: {transfer_id}")
            else:
                self._states[transfer_id] = state
        
        # Create publisher (only if not exists)
        if transfer_id not in self._publishers:
            publisher = ProgressPublisher(client, chat_id, message_id, state)
            self._publishers[transfer_id] = publisher
            publisher.start()
        
        # Create callback for workers
        def callback(current: int, total: int) -> None:
            state.update(current, total)
        
        return callback, state
    
    def cleanup(self, transfer_id: str) -> None:
        """Cleanup progress state and publisher."""
        with self._lock:
            # Stop publisher
            if transfer_id in self._publishers:
                self._publishers[transfer_id].stop()
                del self._publishers[transfer_id]
            
            # Remove state
            self._states.pop(transfer_id, None)
    
    def cleanup_all(self) -> None:
        """Cleanup all progress trackers."""
        with self._lock:
            for publisher in self._publishers.values():
                publisher.stop()
            self._publishers.clear()
            self._states.clear()
    
    def get_state(self, transfer_id: str) -> Optional[ProgressState]:
        """Get progress state by ID."""
        with self._lock:
            return self._states.get(transfer_id)
    
    def get_active_count(self) -> int:
        """Get count of active transfers."""
        with self._lock:
            return sum(1 for s in self._states.values() if s.is_active)


# Global instance
progress_manager = ProgressManager()
