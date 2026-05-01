"""
DEPRECATED: Use TechVJ/progress_controller.py instead.

This module is kept for backwards compatibility only.
The canonical progress system is TechVJ/progress_controller.py.
"""

import warnings
warnings.warn(
    "core.progress_reporter is deprecated. Use TechVJ.progress_controller instead.",
    DeprecationWarning,
    stacklevel=2
)

# Original: Real-Time Progress Reporting System

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified, MessageIdInvalid
import logging

logger = logging.getLogger(__name__)

MIN_UPDATE_INTERVAL = 1.5
SPEED_SAMPLE_WINDOW = 5
PROGRESS_BAR_LENGTH = 12


@dataclass
class TransferProgress:
    transfer_id: str
    transfer_type: str
    file_name: str = ""
    
    total_bytes: int = 0
    current_bytes: int = 0
    
    speed_samples: list = field(default_factory=list)
    
    start_time: float = field(default_factory=time.time)
    last_update_time: float = 0.0
    last_message_text: str = ""
    
    client: Optional[Client] = None
    message: Optional[Message] = None
    
    is_active: bool = True
    is_finalized: bool = False
    
    def calculate_speed(self) -> float:
        if len(self.speed_samples) < 2:
            return 0.0
        
        now = time.time()
        self.speed_samples = [
            (t, b) for t, b in self.speed_samples
            if now - t < SPEED_SAMPLE_WINDOW
        ]
        
        if len(self.speed_samples) < 2:
            return 0.0
        
        oldest = self.speed_samples[0]
        newest = self.speed_samples[-1]
        
        time_diff = newest[0] - oldest[0]
        bytes_diff = newest[1] - oldest[1]
        
        return bytes_diff / time_diff if time_diff > 0 else 0.0
    
    def add_sample(self, bytes_transferred: int) -> None:
        self.speed_samples.append((time.time(), bytes_transferred))
        if len(self.speed_samples) > 20:
            self.speed_samples = self.speed_samples[-20:]


class ProgressReporter:
    def __init__(self):
        self._transfers: Dict[str, TransferProgress] = {}
        self._lock: asyncio.Lock = None
        self._lock_loop_id: int = None
    
    def _get_lock(self) -> asyncio.Lock:
        try:
            current_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return asyncio.Lock()
        
        if self._lock_loop_id != current_id:
            self._lock = asyncio.Lock()
            self._lock_loop_id = current_id
        return self._lock
    
    def create_callback(
        self,
        transfer_id: str,
        client: Client,
        message: Message,
        transfer_type: str,
        file_name: str = ""
    ) -> Callable[[int, int], None]:
        progress = TransferProgress(
            transfer_id=transfer_id,
            transfer_type=transfer_type,
            file_name=file_name,
            client=client,
            message=message,
        )
        self._transfers[transfer_id] = progress
        
        def callback(current: int, total: int) -> None:
            if not progress.is_active:
                return
            
            progress.current_bytes = current
            progress.total_bytes = total
            progress.add_sample(current)
            
            now = time.time()
            if now - progress.last_update_time < MIN_UPDATE_INTERVAL:
                return
            
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._update_message(transfer_id))
            except RuntimeError:
                pass
        
        return callback
    
    async def _update_message(self, transfer_id: str) -> None:
        async with self._get_lock():
            progress = self._transfers.get(transfer_id)
            if not progress or not progress.is_active or progress.is_finalized:
                return
            
            now = time.time()
            if now - progress.last_update_time < MIN_UPDATE_INTERVAL:
                return
            
            text = self._format_progress(progress)
            if text == progress.last_message_text:
                return
            
            try:
                if progress.client and progress.message:
                    await progress.client.edit_message_text(
                        chat_id=progress.message.chat.id,
                        message_id=progress.message.id,
                        text=text
                    )
                    progress.last_update_time = now
                    progress.last_message_text = text
                    
            except MessageNotModified:
                progress.last_update_time = now
            except FloodWait as e:
                wait = getattr(e, 'value', getattr(e, 'x', 5))
                progress.last_update_time = now + wait
            except MessageIdInvalid:
                progress.is_active = False
            except Exception as e:
                logger.debug(f"Progress update error: {e}")
    
    def _format_progress(self, p: TransferProgress) -> str:
        if p.total_bytes <= 0:
            return f"{'⬇️' if p.transfer_type == 'download' else '⬆️'} Processing..."
        
        percentage = (p.current_bytes / p.total_bytes) * 100
        speed = p.calculate_speed()
        elapsed = time.time() - p.start_time
        
        remaining = p.total_bytes - p.current_bytes
        eta = remaining / speed if speed > 0 else 0
        
        filled = int(percentage / 100 * PROGRESS_BAR_LENGTH)
        bar = "█" * filled + "░" * (PROGRESS_BAR_LENGTH - filled)
        
        current_mb = p.current_bytes / (1024 * 1024)
        total_mb = p.total_bytes / (1024 * 1024)
        speed_mbs = speed / (1024 * 1024)
        
        if eta < 60:
            eta_str = f"{int(eta)}s"
        elif eta < 3600:
            eta_str = f"{int(eta // 60)}m {int(eta % 60)}s"
        else:
            eta_str = f"{int(eta // 3600)}h {int((eta % 3600) // 60)}m"
        
        action = "⬇️ Downloading" if p.transfer_type == "download" else "⬆️ Uploading"
        name = f"\n📄 {p.file_name}" if p.file_name else ""
        
        return (
            f"{action}{name}\n\n"
            f"{bar} {percentage:.1f}%\n\n"
            f"📦 {current_mb:.1f} / {total_mb:.1f} MB\n"
            f"⚡ {speed_mbs:.2f} MB/s\n"
            f"⏱ ETA: {eta_str}"
        )
    
    async def finalize(
        self,
        transfer_id: str,
        success: bool = True,
        error: str = None
    ) -> None:
        async with self._get_lock():
            progress = self._transfers.get(transfer_id)
            if not progress or progress.is_finalized:
                return
            
            progress.is_active = False
            progress.is_finalized = True
            
            elapsed = time.time() - progress.start_time
            total_mb = progress.total_bytes / (1024 * 1024) if progress.total_bytes > 0 else 0
            avg_speed = (progress.total_bytes / elapsed / (1024 * 1024)) if elapsed > 0 else 0
            
            if success:
                text = (
                    f"✅ {'Download' if progress.transfer_type == 'download' else 'Upload'} Complete\n\n"
                    f"📦 {total_mb:.1f} MB\n"
                    f"⏱ {elapsed:.1f}s\n"
                    f"⚡ {avg_speed:.2f} MB/s avg"
                )
            else:
                text = f"❌ Failed\n\n{error or 'Unknown error'}"
            
            try:
                if progress.client and progress.message:
                    await progress.client.edit_message_text(
                        chat_id=progress.message.chat.id,
                        message_id=progress.message.id,
                        text=text
                    )
            except:
                pass
    
    def cleanup(self, transfer_id: str) -> None:
        self._transfers.pop(transfer_id, None)
    
    def cancel(self, transfer_id: str) -> None:
        progress = self._transfers.get(transfer_id)
        if progress:
            progress.is_active = False


progress_reporter = ProgressReporter()
