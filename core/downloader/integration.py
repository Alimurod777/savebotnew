"""
core/downloader/integration.py - Integration with TechVJ Transfer System

Provides drop-in replacement for async_transfer downloads using the
Adaptive Download Engine.

Usage:
    # Replace async_transfer.async_downloader with adaptive_downloader
    from core.downloader.integration import AdaptiveTransferBridge
    
    bridge = AdaptiveTransferBridge(user_client)
    await bridge.start()
    
    result = await bridge.download(
        message=source_msg,
        progress_callback=progress_cb,
        cancel_checker=lambda: is_cancelled
    )
"""

import asyncio
import os
import logging
from typing import Optional, Callable, Union, Awaitable
from dataclasses import dataclass
from enum import Enum, auto

from pyrogram import Client
from pyrogram.types import Message

from core.downloader.adaptive_engine import (
    AdaptiveDownloadEngine,
    AdaptiveTask,
    DownloadMode,
    DownloadResult,
    get_adaptive_engine,
    shutdown_adaptive_engine
)
from core.downloader.session_safe_worker import (
    SessionSafeWorkerPool,
    HealthStatus
)

logger = logging.getLogger(__name__)


class TransferStatus(Enum):
    """Compatible with TechVJ.async_transfer.TransferStatus."""
    SUCCESS = auto()
    CANCELLED = auto()
    ERROR = auto()
    TIMEOUT = auto()
    FILE_TOO_LARGE = auto()


@dataclass
class TransferResult:
    """Compatible with TechVJ.async_transfer.TransferResult."""
    status: TransferStatus
    file_path: Optional[str] = None
    message: Optional[Message] = None
    error: Optional[str] = None
    bytes_transferred: int = 0
    download_mode: Optional[DownloadMode] = None


CancelChecker = Callable[[], Union[bool, Awaitable[bool]]]


class AdaptiveTransferBridge:
    """
    Bridge between TechVJ transfer system and Adaptive Download Engine.
    
    Provides compatible interface with session-safe, low-I/O downloads.
    
    Usage:
        bridge = AdaptiveTransferBridge(user_client)
        await bridge.start()
        
        result = await bridge.download(
            message=msg,
            progress_callback=cb,
            download_dir="downloads/",
            cancel_checker=lambda: cancelled
        )
        
        await bridge.shutdown()
    """
    
    def __init__(
        self,
        client: Client,
        download_dir: str = "downloads/temp",
        use_worker_pool: bool = True,
        max_workers: int = 8,
        per_user_limit: int = 3
    ):
        self.client = client
        self.download_dir = download_dir
        self.use_worker_pool = use_worker_pool
        
        self._engine: Optional[AdaptiveDownloadEngine] = None
        self._worker_pool: Optional[SessionSafeWorkerPool] = None
        self._running = False
        
        self._max_workers = max_workers
        self._per_user_limit = per_user_limit
    
    async def start(self) -> None:
        """Start the bridge."""
        if self._running:
            return
        
        os.makedirs(self.download_dir, exist_ok=True)
        
        self._engine = AdaptiveDownloadEngine(
            client=self.client,
            download_dir=self.download_dir,
            max_workers=self._max_workers,
            min_workers=2
        )
        await self._engine.start()
        
        if self.use_worker_pool:
            self._worker_pool = SessionSafeWorkerPool(
                initial_workers=4,
                min_workers=2,
                max_workers=self._max_workers,
                per_user_limit=self._per_user_limit
            )
            await self._worker_pool.start()
        
        self._running = True
        logger.info("AdaptiveTransferBridge started")
    
    async def shutdown(self) -> None:
        """Shutdown the bridge."""
        if not self._running:
            return
        
        self._running = False
        
        if self._worker_pool:
            await self._worker_pool.shutdown()
        
        if self._engine:
            await self._engine.shutdown()
        
        logger.info("AdaptiveTransferBridge shutdown")
    
    async def download(
        self,
        message: Message,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        download_dir: Optional[str] = None,
        cancel_checker: Optional[CancelChecker] = None,
        user_id: int = 0,
        timeout: float = 7200.0
    ) -> TransferResult:
        """
        Download media with adaptive strategy.
        
        Compatible with TechVJ.async_transfer interface.
        
        Args:
            message: Pyrogram Message with media
            progress_callback: Callback(current_bytes, total_bytes)
            download_dir: Override download directory
            cancel_checker: Callable returning True if cancelled
            user_id: User ID for tracking
            timeout: Maximum download time
        
        Returns:
            TransferResult compatible with TechVJ system
        """
        if not self._running:
            return TransferResult(
                status=TransferStatus.ERROR,
                error="Bridge not started"
            )
        
        dest_dir = download_dir or self.download_dir
        user_id = user_id or (message.from_user.id if message.from_user else 0)
        
        # Wrap progress callback with cancellation check
        async def wrapped_download():
            cancel_event = asyncio.Event()
            
            # Start cancellation monitor if checker provided
            if cancel_checker:
                async def monitor_cancel():
                    while not cancel_event.is_set():
                        try:
                            is_cancelled = cancel_checker()
                            if asyncio.iscoroutine(is_cancelled):
                                is_cancelled = await is_cancelled
                            if is_cancelled:
                                cancel_event.set()
                                return
                        except Exception:
                            pass
                        await asyncio.sleep(0.5)
                
                monitor_task = asyncio.create_task(monitor_cancel())
            else:
                monitor_task = None
            
            try:
                result = await asyncio.wait_for(
                    self._engine.download(
                        message=message,
                        progress_callback=progress_callback,
                        user_id=user_id,
                        dest_dir=dest_dir
                    ),
                    timeout=timeout
                )
                
                return self._convert_result(result)
                
            except asyncio.TimeoutError:
                return TransferResult(
                    status=TransferStatus.TIMEOUT,
                    error="Download timeout exceeded"
                )
            finally:
                if monitor_task:
                    monitor_task.cancel()
                    try:
                        await monitor_task
                    except asyncio.CancelledError:
                        pass
        
        # Submit through worker pool if enabled
        if self._worker_pool:
            try:
                return await self._worker_pool.submit(
                    job_id=f"dl_{message.id}_{user_id}",
                    user_id=user_id,
                    handler=wrapped_download
                )
            except Exception as e:
                return TransferResult(
                    status=TransferStatus.ERROR,
                    error=str(e)
                )
        else:
            return await wrapped_download()
    
    def _convert_result(self, engine_result: DownloadResult) -> TransferResult:
        """Convert engine result to transfer result."""
        if engine_result.success:
            return TransferResult(
                status=TransferStatus.SUCCESS,
                file_path=engine_result.file_path,
                bytes_transferred=engine_result.bytes_downloaded,
                download_mode=engine_result.mode
            )
        elif "Cancelled" in (engine_result.error or ""):
            return TransferResult(
                status=TransferStatus.CANCELLED,
                error=engine_result.error,
                bytes_transferred=engine_result.bytes_downloaded
            )
        else:
            return TransferResult(
                status=TransferStatus.ERROR,
                error=engine_result.error,
                bytes_transferred=engine_result.bytes_downloaded
            )
    
    async def cancel_download(self, task_id: str) -> bool:
        """Cancel a specific download."""
        if self._engine:
            return await self._engine.cancel_download(task_id)
        return False
    
    async def cancel_user_downloads(self, user_id: int) -> int:
        """Cancel all downloads for a user."""
        if self._worker_pool:
            return await self._worker_pool.cancel_user_jobs(user_id)
        return 0
    
    def get_stats(self) -> dict:
        """Get combined stats from engine and worker pool."""
        stats = {"running": self._running}
        
        if self._engine:
            stats["engine"] = self._engine.get_stats()
        
        if self._worker_pool:
            stats["worker_pool"] = self._worker_pool.get_stats()
        
        return stats
    
    def record_disk_latency(self, latency_ms: float) -> None:
        """Record disk latency for adaptive scaling."""
        if self._worker_pool:
            self._worker_pool.record_disk_latency(latency_ms)
    
    def record_network_rtt(self, rtt_ms: float) -> None:
        """Record network RTT for adaptive scaling."""
        if self._worker_pool:
            self._worker_pool.record_network_rtt(rtt_ms)


# Global bridge instance
_bridge: Optional[AdaptiveTransferBridge] = None


async def get_transfer_bridge(client: Client) -> AdaptiveTransferBridge:
    """Get or create global transfer bridge."""
    global _bridge
    if _bridge is None:
        _bridge = AdaptiveTransferBridge(client)
        await _bridge.start()
    return _bridge


async def shutdown_transfer_bridge() -> None:
    """Shutdown global bridge."""
    global _bridge
    if _bridge:
        await _bridge.shutdown()
        _bridge = None


# Drop-in replacement function
async def adaptive_download(
    client: Client,
    message: Message,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    download_dir: str = "downloads/temp",
    cancel_checker: Optional[CancelChecker] = None,
    user_id: int = 0
) -> TransferResult:
    """
    Drop-in replacement for async_transfer.async_downloader.
    
    Uses adaptive download engine with session-safe workers.
    
    Args:
        client: Pyrogram Client (userbot)
        message: Message with media
        progress_callback: Progress callback(current, total)
        download_dir: Download directory
        cancel_checker: Cancellation checker
        user_id: User ID
    
    Returns:
        TransferResult compatible with TechVJ system
    """
    bridge = await get_transfer_bridge(client)
    return await bridge.download(
        message=message,
        progress_callback=progress_callback,
        download_dir=download_dir,
        cancel_checker=cancel_checker,
        user_id=user_id
    )
