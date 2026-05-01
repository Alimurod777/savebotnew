"""
core/downloader/adaptive_engine.py - Production-Grade Adaptive Media Download Engine

A high-reliability, low-I/O, adaptive downloader for Telegram-based systems using
Pyrogram user sessions.

DESIGN PRINCIPLE: Session stability and data integrity first, speed last.

Size-Based Strategies:
- <= 100 MB: RAM-ONLY MODE (no disk until final write)
- 100-500 MB: RAM-FIRST MODE (sparse disk checkpoints)
- >= 500 MB: RESUME-REQUIRED MODE (mandatory resume with buffered disk writes)

Key Features:
- Adaptive worker pool (scales based on system load)
- Global RAM budget enforcement
- Per-DC rate limiting
- Session-safe operation (never starves PingTask)
- Graceful error handling with state recovery
"""

import asyncio
import os
import io
import time
import logging
import psutil
from typing import Optional, Callable, Dict, Any, Awaitable, Tuple
from dataclasses import dataclass, field
from enum import Enum

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.raw import functions, types
from pyrogram.raw.base import InputFileLocation
from pyrogram.errors import (
    FloodWait, FileMigrate, Timeout,
    FileReferenceExpired, FileReferenceEmpty, FileReferenceInvalid,
    RPCError
)
from pyrogram.file_id import FileId, FileType, PHOTO_TYPES

logger = logging.getLogger(__name__)


# Size thresholds (bytes)
SIZE_THRESHOLD_RAM_ONLY = 100 * 1024 * 1024      # <= 100 MB
SIZE_THRESHOLD_RAM_FIRST = 500 * 1024 * 1024    # 100-500 MB
# >= 500 MB uses RESUME_REQUIRED mode

# Chunk sizes
NETWORK_CHUNK_SIZE = 512 * 1024                  # 512 KB network chunks
RAM_BUFFER_AGGREGATE = 8 * 1024 * 1024           # 8 MB RAM aggregate buffer
LARGE_FILE_BUFFER = 16 * 1024 * 1024             # 16 MB for large files

# Checkpoint intervals
CHECKPOINT_BYTES_MEDIUM = 5 * 1024 * 1024        # Every 5 MB (100-500MB files)
CHECKPOINT_BYTES_LARGE = 25 * 1024 * 1024        # Every 25 MB (>500MB files)
CHECKPOINT_TIME_INTERVAL = 3.0                   # Every 3 seconds minimum

# Retry configuration
MAX_RETRIES = 5
BASE_RETRY_DELAY = 2.0
MAX_RETRY_DELAY = 60.0

# File reference errors tuple
FILE_REFERENCE_ERRORS = (FileReferenceExpired, FileReferenceEmpty, FileReferenceInvalid)


class DownloadMode(Enum):
    """Download strategy based on file size."""
    RAM_ONLY = "ram_only"           # <= 100 MB
    RAM_FIRST = "ram_first"         # 100-500 MB
    RESUME_REQUIRED = "resume"      # >= 500 MB


@dataclass
class RAMBudget:
    """
    Global RAM budget manager for all downloads.
    
    Enforces memory limits based on system RAM:
    - 2 GB system -> 128 MB budget
    - 4 GB system -> 256 MB budget
    - 8+ GB system -> 512 MB budget
    """
    _instance: 'RAMBudget' = None
    
    max_bytes: int = 0
    used_bytes: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    
    def __post_init__(self):
        if self.max_bytes == 0:
            self.max_bytes = self._calculate_budget()
    
    @classmethod
    def get_instance(cls) -> 'RAMBudget':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def _calculate_budget(self) -> int:
        """Calculate RAM budget based on system memory."""
        try:
            total_ram = psutil.virtual_memory().total
            if total_ram <= 2 * 1024**3:
                return 128 * 1024 * 1024  # 128 MB
            elif total_ram <= 4 * 1024**3:
                return 256 * 1024 * 1024  # 256 MB
            else:
                return 512 * 1024 * 1024  # 512 MB
        except Exception:
            return 128 * 1024 * 1024  # Default 128 MB
    
    async def acquire(self, size: int, timeout: float = 60.0) -> bool:
        """
        Acquire RAM allocation. Blocks if budget exceeded.
        
        FIX Issue 5.3: Increased timeout from 30s to 60s for better handling
        of concurrent large downloads.
        
        Returns False if timeout exceeded (memory pressure).
        """
        start = time.time()
        while True:
            async with self._lock:
                if self.used_bytes + size <= self.max_bytes:
                    self.used_bytes += size
                    return True
            
            if time.time() - start > timeout:
                logger.warning(
                    f"RAM budget timeout after {timeout}s: "
                    f"{size} bytes requested, {self.used_bytes}/{self.max_bytes} used"
                )
                return False
            
            await asyncio.sleep(0.1)
    
    async def release(self, size: int) -> None:
        """Release RAM allocation."""
        async with self._lock:
            self.used_bytes = max(0, self.used_bytes - size)
    
    @property
    def available(self) -> int:
        return max(0, self.max_bytes - self.used_bytes)
    
    @property
    def pressure(self) -> float:
        """Memory pressure 0.0-1.0 (1.0 = full)."""
        if self.max_bytes == 0:
            return 1.0
        return self.used_bytes / self.max_bytes


@dataclass
class ResumeMetadata:
    """
    Resume state for interrupted downloads.
    
    Stored in RAM first, written to disk only on checkpoints.
    """
    file_id: str
    file_unique_id: str
    chat_id: int
    message_id: int
    dc_id: int
    total_size: int
    downloaded_bytes: int = 0
    chunk_size: int = NETWORK_CHUNK_SIZE
    last_checkpoint: float = 0.0
    file_reference: bytes = b""
    
    def should_checkpoint(self, bytes_since_last: int) -> bool:
        """Check if checkpoint is needed."""
        time_elapsed = time.time() - self.last_checkpoint
        
        if self.total_size >= SIZE_THRESHOLD_RAM_FIRST:
            threshold = CHECKPOINT_BYTES_LARGE
        else:
            threshold = CHECKPOINT_BYTES_MEDIUM
        
        return bytes_since_last >= threshold or time_elapsed >= CHECKPOINT_TIME_INTERVAL


@dataclass
class DownloadResult:
    """Result of download operation."""
    success: bool
    file_path: str = ""
    error: str = ""
    bytes_downloaded: int = 0
    elapsed_seconds: float = 0.0
    mode: DownloadMode = DownloadMode.RAM_ONLY


@dataclass
class AdaptiveTask:
    """Task for adaptive download engine."""
    task_id: str
    user_id: int
    chat_id: int
    message_id: int
    file_id: str
    file_unique_id: str
    file_size: int
    file_name: str
    dc_id: int
    access_hash: int
    file_reference: bytes
    thumb_size: str = ""
    dest_path: str = ""
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    
    @property
    def mode(self) -> DownloadMode:
        """Determine download mode based on file size."""
        if self.file_size <= SIZE_THRESHOLD_RAM_ONLY:
            return DownloadMode.RAM_ONLY
        elif self.file_size <= SIZE_THRESHOLD_RAM_FIRST:
            return DownloadMode.RAM_FIRST
        else:
            return DownloadMode.RESUME_REQUIRED


class AdaptiveDownloadEngine:
    """
    Production-grade adaptive media download engine.
    
    Session stability is non-negotiable. All decisions protect:
    1. Session first
    2. Data integrity second
    3. Speed last
    
    Usage:
        engine = AdaptiveDownloadEngine(client)
        await engine.start()
        
        result = await engine.download(
            message=msg,
            progress_callback=callback
        )
        
        await engine.shutdown()
    """
    
    def __init__(
        self,
        client: Client,
        download_dir: str = "downloads/temp",
        max_workers: int = 8,
        min_workers: int = 2
    ):
        self.client = client
        self.download_dir = download_dir
        self.max_workers = max_workers
        self.min_workers = min_workers
        
        self._running = False
        self._ram_budget = RAMBudget.get_instance()
        self._dc_semaphores: Dict[int, asyncio.Semaphore] = {}
        self._active_downloads: Dict[str, asyncio.Task] = {}
        self._resume_states: Dict[str, ResumeMetadata] = {}
        
        # Adaptive worker state
        self._current_workers = min_workers
        self._worker_lock = asyncio.Lock()
        
        # Network health monitoring
        self._last_rtt: float = 0.0
        self._disk_latency: float = 0.0
    
    async def start(self) -> None:
        """Start the download engine."""
        if self._running:
            return
        
        os.makedirs(self.download_dir, exist_ok=True)
        self._running = True
        logger.info(
            f"AdaptiveDownloadEngine started (workers: {self.min_workers}-{self.max_workers}, "
            f"RAM budget: {self._ram_budget.max_bytes // 1024 // 1024}MB)"
        )
    
    async def shutdown(self) -> None:
        """Shutdown gracefully."""
        if not self._running:
            return
        
        self._running = False
        
        # Cancel all active downloads
        for task_id, task in list(self._active_downloads.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self._active_downloads.clear()
        logger.info("AdaptiveDownloadEngine shutdown complete")
    
    def _get_dc_semaphore(self, dc_id: int) -> asyncio.Semaphore:
        """Get per-DC semaphore for rate limiting."""
        if dc_id not in self._dc_semaphores:
            # Conservative limits to protect session
            self._dc_semaphores[dc_id] = asyncio.Semaphore(4)
        return self._dc_semaphores[dc_id]
    
    def _build_input_location(self, task: AdaptiveTask) -> InputFileLocation:
        """Build InputFileLocation for raw API."""
        file_id = FileId.decode(task.file_id)
        file_type = file_id.file_type
        
        if file_type in PHOTO_TYPES:
            return types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=task.file_reference or file_id.file_reference,
                thumb_size=task.thumb_size or file_id.thumbnail_size or ""
            )
        else:
            return types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=task.file_reference or file_id.file_reference,
                thumb_size=task.thumb_size or ""
            )
    
    async def download(
        self,
        message: Message,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        user_id: int = 0,
        dest_dir: str = None
    ) -> DownloadResult:
        """
        Download media with adaptive strategy.
        
        Args:
            message: Pyrogram Message with media
            progress_callback: Optional callback(current, total)
            user_id: User ID for tracking
            dest_dir: Destination directory
        
        Returns:
            DownloadResult with file path or error
        """
        start_time = time.time()
        dest_dir = dest_dir or self.download_dir
        
        # Extract task from message
        task = await self._extract_task(message, user_id, dest_dir)
        if not task:
            return DownloadResult(
                success=False,
                error="No downloadable media in message",
                elapsed_seconds=time.time() - start_time
            )
        
        logger.info(
            f"Download starting: {task.file_name} ({task.file_size / 1024 / 1024:.1f}MB) "
            f"mode={task.mode.value}"
        )
        
        # Get DC semaphore
        semaphore = self._get_dc_semaphore(task.dc_id)
        
        try:
            async with semaphore:
                if task.mode == DownloadMode.RAM_ONLY:
                    result = await self._download_ram_only(task, progress_callback)
                elif task.mode == DownloadMode.RAM_FIRST:
                    result = await self._download_ram_first(task, progress_callback)
                else:
                    result = await self._download_resume_required(task, progress_callback)
                
                result.elapsed_seconds = time.time() - start_time
                result.mode = task.mode
                return result
                
        except asyncio.CancelledError:
            logger.info(f"Download cancelled: {task.task_id}")
            return DownloadResult(
                success=False,
                error="Cancelled",
                bytes_downloaded=0,
                elapsed_seconds=time.time() - start_time,
                mode=task.mode
            )
        except Exception as e:
            logger.error(f"Download failed: {task.task_id} - {e}")
            return DownloadResult(
                success=False,
                error=str(e),
                elapsed_seconds=time.time() - start_time,
                mode=task.mode
            )
    
    async def _download_ram_only(
        self,
        task: AdaptiveTask,
        progress_callback: Optional[Callable[[int, int], None]]
    ) -> DownloadResult:
        """
        RAM-ONLY MODE: Files <= 100 MB
        
        - Download entirely in RAM
        - NO partial disk writes
        - Resume DISABLED
        - Single final write to disk
        - Disk I/O: exactly one write
        """
        buffer = io.BytesIO()
        location = self._build_input_location(task)
        offset = 0
        
        # Acquire RAM budget
        if not await self._ram_budget.acquire(task.file_size):
            return DownloadResult(
                success=False,
                error="RAM budget exceeded",
                bytes_downloaded=0
            )
        
        try:
            while offset < task.file_size:
                if task.cancel_event.is_set():
                    return DownloadResult(success=False, error="Cancelled", bytes_downloaded=offset)
                
                chunk_size = min(NETWORK_CHUNK_SIZE, task.file_size - offset)
                
                chunk = await self._fetch_chunk_with_retry(
                    location, offset, chunk_size, task
                )
                
                if chunk is None:
                    return DownloadResult(
                        success=False,
                        error="Failed to fetch chunk",
                        bytes_downloaded=offset
                    )
                
                buffer.write(chunk)
                offset += len(chunk)
                
                if progress_callback:
                    try:
                        progress_callback(offset, task.file_size)
                    except Exception:
                        pass
            
            # Single final write to disk
            file_path = os.path.join(self.download_dir, task.file_name)
            await asyncio.to_thread(self._write_file, file_path, buffer.getvalue())
            
            return DownloadResult(
                success=True,
                file_path=file_path,
                bytes_downloaded=offset
            )
            
        finally:
            await self._ram_budget.release(task.file_size)
            buffer.close()
    
    async def _download_ram_first(
        self,
        task: AdaptiveTask,
        progress_callback: Optional[Callable[[int, int], None]]
    ) -> DownloadResult:
        """
        RAM-FIRST MODE: Files 100-500 MB
        
        - Primary buffering in RAM
        - Disk writes only as sparse checkpoints
        - Resume metadata kept in RAM
        - Resume activated only after interruption
        - Checkpoints: Every 5-10 MB or every >=3 seconds
        """
        buffer = io.BytesIO()
        location = self._build_input_location(task)
        offset = 0
        last_checkpoint_bytes = 0
        last_checkpoint_time = time.time()
        
        # Use smaller RAM allocation (buffer aggregates)
        buffer_size = min(RAM_BUFFER_AGGREGATE * 2, task.file_size)
        
        if not await self._ram_budget.acquire(buffer_size):
            return DownloadResult(
                success=False,
                error="RAM budget exceeded",
                bytes_downloaded=0
            )
        
        # Initialize resume metadata (in RAM)
        resume_meta = ResumeMetadata(
            file_id=task.file_id,
            file_unique_id=task.file_unique_id,
            chat_id=task.chat_id,
            message_id=task.message_id,
            dc_id=task.dc_id,
            total_size=task.file_size,
            file_reference=task.file_reference
        )
        self._resume_states[task.task_id] = resume_meta
        
        part_path = os.path.join(self.download_dir, f"{task.task_id}.part")
        
        try:
            while offset < task.file_size:
                if task.cancel_event.is_set():
                    # Save state on cancel
                    await self._checkpoint_to_disk(part_path, buffer, resume_meta)
                    return DownloadResult(success=False, error="Cancelled", bytes_downloaded=offset)
                
                chunk_size = min(NETWORK_CHUNK_SIZE, task.file_size - offset)
                
                chunk = await self._fetch_chunk_with_retry(
                    location, offset, chunk_size, task
                )
                
                if chunk is None:
                    await self._checkpoint_to_disk(part_path, buffer, resume_meta)
                    return DownloadResult(
                        success=False,
                        error="Failed to fetch chunk",
                        bytes_downloaded=offset
                    )
                
                buffer.write(chunk)
                offset += len(chunk)
                resume_meta.downloaded_bytes = offset
                
                if progress_callback:
                    try:
                        progress_callback(offset, task.file_size)
                    except Exception:
                        pass
                
                # Sparse checkpoints
                bytes_since_checkpoint = offset - last_checkpoint_bytes
                time_since_checkpoint = time.time() - last_checkpoint_time
                
                if bytes_since_checkpoint >= CHECKPOINT_BYTES_MEDIUM or time_since_checkpoint >= CHECKPOINT_TIME_INTERVAL:
                    await self._checkpoint_to_disk(part_path, buffer, resume_meta)
                    buffer = io.BytesIO()  # Reset buffer after checkpoint
                    last_checkpoint_bytes = offset
                    last_checkpoint_time = time.time()
                    resume_meta.last_checkpoint = last_checkpoint_time
            
            # Final write
            await self._checkpoint_to_disk(part_path, buffer, resume_meta)
            
            # Rename to final
            file_path = os.path.join(self.download_dir, task.file_name)
            await asyncio.to_thread(self._finalize_file, part_path, file_path)
            
            # Cleanup resume state
            self._resume_states.pop(task.task_id, None)
            
            return DownloadResult(
                success=True,
                file_path=file_path,
                bytes_downloaded=offset
            )
            
        finally:
            await self._ram_budget.release(buffer_size)
            buffer.close()
    
    async def _download_resume_required(
        self,
        task: AdaptiveTask,
        progress_callback: Optional[Callable[[int, int], None]]
    ) -> DownloadResult:
        """
        RESUME-REQUIRED MODE: Files >= 500 MB
        
        - Resume is MANDATORY
        - Network chunks: 512 KB
        - RAM buffers: 8-16 MB aggregates
        - Disk writes: Only when buffer full
        - Checkpoints: Every 25-50 MB or every 3-5 seconds
        - Rate-limited, bounded disk writes
        """
        location = self._build_input_location(task)
        part_path = os.path.join(self.download_dir, f"{task.task_id}.part")
        
        # Check for existing partial download
        offset = 0
        if os.path.exists(part_path):
            offset = os.path.getsize(part_path)
            logger.info(f"Resuming from offset {offset}/{task.file_size}")
        
        # Initialize resume metadata
        resume_meta = ResumeMetadata(
            file_id=task.file_id,
            file_unique_id=task.file_unique_id,
            chat_id=task.chat_id,
            message_id=task.message_id,
            dc_id=task.dc_id,
            total_size=task.file_size,
            downloaded_bytes=offset,
            file_reference=task.file_reference
        )
        self._resume_states[task.task_id] = resume_meta
        
        # Allocate RAM buffer
        if not await self._ram_budget.acquire(LARGE_FILE_BUFFER):
            return DownloadResult(
                success=False,
                error="RAM budget exceeded",
                bytes_downloaded=offset
            )
        
        buffer = io.BytesIO()
        last_checkpoint_bytes = offset
        last_checkpoint_time = time.time()
        
        try:
            # Open file in append mode
            file_handle = await asyncio.to_thread(
                open, part_path, 'ab' if offset > 0 else 'wb'
            )
            
            try:
                while offset < task.file_size:
                    if task.cancel_event.is_set():
                        # Flush buffer and save state
                        if buffer.tell() > 0:
                            await asyncio.to_thread(file_handle.write, buffer.getvalue())
                            await asyncio.to_thread(file_handle.flush)
                        await self._save_resume_metadata(task.task_id, resume_meta)
                        return DownloadResult(success=False, error="Cancelled", bytes_downloaded=offset)
                    
                    chunk_size = min(NETWORK_CHUNK_SIZE, task.file_size - offset)
                    
                    chunk = await self._fetch_chunk_with_retry(
                        location, offset, chunk_size, task
                    )
                    
                    if chunk is None:
                        if buffer.tell() > 0:
                            await asyncio.to_thread(file_handle.write, buffer.getvalue())
                            await asyncio.to_thread(file_handle.flush)
                        await self._save_resume_metadata(task.task_id, resume_meta)
                        return DownloadResult(
                            success=False,
                            error="Failed to fetch chunk",
                            bytes_downloaded=offset
                        )
                    
                    buffer.write(chunk)
                    offset += len(chunk)
                    resume_meta.downloaded_bytes = offset
                    
                    if progress_callback:
                        try:
                            progress_callback(offset, task.file_size)
                        except Exception:
                            pass
                    
                    # Flush buffer when full
                    if buffer.tell() >= LARGE_FILE_BUFFER:
                        await asyncio.to_thread(file_handle.write, buffer.getvalue())
                        buffer = io.BytesIO()
                    
                    # Sparse checkpoints
                    bytes_since_checkpoint = offset - last_checkpoint_bytes
                    time_since_checkpoint = time.time() - last_checkpoint_time
                    
                    if bytes_since_checkpoint >= CHECKPOINT_BYTES_LARGE or time_since_checkpoint >= 5.0:
                        await asyncio.to_thread(file_handle.flush)
                        await self._save_resume_metadata(task.task_id, resume_meta)
                        last_checkpoint_bytes = offset
                        last_checkpoint_time = time.time()
                        resume_meta.last_checkpoint = last_checkpoint_time
                
                # Flush remaining buffer
                if buffer.tell() > 0:
                    await asyncio.to_thread(file_handle.write, buffer.getvalue())
                    await asyncio.to_thread(file_handle.flush)
                
            finally:
                await asyncio.to_thread(file_handle.close)
            
            # Rename to final
            file_path = os.path.join(self.download_dir, task.file_name)
            await asyncio.to_thread(self._finalize_file, part_path, file_path)
            
            # Cleanup resume state
            self._resume_states.pop(task.task_id, None)
            self._cleanup_resume_file(task.task_id)
            
            return DownloadResult(
                success=True,
                file_path=file_path,
                bytes_downloaded=offset
            )
            
        finally:
            await self._ram_budget.release(LARGE_FILE_BUFFER)
            buffer.close()
    
    async def _fetch_chunk_with_retry(
        self,
        location: InputFileLocation,
        offset: int,
        limit: int,
        task: AdaptiveTask
    ) -> Optional[bytes]:
        """
        Fetch chunk with retry logic and error handling.
        
        Handles:
        - FloodWait
        - File reference errors
        - Network timeouts
        - DC migrations
        """
        for attempt in range(MAX_RETRIES):
            if task.cancel_event.is_set():
                return None
            
            try:
                result = await self.client.invoke(
                    functions.upload.GetFile(
                        location=location,
                        offset=offset,
                        limit=limit
                    ),
                    sleep_threshold=60
                )
                
                if isinstance(result, types.upload.File):
                    return result.bytes
                elif isinstance(result, types.upload.FileCdnRedirect):
                    # Handle CDN redirect
                    logger.debug("CDN redirect, retrying on main DC")
                    await asyncio.sleep(1)
                    continue
                else:
                    logger.warning(f"Unexpected response type: {type(result)}")
                    return None
                    
            except FloodWait as e:
                wait_time = getattr(e, 'value', getattr(e, 'x', 30))
                logger.warning(f"FloodWait: {wait_time}s")
                # Cap wait time to avoid session issues
                await asyncio.sleep(min(wait_time, 60))
                continue
                
            except FileMigrate as e:
                new_dc = getattr(e, 'value', getattr(e, 'x', 2))
                logger.info(f"File migrated to DC{new_dc}")
                await asyncio.sleep(1)
                continue
                
            except FILE_REFERENCE_ERRORS as e:
                logger.warning(f"File reference error: {type(e).__name__}")
                return None  # Caller must handle refresh
                
            except (Timeout, ConnectionError, OSError) as e:
                delay = min(BASE_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                logger.warning(f"Network error, retry in {delay}s: {e}")
                await asyncio.sleep(delay)
                continue
                
            except RPCError as e:
                error_id = getattr(e, 'ID', '') or str(e)
                if 'FILE_REFERENCE' in error_id.upper():
                    logger.warning(f"File reference RPC error: {error_id}")
                    return None
                raise
        
        logger.error(f"Max retries exceeded at offset {offset}")
        return None
    
    async def _checkpoint_to_disk(
        self,
        path: str,
        buffer: io.BytesIO,
        meta: ResumeMetadata
    ) -> None:
        """Write buffer to disk and update metadata."""
        if buffer.tell() > 0:
            data = buffer.getvalue()
            await asyncio.to_thread(self._append_file, path, data)
    
    async def _save_resume_metadata(self, task_id: str, meta: ResumeMetadata) -> None:
        """Save resume metadata to disk as lightweight JSON state file."""
        import json
        meta_path = os.path.join(self.download_dir, f"{task_id}.resume")
        data = {
            'file_id': meta.file_id,
            'file_unique_id': meta.file_unique_id,
            'chat_id': meta.chat_id,
            'message_id': meta.message_id,
            'dc_id': meta.dc_id,
            'total_size': meta.total_size,
            'downloaded_bytes': meta.downloaded_bytes,
            'chunk_size': meta.chunk_size,
            'file_reference': meta.file_reference.hex() if meta.file_reference else ""
        }
        try:
            await asyncio.to_thread(self._write_json, meta_path, data)
        except Exception as e:
            # Resume metadata loss is non-fatal — worst case is re-download
            logger.warning(f"Failed to save resume metadata for {task_id}: {e}")
    
    def _cleanup_resume_file(self, task_id: str) -> None:
        """Remove resume metadata file."""
        meta_path = os.path.join(self.download_dir, f"{task_id}.resume")
        try:
            if os.path.exists(meta_path):
                os.remove(meta_path)
        except Exception:
            pass
    
    @staticmethod
    def _write_file(path: str, data: bytes) -> None:
        """Write data to file (blocking, run in thread)."""
        with open(path, 'wb') as f:
            f.write(data)
    
    @staticmethod
    def _append_file(path: str, data: bytes) -> None:
        """Append data to file (blocking, run in thread)."""
        with open(path, 'ab') as f:
            f.write(data)
    
    @staticmethod
    def _write_json(path: str, data: dict) -> None:
        """Write JSON to file (blocking, run in thread)."""
        import json
        with open(path, 'w') as f:
            json.dump(data, f)
    
    @staticmethod
    def _finalize_file(part_path: str, final_path: str) -> None:
        """Rename part file to final path."""
        if os.path.exists(final_path):
            os.remove(final_path)
        os.rename(part_path, final_path)
    
    async def _extract_task(
        self,
        message: Message,
        user_id: int,
        dest_dir: str
    ) -> Optional[AdaptiveTask]:
        """Extract task from message."""
        import uuid
        
        if not message.media:
            return None
        
        media = None
        file_name = None
        
        if message.document:
            media = message.document
            file_name = media.file_name or f"doc_{message.id}"
        elif message.video:
            media = message.video
            file_name = media.file_name or f"video_{message.id}.mp4"
        elif message.audio:
            media = message.audio
            file_name = media.file_name or f"audio_{message.id}.mp3"
        elif message.photo:
            media = message.photo
            file_name = f"photo_{message.id}.jpg"
        elif message.animation:
            media = message.animation
            file_name = media.file_name or f"anim_{message.id}.mp4"
        elif message.voice:
            media = message.voice
            file_name = f"voice_{message.id}.ogg"
        elif message.video_note:
            media = message.video_note
            file_name = f"vnote_{message.id}.mp4"
        elif message.sticker:
            media = message.sticker
            ext = ".webm" if media.is_video else ".webp"
            file_name = f"sticker_{message.id}{ext}"
        
        if not media:
            return None
        
        file_id_str = media.file_id
        file_unique_id = media.file_unique_id
        file_size = getattr(media, 'file_size', 0) or 0
        
        try:
            file_id_obj = FileId.decode(file_id_str)
        except Exception as e:
            logger.error(f"Failed to decode file_id: {e}")
            return None
        
        return AdaptiveTask(
            task_id=str(uuid.uuid4()),
            user_id=user_id or (message.from_user.id if message.from_user else 0),
            chat_id=message.chat.id,
            message_id=message.id,
            file_id=file_id_str,
            file_unique_id=file_unique_id,
            file_size=file_size,
            file_name=file_name,
            dc_id=file_id_obj.dc_id,
            access_hash=file_id_obj.access_hash,
            file_reference=file_id_obj.file_reference or b"",
            dest_path=os.path.join(dest_dir, file_name)
        )
    
    async def cancel_download(self, task_id: str) -> bool:
        """Cancel a specific download."""
        if task_id in self._resume_states:
            meta = self._resume_states[task_id]
            await self._save_resume_metadata(task_id, meta)
        
        if task_id in self._active_downloads:
            self._active_downloads[task_id].cancel()
            return True
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get engine statistics."""
        return {
            "running": self._running,
            "active_downloads": len(self._active_downloads),
            "ram_budget_mb": self._ram_budget.max_bytes // 1024 // 1024,
            "ram_used_mb": self._ram_budget.used_bytes // 1024 // 1024,
            "ram_pressure": f"{self._ram_budget.pressure:.1%}",
            "resume_states": len(self._resume_states)
        }


# Global instance
_adaptive_engine: Optional[AdaptiveDownloadEngine] = None


async def get_adaptive_engine(client: Client) -> AdaptiveDownloadEngine:
    """Get or create global adaptive download engine."""
    global _adaptive_engine
    if _adaptive_engine is None:
        _adaptive_engine = AdaptiveDownloadEngine(client)
        await _adaptive_engine.start()
    return _adaptive_engine


async def shutdown_adaptive_engine() -> None:
    """Shutdown global engine."""
    global _adaptive_engine
    if _adaptive_engine:
        await _adaptive_engine.shutdown()
        _adaptive_engine = None
