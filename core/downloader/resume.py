"""
core/downloader/resume.py - Resume download support for Telegram media.

Features:
- Resume interrupted downloads from last byte offset
- .part file management for large files
- RAM buffer for small files (< 20MB)
- State persistence for recovery
- Automatic cleanup on completion

Based on test/downloader.py implementation.
"""

import os
import json
import asyncio
import logging
from typing import Optional, Callable, Dict, Any, Union
from dataclasses import dataclass, field, asdict
from pathlib import Path

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, FileReferenceExpired, FileReferenceInvalid

logger = logging.getLogger(__name__)

# Constants
SMALL_FILE_THRESHOLD = 2 * 1024 * 1024 * 1024  # 2GB - download_media (parallel, fast). stream_media only for 2GB+
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for stream_media (2GB+ files only)
STATE_FILE_SUFFIX = ".resume.json"
PART_FILE_SUFFIX = ".part"
MAX_RETRIES = 5


@dataclass
class ResumeState:
    """State for a resumable download."""
    file_id: str
    chat_id: int
    message_id: int
    total_size: int
    downloaded_bytes: int = 0
    chunk_index: int = 0
    file_path: str = ""
    part_path: str = ""
    state_path: str = ""
    completed: bool = False
    error: Optional[str] = None
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "ResumeState":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
    
    def save(self) -> None:
        """Save state to JSON file."""
        if self.state_path:
            try:
                # Check if parent directory still exists (may have been
                # deleted by cleanup_user_temp after cancellation)
                parent = os.path.dirname(self.state_path)
                if parent and not os.path.isdir(parent):
                    logger.debug(f"Skip resume save — directory removed: {parent}")
                    return
                with open(self.state_path, 'w', encoding='utf-8') as f:
                    json.dump(self.to_dict(), f)
            except Exception as e:
                logger.warning(f"Failed to save resume state: {e}")
    
    @classmethod
    def load(cls, state_path: str) -> Optional["ResumeState"]:
        """Load state from JSON file."""
        try:
            if os.path.exists(state_path):
                with open(state_path, 'r', encoding='utf-8') as f:
                    return cls.from_dict(json.load(f))
        except Exception as e:
            logger.warning(f"Failed to load resume state: {e}")
        return None
    
    def cleanup(self) -> None:
        """Remove state and part files."""
        for path in [self.state_path, self.part_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as e:
                    logger.warning(f"Failed to cleanup {path}: {e}")


class ResumableDownloader:
    """
    Resumable download handler using Pyrogram's stream_media.
    
    Features:
    - Chunk-based streaming with offset support
    - Automatic resume from last position
    - Small files: RAM buffer (< 20MB)
    - Large files: .part file on disk
    - State persistence for crash recovery
    
    Usage:
        downloader = ResumableDownloader(client, download_dir)
        file_path = await downloader.download(
            message=msg,
            progress_callback=callback
        )
    """
    
    def __init__(
        self,
        client: Client,
        download_dir: str = "downloads/temp",
        chunk_size: int = CHUNK_SIZE,
        small_file_threshold: int = SMALL_FILE_THRESHOLD
    ):
        self.client = client
        self.download_dir = download_dir
        self.chunk_size = chunk_size
        self.small_file_threshold = small_file_threshold
        
        # Ensure download directory exists
        os.makedirs(download_dir, exist_ok=True)
    
    def _get_file_info(self, message: Message) -> tuple:
        """Extract file info from message."""
        media = None
        file_name = None
        file_size = 0
        file_id = None
        
        # Check media types
        if message.video:
            media = message.video
            file_name = media.file_name or f"video_{message.id}.mp4"
        elif message.document:
            media = message.document
            file_name = media.file_name or f"document_{message.id}"
        elif message.audio:
            media = message.audio
            file_name = media.file_name or f"audio_{message.id}.mp3"
        elif message.photo:
            media = message.photo
            file_name = f"photo_{message.id}.jpg"
        elif message.animation:
            media = message.animation
            file_name = media.file_name or f"animation_{message.id}.mp4"
        elif message.voice:
            media = message.voice
            file_name = f"voice_{message.id}.ogg"
        elif message.video_note:
            media = message.video_note
            file_name = f"video_note_{message.id}.mp4"
        elif message.sticker:
            media = message.sticker
            ext = ".webm" if media.is_video else ".webp"
            file_name = f"sticker_{message.id}{ext}"
        
        if media:
            file_size = getattr(media, 'file_size', 0) or 0
            file_id = getattr(media, 'file_id', None)
        
        return file_name, file_size, file_id
    
    def _get_paths(self, file_name: str) -> tuple:
        """Get file, part, and state paths."""
        file_path = os.path.join(self.download_dir, file_name)
        part_path = file_path + PART_FILE_SUFFIX
        state_path = file_path + STATE_FILE_SUFFIX
        return file_path, part_path, state_path
    
    async def download(
        self,
        message: Message,
        progress_callback: Callable[[int, int], None] = None,
        cancel_event: asyncio.Event = None
    ) -> Optional[str]:
        """
        Download media with resume support.
        
        Args:
            message: Pyrogram Message with media
            progress_callback: Callback(current, total)
            cancel_event: Event to signal cancellation
        
        Returns:
            File path on success, None on failure/cancellation
        """
        file_name, file_size, file_id = self._get_file_info(message)
        
        if not file_name or not file_id:
            logger.warning("No downloadable media in message")
            return None
        
        file_path, part_path, state_path = self._get_paths(file_name)
        
        # Check if already completed
        if os.path.exists(file_path):
            local_size = os.path.getsize(file_path)
            if local_size == file_size:
                logger.info(f"File already exists: {file_name}")
                # Cleanup any leftover state
                for p in [part_path, state_path]:
                    if os.path.exists(p):
                        os.remove(p)
                return file_path
        
        # Load or create state
        state = ResumeState.load(state_path)
        if state and state.file_id == file_id:
            # Resume from existing state
            logger.info(f"Resuming download: {file_name} from {state.downloaded_bytes}/{file_size}")
        else:
            # New download
            state = ResumeState(
                file_id=file_id,
                chat_id=message.chat.id,
                message_id=message.id,
                total_size=file_size,
                file_path=file_path,
                part_path=part_path,
                state_path=state_path
            )
        
        # Check existing part file
        if os.path.exists(part_path):
            part_size = os.path.getsize(part_path)
            if part_size <= file_size:
                if part_size % self.chunk_size != 0:
                    safe_size = part_size - (part_size % self.chunk_size)
                    with open(part_path, 'rb+') as f:
                        f.truncate(safe_size)
                    part_size = safe_size
                state.downloaded_bytes = part_size
                state.chunk_index = part_size // self.chunk_size
            else:
                # Part file is larger than expected, start fresh
                os.remove(part_path)
                state.downloaded_bytes = 0
                state.chunk_index = 0
        
        try:
            # Download using stream_media with offset
            result = await self._stream_download(
                message=message,
                state=state,
                progress_callback=progress_callback,
                cancel_event=cancel_event
            )
            
            if result:
                # Rename .part to final file
                if os.path.exists(part_path):
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    os.rename(part_path, file_path)
                
                # Cleanup state
                if os.path.exists(state_path):
                    os.remove(state_path)
                
                state.completed = True
                logger.info(f"Download completed: {file_name}")
                return file_path
            else:
                return None
                
        except asyncio.CancelledError:
            logger.info(f"Download cancelled: {file_name}")
            # Don't save state — temp directory may already be removed
            # by cleanup_user_temp(). Cancelled downloads start fresh.
            raise
        except Exception as e:
            logger.error(f"Download error: {e}")
            state.error = str(e)
            state.save()
            raise
    
    async def _stream_download(
        self,
        message: Message,
        state: ResumeState,
        progress_callback: Callable[[int, int], None] = None,
        cancel_event: asyncio.Event = None
    ) -> bool:
        """
        Stream download with resume support and connection hardening.
        
        CRITICAL: 
        - Progress callback is called on EVERY chunk
        - Connection errors trigger automatic retry with resume
        - DC switches handled transparently
        """
        import time as time_module
        
        use_ram_buffer = state.total_size <= self.small_file_threshold and state.downloaded_bytes == 0
        last_log_time = 0
        max_retries = 5
        retry_delay = 2.0
        
        for attempt in range(max_retries):
            try:
                if use_ram_buffer:
                    return await self._stream_ram_buffer(
                        message, state, progress_callback, cancel_event
                    )
                else:
                    return await self._stream_to_disk(
                        message, state, progress_callback, cancel_event, last_log_time
                    )
                    
            except (ConnectionResetError, TimeoutError, OSError) as e:
                # FileNotFoundError means temp directory was deleted
                # (user cancelled via /stop) — don't retry, bail out
                if isinstance(e, FileNotFoundError):
                    logger.info(f"Download aborted — temp dir removed: {e}")
                    return False

                # Connection errors - retry with backoff
                if attempt < max_retries - 1:
                    wait = retry_delay * (attempt + 1)
                    logger.warning(f"Connection error ({type(e).__name__}), retrying in {wait}s: {e}")
                    state.save()  # Save progress before retry
                    await asyncio.sleep(wait)
                    
                    # Recalculate skip_chunks from saved state
                    if os.path.exists(state.part_path):
                        state.downloaded_bytes = os.path.getsize(state.part_path)
                        state.chunk_index = state.downloaded_bytes // self.chunk_size
                    continue
                else:
                    logger.error(f"Max retries exceeded: {e}")
                    state.save()
                    raise
                    
            except asyncio.CancelledError:
                # Don't save state — temp directory may already be removed
                raise
                
            except Exception as e:
                error_str = str(e).lower()
                # Handle DC migration / media redirect
                if 'file_reference' in error_str or 'dc' in error_str:
                    if attempt < max_retries - 1:
                        logger.warning(f"DC/FileRef error, retrying: {e}")
                        state.save()
                        await asyncio.sleep(retry_delay)
                        continue
                raise
        
        return False
    
    async def _stream_ram_buffer(
        self,
        message: Message,
        state: ResumeState,
        progress_callback: Callable[[int, int], None] = None,
        cancel_event: asyncio.Event = None
    ) -> bool:
        """Download small/medium files using download_media (parallel, faster)."""
        def _progress_wrapper(current, total):
            state.downloaded_bytes = current
            if progress_callback:
                try:
                    progress_callback(current, total)
                except Exception:
                    pass

        file_path = await self.client.download_media(
            message,
            file_name=state.file_path,
            progress=_progress_wrapper
        )

        if file_path and os.path.exists(file_path):
            state.downloaded_bytes = state.total_size
            if state.file_path != file_path:
                import shutil
                shutil.move(file_path, state.file_path)
            return True
        return False
    
    async def _stream_to_disk(
        self,
        message: Message,
        state: ResumeState,
        progress_callback: Callable[[int, int], None] = None,
        cancel_event: asyncio.Event = None,
        last_log_time: float = 0
    ) -> bool:
        """Stream large file to disk with resume."""
        import time as time_module
        
        mode = 'ab' if state.downloaded_bytes > 0 else 'wb'
        skip_chunks = state.chunk_index
        
        if state.downloaded_bytes > 0:
            logger.info(f"Resuming from chunk {skip_chunks}, bytes {state.downloaded_bytes}/{state.total_size}")
            if progress_callback:
                try:
                    progress_callback(state.downloaded_bytes, state.total_size)
                except Exception:
                    pass
        
        with open(state.part_path, mode) as f:
            async for chunk in self.client.stream_media(message, offset=skip_chunks):
                if cancel_event and cancel_event.is_set():
                    # Don't save state — temp directory may be removed
                    return False
                
                f.write(chunk)
                state.downloaded_bytes += len(chunk)
                state.chunk_index += 1
                
                if progress_callback:
                    try:
                        progress_callback(state.downloaded_bytes, state.total_size)
                    except Exception as e:
                        logger.debug(f"Progress callback error: {e}")
                
                # Save state every 10MB
                if state.downloaded_bytes % (10 * 1024 * 1024) < len(chunk):
                    state.save()
                    now = time_module.time()
                    if now - last_log_time > 5:
                        logger.debug(f"Progress: {state.downloaded_bytes}/{state.total_size} bytes")
                        last_log_time = now
        
        if progress_callback:
            try:
                progress_callback(state.downloaded_bytes, state.total_size)
            except Exception:
                pass
        
        # Verify
        if os.path.exists(state.part_path):
            actual_size = os.path.getsize(state.part_path)
            if actual_size >= state.total_size * 0.99:
                return True
            else:
                logger.warning(f"Incomplete: {actual_size}/{state.total_size}")
                state.save()
                return False
        
        return False


class ResumeDownloadManager:
    """
    Manager for resumable downloads with state persistence.
    
    Handles:
    - Multiple concurrent downloads
    - State recovery on restart
    - Cleanup of completed/stale downloads
    """
    
    def __init__(self, download_dir: str = "downloads/temp"):
        self.download_dir = download_dir
        self._active_downloads: Dict[str, ResumableDownloader] = {}
        self._states: Dict[str, ResumeState] = {}
        
        os.makedirs(download_dir, exist_ok=True)
    
    def get_resumable_downloads(self) -> list:
        """Find all incomplete downloads that can be resumed."""
        resumable = []
        
        try:
            for file in os.listdir(self.download_dir):
                if file.endswith(STATE_FILE_SUFFIX):
                    state_path = os.path.join(self.download_dir, file)
                    state = ResumeState.load(state_path)
                    if state and not state.completed:
                        resumable.append(state)
        except Exception as e:
            logger.warning(f"Error scanning for resumable downloads: {e}")
        
        return resumable
    
    def cleanup_stale_downloads(self, max_age_hours: int = 24) -> int:
        """Remove stale .part and .resume files older than max_age_hours."""
        import time
        cleaned = 0
        cutoff = time.time() - (max_age_hours * 3600)
        
        try:
            for file in os.listdir(self.download_dir):
                if file.endswith((PART_FILE_SUFFIX, STATE_FILE_SUFFIX)):
                    file_path = os.path.join(self.download_dir, file)
                    if os.path.getmtime(file_path) < cutoff:
                        os.remove(file_path)
                        cleaned += 1
        except Exception as e:
            logger.warning(f"Error cleaning stale downloads: {e}")
        
        if cleaned > 0:
            logger.info(f"Cleaned {cleaned} stale download files")
        
        return cleaned


# Convenience function for integration with existing code
async def download_with_resume(
    client: Client,
    message: Message,
    download_dir: str = "downloads/temp",
    progress_callback: Callable[[int, int], None] = None,
    cancel_event: asyncio.Event = None
) -> Optional[str]:
    """
    Download media with automatic resume support.
    
    This is the main entry point for resume downloads.
    
    Args:
        client: Pyrogram Client
        message: Message with media
        download_dir: Directory for downloads
        progress_callback: Progress callback(current, total)
        cancel_event: Cancellation event
    
    Returns:
        File path on success, None on failure
    """
    downloader = ResumableDownloader(client, download_dir)
    return await downloader.download(message, progress_callback, cancel_event)
