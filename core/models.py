"""
Data Models for Download System

Production-grade dataclasses for:
- DownloadTask: Complete download job metadata
- ProgressEvent: Progress updates sent to ProgressManager
- SpeedTracker: Rolling average speed calculator
- DownloadResult: Result of download operation
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time


class DownloadStatus(Enum):
    """Download task status values."""
    PENDING = "PENDING"
    DOWNLOADING = "DOWNLOADING"
    UPLOADING = "UPLOADING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


@dataclass
class DownloadTask:
    """
    Complete download task model.
    
    Stored in SQLite for persistence and resume support.
    Contains all metadata needed for raw API downloads.
    """
    
    # Primary identifiers
    task_id: str
    file_unique_id: str
    
    # User context
    user_id: int
    chat_id: int
    message_id: int
    
    # File metadata
    file_id: str
    file_size: int
    file_name: str
    mime_type: str = ""
    media_type: str = "document"
    dc_id: int = 0
    
    # Raw API access
    access_hash: int = 0
    file_reference: bytes = b""
    thumb_size: str = ""
    
    # Paths
    dest_path: str = ""
    temp_path: str = ""
    
    # Progress tracking
    offset: int = 0
    downloaded_bytes: int = 0
    
    # Status
    status: str = "PENDING"
    error_message: str = ""
    retry_count: int = 0
    max_retries: int = 5
    
    # Progress message tracking
    progress_message_id: int = 0
    progress_chat_id: int = 0
    
    # Timestamps
    created_at: float = field(default_factory=time.time)
    started_at: float = 0.0
    completed_at: float = 0.0
    updated_at: float = field(default_factory=time.time)
    
    # Queue priority (lower = higher priority)
    priority: int = 10
    
    def to_tuple(self) -> tuple:
        """Convert to tuple for SQLite INSERT."""
        return (
            self.task_id, self.file_unique_id, self.user_id, self.chat_id,
            self.message_id, self.file_id, self.file_size, self.file_name,
            self.mime_type, self.media_type, self.dc_id, self.access_hash,
            self.file_reference.hex() if self.file_reference else "",
            self.thumb_size, self.dest_path, self.temp_path, self.offset,
            self.downloaded_bytes, self.status, self.error_message,
            self.retry_count, self.max_retries, self.progress_message_id,
            self.progress_chat_id, self.created_at, self.started_at,
            self.completed_at, self.updated_at, self.priority
        )
    
    @classmethod
    def from_row(cls, row: tuple) -> 'DownloadTask':
        """Create from SQLite row."""
        file_ref = bytes.fromhex(row[12]) if row[12] else b""
        return cls(
            task_id=row[0], file_unique_id=row[1], user_id=row[2],
            chat_id=row[3], message_id=row[4], file_id=row[5],
            file_size=row[6], file_name=row[7], mime_type=row[8],
            media_type=row[9], dc_id=row[10], access_hash=row[11],
            file_reference=file_ref, thumb_size=row[13],
            dest_path=row[14], temp_path=row[15], offset=row[16],
            downloaded_bytes=row[17], status=row[18], error_message=row[19],
            retry_count=row[20], max_retries=row[21],
            progress_message_id=row[22], progress_chat_id=row[23],
            created_at=row[24], started_at=row[25], completed_at=row[26],
            updated_at=row[27], priority=row[28]
        )
    
    @property
    def percent(self) -> float:
        """Calculate download percentage."""
        if self.file_size <= 0:
            return 0.0
        return min(100.0, (self.downloaded_bytes / self.file_size) * 100)


@dataclass
class ProgressEvent:
    """
    Progress update event sent from workers to ProgressManager.
    
    Aggregated by ProgressManager for rate-limited UI updates.
    """
    task_id: str
    user_id: int
    chat_id: int
    message_id: int
    current_bytes: int
    total_bytes: int
    speed: float = 0.0
    eta: float = 0.0
    status: str = "DOWNLOADING"
    file_name: str = ""
    error: str = ""
    timestamp: float = field(default_factory=time.time)
    
    @property
    def percent(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        return min(100.0, (self.current_bytes / self.total_bytes) * 100)


@dataclass
class DownloadResult:
    """Result of a download operation."""
    success: bool
    file_path: str = ""
    error: str = ""
    bytes_downloaded: int = 0
    elapsed_seconds: float = 0.0


@dataclass 
class SpeedTracker:
    """
    Rolling average speed calculator.
    
    Maintains a fixed-size sample window for smooth speed estimates.
    """
    samples: list = field(default_factory=list)
    max_samples: int = 10
    
    def add_sample(self, total_bytes: int, timestamp: float) -> None:
        """Add a progress sample."""
        self.samples.append((total_bytes, timestamp))
        if len(self.samples) > self.max_samples:
            self.samples.pop(0)
    
    def get_speed(self) -> float:
        """Get current speed in bytes/sec."""
        if len(self.samples) < 2:
            return 0.0
        oldest, newest = self.samples[0], self.samples[-1]
        time_diff = newest[1] - oldest[1]
        if time_diff <= 0:
            return 0.0
        return (newest[0] - oldest[0]) / time_diff
    
    def get_eta(self, current: int, total: int) -> float:
        """Get estimated time remaining in seconds."""
        speed = self.get_speed()
        if speed <= 0:
            return 0.0
        remaining = total - current
        if remaining <= 0:
            return 0.0
        return remaining / speed
    
    def reset(self) -> None:
        """Clear all samples."""
        self.samples.clear()


# Sentinel object for shutdown signaling
SHUTDOWN_SENTINEL = object()


@dataclass
class DCConfig:
    """Per-DC configuration for rate limiting."""
    dc_id: int
    max_concurrent: int = 8
    chunk_size: int = 512 * 1024
