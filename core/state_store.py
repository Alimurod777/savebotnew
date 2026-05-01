"""
SQLite State Store for Download Tasks

Local persistent storage for:
- Download task metadata and progress
- Resume offsets for partial downloads
- Duplicate detection

ALL blocking SQLite I/O is wrapped with asyncio.to_thread()
to keep the event loop non-blocking.

Schema uses WAL mode for concurrent access and performance.
"""

import sqlite3
import asyncio
import os
import time
import logging
from typing import List, Optional
from pathlib import Path

from core.models import DownloadTask

logger = logging.getLogger(__name__)

# Default database path (absolute - Colab/Kaggle safe)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "downloads.db")


class StateStore:
    """
    SQLite-based state store for download tasks.
    
    Features:
    - All I/O via asyncio.to_thread() (non-blocking)
    - WAL mode for concurrent reads
    - Automatic offset/part file reconciliation on startup
    - Duplicate detection via user_id + file_unique_id
    """
    
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._initialized = False
    
    def _get_connection(self) -> sqlite3.Connection:
        """
        Get a new SQLite connection.
        
        Each call creates a new connection (safe for threaded access).
        Connections are configured with WAL mode and optimized pragmas.
        """
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # Performance optimizations
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn
    
    def _init_schema_sync(self) -> None:
        """Initialize database schema (blocking - called via to_thread)."""
        # Ensure data directory exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        
        conn = self._get_connection()
        try:
            conn.executescript("""
                -- Main downloads table
                CREATE TABLE IF NOT EXISTS downloads (
                    task_id TEXT PRIMARY KEY,
                    file_unique_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    file_id TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    mime_type TEXT DEFAULT '',
                    media_type TEXT DEFAULT 'document',
                    dc_id INTEGER DEFAULT 0,
                    access_hash INTEGER DEFAULT 0,
                    file_reference TEXT DEFAULT '',
                    thumb_size TEXT DEFAULT '',
                    dest_path TEXT DEFAULT '',
                    temp_path TEXT DEFAULT '',
                    offset INTEGER DEFAULT 0,
                    downloaded_bytes INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'PENDING',
                    error_message TEXT DEFAULT '',
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 5,
                    progress_message_id INTEGER DEFAULT 0,
                    progress_chat_id INTEGER DEFAULT 0,
                    created_at REAL NOT NULL,
                    started_at REAL DEFAULT 0,
                    completed_at REAL DEFAULT 0,
                    updated_at REAL NOT NULL,
                    priority INTEGER DEFAULT 10
                );
                
                -- Indexes for common queries
                CREATE INDEX IF NOT EXISTS idx_status ON downloads(status);
                CREATE INDEX IF NOT EXISTS idx_user_id ON downloads(user_id);
                CREATE INDEX IF NOT EXISTS idx_user_status ON downloads(user_id, status);
                CREATE INDEX IF NOT EXISTS idx_file_unique_id ON downloads(file_unique_id);
                
                -- Unique constraint for duplicate detection
                CREATE UNIQUE INDEX IF NOT EXISTS idx_user_file 
                    ON downloads(user_id, file_unique_id);
                
                -- Priority queue ordering
                CREATE INDEX IF NOT EXISTS idx_queue_order 
                    ON downloads(status, priority, created_at);
            """)
            conn.commit()
            logger.info(f"SQLite schema initialized: {self.db_path}")
        finally:
            conn.close()
    
    async def init(self) -> None:
        """Initialize the state store (async wrapper)."""
        if self._initialized:
            return
        await asyncio.to_thread(self._init_schema_sync)
        self._initialized = True
    
    async def close(self) -> None:
        """Close the state store."""
        # SQLite connections are created per-operation, nothing to close
        self._initialized = False
        logger.info("StateStore closed")
    
    # ==================== INSERT OPERATIONS ====================
    
    def _insert_task_sync(self, task: DownloadTask) -> bool:
        """Insert a task (blocking)."""
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO downloads VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, task.to_tuple())
            conn.commit()
            return True
        except sqlite3.IntegrityError as e:
            logger.warning(f"Task insert failed (likely duplicate): {e}")
            return False
        except Exception as e:
            logger.error(f"Task insert error: {e}")
            return False
        finally:
            conn.close()
    
    async def insert_task(self, task: DownloadTask) -> bool:
        """Insert a new download task."""
        return await asyncio.to_thread(self._insert_task_sync, task)
    
    # ==================== DUPLICATE DETECTION ====================
    
    def _check_duplicate_sync(
        self,
        user_id: int,
        file_unique_id: str
    ) -> Optional[DownloadTask]:
        """Check for duplicate (blocking)."""
        conn = self._get_connection()
        try:
            cursor = conn.execute("""
                SELECT * FROM downloads 
                WHERE user_id = ? AND file_unique_id = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (user_id, file_unique_id))
            row = cursor.fetchone()
            if row:
                return DownloadTask.from_row(tuple(row))
            return None
        finally:
            conn.close()
    
    async def check_duplicate(
        self,
        user_id: int,
        file_unique_id: str
    ) -> Optional[DownloadTask]:
        """
        Check if file was already downloaded by user.
        
        Returns existing task if found, None otherwise.
        """
        return await asyncio.to_thread(
            self._check_duplicate_sync, user_id, file_unique_id
        )
    
    # ==================== RESUME OPERATIONS ====================
    
    def _load_incomplete_sync(self) -> List[DownloadTask]:
        """Load incomplete tasks for resume (blocking)."""
        conn = self._get_connection()
        try:
            cursor = conn.execute("""
                SELECT * FROM downloads 
                WHERE status IN ('PENDING', 'DOWNLOADING')
                ORDER BY priority ASC, created_at ASC
            """)
            tasks = []
            for row in cursor.fetchall():
                task = DownloadTask.from_row(tuple(row))
                
                # CRITICAL: Reconcile offset with actual .part file size
                if task.temp_path and os.path.exists(task.temp_path):
                    actual_size = os.path.getsize(task.temp_path)
                    task.offset = actual_size
                    task.downloaded_bytes = actual_size
                    logger.debug(f"Resume {task.task_id}: offset={actual_size}")
                else:
                    # No .part file - start from beginning
                    task.offset = 0
                    task.downloaded_bytes = 0
                
                # Reset status to PENDING for re-queue
                task.status = "PENDING"
                tasks.append(task)
            
            return tasks
        finally:
            conn.close()
    
    async def load_incomplete_tasks(self) -> List[DownloadTask]:
        """
        Load all incomplete tasks for resume on startup.
        
        Reconciles stored offsets with actual .part file sizes.
        """
        return await asyncio.to_thread(self._load_incomplete_sync)
    
    # ==================== UPDATE OPERATIONS ====================
    
    def _update_offset_sync(
        self,
        task_id: str,
        offset: int,
        downloaded: int
    ) -> None:
        """Update offset (blocking)."""
        conn = self._get_connection()
        try:
            conn.execute("""
                UPDATE downloads 
                SET offset = ?, downloaded_bytes = ?, updated_at = ?
                WHERE task_id = ?
            """, (offset, downloaded, time.time(), task_id))
            conn.commit()
        finally:
            conn.close()
    
    async def update_offset(
        self,
        task_id: str,
        offset: int,
        downloaded: int = None
    ) -> None:
        """Update download offset for resume support."""
        if downloaded is None:
            downloaded = offset
        await asyncio.to_thread(self._update_offset_sync, task_id, offset, downloaded)
    
    def _update_status_sync(
        self,
        task_id: str,
        status: str,
        error: str = ""
    ) -> None:
        """Update status (blocking)."""
        conn = self._get_connection()
        try:
            now = time.time()
            if status == "DOWNLOADING":
                conn.execute("""
                    UPDATE downloads 
                    SET status = ?, error_message = ?, started_at = ?, updated_at = ?
                    WHERE task_id = ?
                """, (status, error, now, now, task_id))
            elif status == "COMPLETED":
                conn.execute("""
                    UPDATE downloads 
                    SET status = ?, error_message = ?, completed_at = ?, updated_at = ?
                    WHERE task_id = ?
                """, (status, error, now, now, task_id))
            else:
                conn.execute("""
                    UPDATE downloads 
                    SET status = ?, error_message = ?, updated_at = ?
                    WHERE task_id = ?
                """, (status, error, now, task_id))
            conn.commit()
        finally:
            conn.close()
    
    async def update_status(
        self,
        task_id: str,
        status: str,
        error: str = ""
    ) -> None:
        """Update task status."""
        await asyncio.to_thread(self._update_status_sync, task_id, status, error)
    
    def _increment_retry_sync(self, task_id: str) -> int:
        """Increment retry count (blocking)."""
        conn = self._get_connection()
        try:
            conn.execute("""
                UPDATE downloads 
                SET retry_count = retry_count + 1, updated_at = ?
                WHERE task_id = ?
            """, (time.time(), task_id))
            conn.commit()
            cursor = conn.execute(
                "SELECT retry_count FROM downloads WHERE task_id = ?",
                (task_id,)
            )
            row = cursor.fetchone()
            return row[0] if row else 0
        finally:
            conn.close()
    
    async def increment_retry(self, task_id: str) -> int:
        """Increment retry count and return new value."""
        return await asyncio.to_thread(self._increment_retry_sync, task_id)
    
    def _update_progress_message_sync(
        self,
        task_id: str,
        message_id: int,
        chat_id: int
    ) -> None:
        """Update progress message ID (blocking)."""
        conn = self._get_connection()
        try:
            conn.execute("""
                UPDATE downloads 
                SET progress_message_id = ?, progress_chat_id = ?, updated_at = ?
                WHERE task_id = ?
            """, (message_id, chat_id, time.time(), task_id))
            conn.commit()
        finally:
            conn.close()
    
    async def update_progress_message(
        self,
        task_id: str,
        message_id: int,
        chat_id: int
    ) -> None:
        """Store progress message ID for updates."""
        await asyncio.to_thread(
            self._update_progress_message_sync, task_id, message_id, chat_id
        )
    
    def _update_file_reference_sync(
        self,
        task_id: str,
        file_reference: bytes,
        file_id: str
    ) -> None:
        """Update file reference (blocking)."""
        conn = self._get_connection()
        try:
            # Store file_reference as hex string for SQLite compatibility
            file_ref_hex = file_reference.hex() if file_reference else ""
            conn.execute("""
                UPDATE downloads 
                SET file_reference = ?, file_id = ?, updated_at = ?
                WHERE task_id = ?
            """, (file_ref_hex, file_id, time.time(), task_id))
            conn.commit()
        finally:
            conn.close()
    
    async def update_file_reference(
        self,
        task_id: str,
        file_reference: bytes,
        file_id: str
    ) -> None:
        """Update file reference after refresh from message reload."""
        await asyncio.to_thread(
            self._update_file_reference_sync, task_id, file_reference, file_id
        )
    
    # ==================== QUERY OPERATIONS ====================
    
    def _get_task_sync(self, task_id: str) -> Optional[DownloadTask]:
        """Get task by ID (blocking)."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM downloads WHERE task_id = ?",
                (task_id,)
            )
            row = cursor.fetchone()
            if row:
                return DownloadTask.from_row(tuple(row))
            return None
        finally:
            conn.close()
    
    async def get_task(self, task_id: str) -> Optional[DownloadTask]:
        """Get task by ID."""
        return await asyncio.to_thread(self._get_task_sync, task_id)
    
    def _get_user_tasks_sync(
        self,
        user_id: int,
        status: str = None,
        limit: int = 50
    ) -> List[DownloadTask]:
        """Get user tasks (blocking)."""
        conn = self._get_connection()
        try:
            if status:
                cursor = conn.execute("""
                    SELECT * FROM downloads 
                    WHERE user_id = ? AND status = ? 
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (user_id, status, limit))
            else:
                cursor = conn.execute("""
                    SELECT * FROM downloads 
                    WHERE user_id = ? 
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (user_id, limit))
            return [DownloadTask.from_row(tuple(row)) for row in cursor.fetchall()]
        finally:
            conn.close()
    
    async def get_user_tasks(
        self,
        user_id: int,
        status: str = None,
        limit: int = 50
    ) -> List[DownloadTask]:
        """Get all tasks for a user."""
        return await asyncio.to_thread(
            self._get_user_tasks_sync, user_id, status, limit
        )
    
    # ==================== CANCEL OPERATIONS ====================
    
    def _cancel_user_tasks_sync(self, user_id: int) -> int:
        """Cancel all user tasks (blocking)."""
        conn = self._get_connection()
        try:
            cursor = conn.execute("""
                UPDATE downloads 
                SET status = 'CANCELED', updated_at = ?
                WHERE user_id = ? AND status IN ('PENDING', 'DOWNLOADING')
            """, (time.time(), user_id))
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()
    
    async def cancel_user_tasks(self, user_id: int) -> int:
        """Cancel all pending/downloading tasks for a user."""
        return await asyncio.to_thread(self._cancel_user_tasks_sync, user_id)
    
    def _cancel_task_sync(self, task_id: str) -> bool:
        """Cancel a specific task (blocking)."""
        conn = self._get_connection()
        try:
            cursor = conn.execute("""
                UPDATE downloads 
                SET status = 'CANCELED', updated_at = ?
                WHERE task_id = ? AND status IN ('PENDING', 'DOWNLOADING')
            """, (time.time(), task_id))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a specific task."""
        return await asyncio.to_thread(self._cancel_task_sync, task_id)
    
    # ==================== STATISTICS ====================
    
    def _get_stats_sync(self) -> dict:
        """Get statistics (blocking)."""
        conn = self._get_connection()
        try:
            stats = {}
            for status in ['PENDING', 'DOWNLOADING', 'COMPLETED', 'FAILED', 'CANCELED']:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM downloads WHERE status = ?",
                    (status,)
                )
                stats[status.lower()] = cursor.fetchone()[0]
            
            # Total downloaded bytes
            cursor = conn.execute("""
                SELECT COALESCE(SUM(downloaded_bytes), 0) 
                FROM downloads WHERE status = 'COMPLETED'
            """)
            stats['total_bytes'] = cursor.fetchone()[0]
            
            return stats
        finally:
            conn.close()
    
    async def get_stats(self) -> dict:
        """Get download statistics."""
        return await asyncio.to_thread(self._get_stats_sync)
    
    # ==================== CLEANUP ====================
    
    def _cleanup_old_sync(self, days: int = 7) -> int:
        """Cleanup old completed/failed tasks (blocking)."""
        conn = self._get_connection()
        try:
            cutoff = time.time() - (days * 86400)
            cursor = conn.execute("""
                DELETE FROM downloads 
                WHERE status IN ('COMPLETED', 'FAILED', 'CANCELED')
                AND updated_at < ?
            """, (cutoff,))
            conn.commit()
            deleted = cursor.rowcount
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} old tasks")
            return deleted
        finally:
            conn.close()
    
    async def cleanup_old(self, days: int = 7) -> int:
        """Clean up old completed/failed tasks."""
        return await asyncio.to_thread(self._cleanup_old_sync, days)


# Global instance
state_store = StateStore()
