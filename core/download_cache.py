"""
Local Download Cache and Resume System

Features:
- In-memory cache with disk persistence
- Partial file tracking for resume
- Duplicate detection via file hashes
- Atomic file operations
- No MongoDB dependency
"""

import os
import json
import hashlib
import time
import aiofiles
import aiofiles.os
from dataclasses import dataclass, field, asdict
from typing import Dict, Optional, Set
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

CACHE_DIR = "data/download_cache"
RESUME_DIR = "data/resume"
CACHE_FILE = "cache_index.json"
DUPLICATE_CACHE_FILE = "duplicates.json"
HASH_CHUNK_SIZE = 4 * 1024 * 1024


@dataclass
class DownloadRecord:
    file_unique_id: str
    user_id: int
    chat_id: int
    message_id: int
    
    file_size: int = 0
    file_name: str = ""
    mime_type: str = ""
    
    downloaded_bytes: int = 0
    temp_path: Optional[str] = None
    final_path: Optional[str] = None
    
    content_hash: Optional[str] = None
    
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    
    is_complete: bool = False
    is_cancelled: bool = False
    error: Optional[str] = None
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'DownloadRecord':
        return cls(**data)


class DownloadCache:
    def __init__(self):
        self._records: Dict[str, DownloadRecord] = {}
        self._user_records: Dict[int, Set[str]] = {}
        self._content_hashes: Dict[str, str] = {}
        self._dirty = False
        self._lock = None
        self._lock_loop_id = None
        
        os.makedirs(CACHE_DIR, exist_ok=True)
        os.makedirs(RESUME_DIR, exist_ok=True)
    
    def _get_lock(self):
        import asyncio
        try:
            current_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return asyncio.Lock()
        
        if self._lock_loop_id != current_id:
            self._lock = asyncio.Lock()
            self._lock_loop_id = current_id
        return self._lock
    
    async def initialize(self) -> None:
        await self._load_cache()
        await self._load_duplicates()
        logger.info(f"Download cache initialized: {len(self._records)} records")
    
    async def shutdown(self) -> None:
        await self._save_cache()
        await self._save_duplicates()
        logger.info("Download cache saved")
    
    async def start_download(
        self,
        file_unique_id: str,
        user_id: int,
        chat_id: int,
        message_id: int,
        file_size: int,
        file_name: str = "",
        mime_type: str = ""
    ) -> DownloadRecord:
        async with self._get_lock():
            existing = self._records.get(file_unique_id)
            if existing and not existing.is_complete and not existing.is_cancelled:
                if existing.temp_path and os.path.exists(existing.temp_path):
                    existing.downloaded_bytes = os.path.getsize(existing.temp_path)
                    logger.info(f"Resuming download: {file_unique_id} at {existing.downloaded_bytes} bytes")
                    return existing
            
            temp_path = os.path.join(RESUME_DIR, f"{file_unique_id}.partial")
            
            record = DownloadRecord(
                file_unique_id=file_unique_id,
                user_id=user_id,
                chat_id=chat_id,
                message_id=message_id,
                file_size=file_size,
                file_name=file_name,
                mime_type=mime_type,
                temp_path=temp_path,
            )
            
            self._records[file_unique_id] = record
            if user_id not in self._user_records:
                self._user_records[user_id] = set()
            self._user_records[user_id].add(file_unique_id)
            self._dirty = True
            
            return record
    
    async def update_progress(self, file_unique_id: str, downloaded_bytes: int) -> None:
        record = self._records.get(file_unique_id)
        if record:
            record.downloaded_bytes = downloaded_bytes
    
    async def complete_download(self, file_unique_id: str, final_path: str) -> None:
        async with self._get_lock():
            record = self._records.get(file_unique_id)
            if record:
                record.is_complete = True
                record.completed_at = time.time()
                record.final_path = final_path
                record.downloaded_bytes = record.file_size
                
                if record.temp_path and os.path.exists(record.temp_path):
                    try:
                        os.remove(record.temp_path)
                    except:
                        pass
                record.temp_path = None
                
                if final_path and os.path.exists(final_path):
                    record.content_hash = await self._calculate_content_hash(final_path)
                    if record.content_hash:
                        self._content_hashes[record.content_hash] = file_unique_id
                
                self._dirty = True
                await self._save_cache()
    
    async def cancel_download(self, file_unique_id: str) -> None:
        async with self._get_lock():
            record = self._records.get(file_unique_id)
            if record:
                record.is_cancelled = True
                
                if record.temp_path and os.path.exists(record.temp_path):
                    try:
                        os.remove(record.temp_path)
                    except:
                        pass
                
                self._dirty = True
    
    def get_resume_offset(self, file_unique_id: str) -> int:
        record = self._records.get(file_unique_id)
        if record and record.temp_path and os.path.exists(record.temp_path):
            return os.path.getsize(record.temp_path)
        return 0
    
    async def check_duplicate(
        self,
        file_unique_id: str,
        content_hash: Optional[str] = None
    ) -> Optional[str]:
        existing = self._records.get(file_unique_id)
        if existing and existing.is_complete:
            return file_unique_id
        
        if content_hash and content_hash in self._content_hashes:
            return self._content_hashes[content_hash]
        
        return None
    
    async def is_duplicate_for_user(self, user_id: int, file_unique_id: str) -> bool:
        user_files = self._user_records.get(user_id, set())
        if file_unique_id in user_files:
            record = self._records.get(file_unique_id)
            return record and record.is_complete
        return False
    
    async def _calculate_content_hash(self, file_path: str) -> Optional[str]:
        try:
            hasher = hashlib.md5()
            async with aiofiles.open(file_path, 'rb') as f:
                chunk = await f.read(HASH_CHUNK_SIZE)
                hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            logger.warning(f"Could not hash file: {e}")
            return None
    
    async def _save_cache(self) -> None:
        if not self._dirty:
            return
        
        try:
            cache_path = os.path.join(CACHE_DIR, CACHE_FILE)
            data = {
                'records': {
                    fid: record.to_dict()
                    for fid, record in self._records.items()
                    if record.is_complete or (record.temp_path and os.path.exists(record.temp_path))
                },
                'saved_at': time.time(),
            }
            
            async with aiofiles.open(cache_path + '.tmp', 'w') as f:
                await f.write(json.dumps(data, indent=2))
            os.replace(cache_path + '.tmp', cache_path)
            
            self._dirty = False
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    async def _load_cache(self) -> None:
        try:
            cache_path = os.path.join(CACHE_DIR, CACHE_FILE)
            if not os.path.exists(cache_path):
                return
            
            async with aiofiles.open(cache_path, 'r') as f:
                content = await f.read()
                data = json.loads(content)
            
            for fid, record_data in data.get('records', {}).items():
                record = DownloadRecord.from_dict(record_data)
                self._records[fid] = record
                
                if record.user_id not in self._user_records:
                    self._user_records[record.user_id] = set()
                self._user_records[record.user_id].add(fid)
                
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")
    
    async def _save_duplicates(self) -> None:
        try:
            path = os.path.join(CACHE_DIR, DUPLICATE_CACHE_FILE)
            async with aiofiles.open(path + '.tmp', 'w') as f:
                await f.write(json.dumps(self._content_hashes, indent=2))
            os.replace(path + '.tmp', path)
        except Exception as e:
            logger.error(f"Failed to save duplicates: {e}")
    
    async def _load_duplicates(self) -> None:
        try:
            path = os.path.join(CACHE_DIR, DUPLICATE_CACHE_FILE)
            if os.path.exists(path):
                async with aiofiles.open(path, 'r') as f:
                    content = await f.read()
                    self._content_hashes = json.loads(content)
        except Exception as e:
            logger.error(f"Failed to load duplicates: {e}")
    
    async def cleanup_user(self, user_id: int) -> int:
        count = 0
        async with self._get_lock():
            file_ids = list(self._user_records.get(user_id, set()))
            
            for fid in file_ids:
                record = self._records.get(fid)
                if record:
                    if record.temp_path and os.path.exists(record.temp_path):
                        try:
                            os.remove(record.temp_path)
                        except:
                            pass
                    
                    del self._records[fid]
                    count += 1
            
            if user_id in self._user_records:
                del self._user_records[user_id]
            
            self._dirty = True
        return count
    
    async def cleanup_expired(self, max_age_hours: int = 24) -> int:
        count = 0
        async with self._get_lock():
            cutoff = time.time() - (max_age_hours * 3600)
            
            for fid, record in list(self._records.items()):
                if not record.is_complete and record.started_at < cutoff:
                    if record.temp_path and os.path.exists(record.temp_path):
                        try:
                            os.remove(record.temp_path)
                        except:
                            pass
                    
                    del self._records[fid]
                    if record.user_id in self._user_records:
                        self._user_records[record.user_id].discard(fid)
                    count += 1
            
            if count > 0:
                self._dirty = True
                await self._save_cache()
        
        return count


download_cache = DownloadCache()
