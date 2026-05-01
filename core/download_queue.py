"""
Fixed Worker Pool Download Queue

Features:
- Exactly 150 concurrent workers (no scaling)
- Priority queue with user fairness
- Per-user rate limiting
- Clean cancellation support
- Resume state persistence
"""

import asyncio
import time
import json
import os
from dataclasses import dataclass, field, asdict
from typing import Dict, Set, Optional, Callable, List
from enum import Enum, auto
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

WORKER_COUNT = 150
MAX_CONCURRENT_PER_USER = 10
QUEUE_PERSIST_FILE = "data/queue_state.json"
USER_RATE_LIMIT_DELAY = 0.5


class DownloadPriority(Enum):
    HIGH = 1
    NORMAL = 2
    LOW = 3


class DownloadStatus(Enum):
    QUEUED = auto()
    DOWNLOADING = auto()
    UPLOADING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass
class DownloadJob:
    job_id: str
    user_id: int
    chat_id: int
    message_id: int
    
    priority: DownloadPriority = DownloadPriority.NORMAL
    status: DownloadStatus = DownloadStatus.QUEUED
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    
    file_size: int = 0
    downloaded_bytes: int = 0
    uploaded_bytes: int = 0
    
    temp_file_path: Optional[str] = None
    resume_offset: int = 0
    
    on_progress: Optional[Callable] = field(default=None, repr=False)
    on_complete: Optional[Callable] = field(default=None, repr=False)
    on_error: Optional[Callable] = field(default=None, repr=False)
    
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    
    def to_dict(self) -> dict:
        return {
            'job_id': self.job_id,
            'user_id': self.user_id,
            'chat_id': self.chat_id,
            'message_id': self.message_id,
            'priority': self.priority.value,
            'status': self.status.name,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'file_size': self.file_size,
            'downloaded_bytes': self.downloaded_bytes,
            'temp_file_path': self.temp_file_path,
            'resume_offset': self.resume_offset,
            'error_message': self.error_message,
            'retry_count': self.retry_count,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'DownloadJob':
        return cls(
            job_id=data['job_id'],
            user_id=data['user_id'],
            chat_id=data['chat_id'],
            message_id=data['message_id'],
            priority=DownloadPriority(data.get('priority', 2)),
            status=DownloadStatus[data.get('status', 'QUEUED')],
            created_at=data.get('created_at', time.time()),
            started_at=data.get('started_at'),
            file_size=data.get('file_size', 0),
            downloaded_bytes=data.get('downloaded_bytes', 0),
            temp_file_path=data.get('temp_file_path'),
            resume_offset=data.get('resume_offset', 0),
            error_message=data.get('error_message'),
            retry_count=data.get('retry_count', 0),
        )


class DownloadQueue:
    def __init__(self):
        self._jobs: Dict[str, DownloadJob] = {}
        self._user_jobs: Dict[int, Set[str]] = defaultdict(set)
        self._active_jobs: Set[str] = set()
        
        self._workers: List[asyncio.Task] = []
        self._job_available = asyncio.Event()
        self._shutdown_event = asyncio.Event()
        
        self._user_last_start: Dict[int, float] = {}
        
        self._lock: asyncio.Lock = None
        self._lock_loop_id: int = None
        
        self._stats = {
            'total_queued': 0,
            'total_completed': 0,
            'total_failed': 0,
            'total_cancelled': 0,
        }
    
    def _get_lock(self) -> asyncio.Lock:
        try:
            current_loop_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return asyncio.Lock()
        
        if self._lock_loop_id != current_loop_id:
            self._lock = asyncio.Lock()
            self._lock_loop_id = current_loop_id
        return self._lock
    
    async def start(self, worker_handler: Callable) -> None:
        logger.info(f"Starting download queue with {WORKER_COUNT} workers")
        
        await self._load_state()
        
        for i in range(WORKER_COUNT):
            worker = asyncio.create_task(
                self._worker_loop(i, worker_handler),
                name=f"download_worker_{i}"
            )
            self._workers.append(worker)
        
        logger.info(f"Download queue started with {len(self._workers)} workers")
    
    async def stop(self) -> None:
        logger.info("Stopping download queue...")
        
        self._shutdown_event.set()
        self._job_available.set()
        
        if self._workers:
            done, pending = await asyncio.wait(
                self._workers, 
                timeout=30.0,
                return_when=asyncio.ALL_COMPLETED
            )
            
            for task in pending:
                task.cancel()
        
        await self._save_state()
        logger.info("Download queue stopped")
    
    async def _worker_loop(self, worker_id: int, handler: Callable) -> None:
        logger.debug(f"Worker {worker_id} started")
        
        while not self._shutdown_event.is_set():
            try:
                await self._job_available.wait()
                
                if self._shutdown_event.is_set():
                    break
                
                job = await self._get_next_job()
                
                if job is None:
                    self._job_available.clear()
                    continue
                
                try:
                    job.status = DownloadStatus.DOWNLOADING
                    job.started_at = time.time()
                    
                    success = await handler(job)
                    
                    if success:
                        job.status = DownloadStatus.COMPLETED
                        job.completed_at = time.time()
                        self._stats['total_completed'] += 1
                    else:
                        job.status = DownloadStatus.FAILED
                        self._stats['total_failed'] += 1
                        
                except asyncio.CancelledError:
                    job.status = DownloadStatus.CANCELLED
                    self._stats['total_cancelled'] += 1
                    raise
                    
                except Exception as e:
                    logger.error(f"Worker {worker_id} error: {e}")
                    job.status = DownloadStatus.FAILED
                    job.error_message = str(e)
                    self._stats['total_failed'] += 1
                    
                finally:
                    async with self._get_lock():
                        self._active_jobs.discard(job.job_id)
                    
            except asyncio.CancelledError:
                logger.debug(f"Worker {worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} unexpected error: {e}")
                await asyncio.sleep(1)
        
        logger.debug(f"Worker {worker_id} stopped")
    
    async def _get_next_job(self) -> Optional[DownloadJob]:
        async with self._get_lock():
            queued = [
                job for job in self._jobs.values()
                if job.status == DownloadStatus.QUEUED
                and job.job_id not in self._active_jobs
            ]
            
            if not queued:
                return None
            
            queued.sort(key=lambda j: (j.priority.value, j.created_at))
            
            now = time.time()
            for job in queued:
                user_id = job.user_id
                
                user_active = sum(
                    1 for jid in self._active_jobs
                    if self._jobs.get(jid) and self._jobs[jid].user_id == user_id
                )
                if user_active >= MAX_CONCURRENT_PER_USER:
                    continue
                
                last_start = self._user_last_start.get(user_id, 0)
                if now - last_start < USER_RATE_LIMIT_DELAY:
                    continue
                
                self._active_jobs.add(job.job_id)
                self._user_last_start[user_id] = now
                return job
            
            return None
    
    async def add_job(self, job: DownloadJob) -> str:
        async with self._get_lock():
            self._jobs[job.job_id] = job
            self._user_jobs[job.user_id].add(job.job_id)
            self._stats['total_queued'] += 1
        
        self._job_available.set()
        logger.debug(f"Job {job.job_id} added to queue")
        return job.job_id
    
    async def cancel_job(self, job_id: str) -> bool:
        async with self._get_lock():
            job = self._jobs.get(job_id)
            if not job:
                return False
            
            if job.status in (DownloadStatus.COMPLETED, DownloadStatus.CANCELLED):
                return False
            
            job.status = DownloadStatus.CANCELLED
            self._active_jobs.discard(job_id)
            self._stats['total_cancelled'] += 1
            return True
    
    async def cancel_user_jobs(self, user_id: int) -> int:
        cancelled = 0
        async with self._get_lock():
            job_ids = list(self._user_jobs.get(user_id, set()))
            for job_id in job_ids:
                job = self._jobs.get(job_id)
                if job and job.status in (DownloadStatus.QUEUED, DownloadStatus.DOWNLOADING):
                    job.status = DownloadStatus.CANCELLED
                    self._active_jobs.discard(job_id)
                    cancelled += 1
        
        self._stats['total_cancelled'] += cancelled
        return cancelled
    
    async def get_user_queue_position(self, user_id: int) -> Dict[str, int]:
        async with self._get_lock():
            user_job_ids = self._user_jobs.get(user_id, set())
            
            queued = sum(
                1 for jid in user_job_ids
                if self._jobs.get(jid) and self._jobs[jid].status == DownloadStatus.QUEUED
            )
            active = sum(
                1 for jid in user_job_ids
                if self._jobs.get(jid) and self._jobs[jid].status == DownloadStatus.DOWNLOADING
            )
            
            return {
                'queued': queued,
                'active': active,
                'total_in_queue': len([
                    j for j in self._jobs.values()
                    if j.status == DownloadStatus.QUEUED
                ]),
                'total_active': len(self._active_jobs),
            }
    
    async def _save_state(self) -> None:
        try:
            os.makedirs(os.path.dirname(QUEUE_PERSIST_FILE), exist_ok=True)
            
            state = {
                'jobs': [
                    job.to_dict() for job in self._jobs.values()
                    if job.status in (DownloadStatus.QUEUED, DownloadStatus.DOWNLOADING)
                    and job.temp_file_path
                ],
                'stats': self._stats,
                'saved_at': time.time(),
            }
            
            with open(QUEUE_PERSIST_FILE + '.tmp', 'w') as f:
                json.dump(state, f, indent=2)
            os.replace(QUEUE_PERSIST_FILE + '.tmp', QUEUE_PERSIST_FILE)
            
            logger.info(f"Queue state saved: {len(state['jobs'])} resumable jobs")
            
        except Exception as e:
            logger.error(f"Failed to save queue state: {e}")
    
    async def _load_state(self) -> None:
        try:
            if not os.path.exists(QUEUE_PERSIST_FILE):
                return
            
            with open(QUEUE_PERSIST_FILE, 'r') as f:
                state = json.load(f)
            
            for job_data in state.get('jobs', []):
                job = DownloadJob.from_dict(job_data)
                
                if job.temp_file_path and os.path.exists(job.temp_file_path):
                    job.status = DownloadStatus.QUEUED
                    job.resume_offset = job.downloaded_bytes
                    self._jobs[job.job_id] = job
                    self._user_jobs[job.user_id].add(job.job_id)
            
            logger.info(f"Queue state loaded: {len(self._jobs)} jobs restored")
            
        except Exception as e:
            logger.error(f"Failed to load queue state: {e}")
    
    def get_stats(self) -> dict:
        return {
            **self._stats,
            'active_workers': sum(1 for w in self._workers if not w.done()),
            'active_jobs': len(self._active_jobs),
            'queued_jobs': sum(
                1 for j in self._jobs.values()
                if j.status == DownloadStatus.QUEUED
            ),
        }


download_queue = DownloadQueue()
