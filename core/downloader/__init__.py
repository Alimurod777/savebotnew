"""
core/downloader - Telegram Media Download Engine

Public API (Legacy):
    from core.downloader import DownloadEngine, get_engine
    
    engine = DownloadEngine(workers=150, adaptive=True)
    await engine.start()
    
    file_path = await engine.download(message, client, progress_callback)
    
    await engine.shutdown()

Public API (Production - Adaptive):
    from core.downloader import AdaptiveDownloadEngine, get_adaptive_engine
    
    engine = await get_adaptive_engine(client)
    result = await engine.download(message, progress_callback=callback)
    
    # Strategies based on file size:
    # - <= 100 MB: RAM-ONLY (single disk write)
    # - 100-500 MB: RAM-FIRST (sparse checkpoints)
    # - >= 500 MB: RESUME-REQUIRED (mandatory resume)

For progress callbacks:
    callback = engine.create_progress_callback(client, status_msg, "upload")
    await client.send_video(..., progress=callback)
"""

from core.downloader.engine import (
    DownloadEngine,
    get_engine,
    shutdown_engine,
)
from core.downloader.worker import (
    WorkerPool,
    DownloadTask,
    TaskStatus,
)
from core.downloader.progress import (
    ProgressTracker,
    progress_tracker,
)
from core.downloader.progress_v2 import (
    ProgressState,
    ProgressPublisher,
    ProgressManager,
    progress_manager,
    TransferStatus,
)
from core.downloader.resume import (
    ResumableDownloader,
    ResumeDownloadManager,
    ResumeState,
    download_with_resume,
)
from core.downloader.adaptive_engine import (
    AdaptiveDownloadEngine,
    AdaptiveTask,
    DownloadMode,
    DownloadResult as AdaptiveDownloadResult,
    RAMBudget,
    ResumeMetadata,
    get_adaptive_engine,
    shutdown_adaptive_engine,
)
from core.downloader.session_safe_worker import (
    SessionSafeWorkerPool,
    DownloadJob,
    WorkerState,
    HealthStatus,
)
from core.downloader.integration import (
    AdaptiveTransferBridge,
    TransferResult,
    TransferStatus,
    get_transfer_bridge,
    shutdown_transfer_bridge,
    adaptive_download,
)

__all__ = [
    # Legacy Engine
    'DownloadEngine',
    'get_engine',
    'shutdown_engine',
    
    # Production Adaptive Engine
    'AdaptiveDownloadEngine',
    'AdaptiveTask',
    'DownloadMode',
    'AdaptiveDownloadResult',
    'RAMBudget',
    'ResumeMetadata',
    'get_adaptive_engine',
    'shutdown_adaptive_engine',
    
    # Session-Safe Worker Pool
    'SessionSafeWorkerPool',
    'DownloadJob',
    'WorkerState',
    'HealthStatus',
    
    # TechVJ Integration (drop-in replacement)
    'AdaptiveTransferBridge',
    'TransferResult',
    'TransferStatus',
    'get_transfer_bridge',
    'shutdown_transfer_bridge',
    'adaptive_download',
    
    # Legacy Worker
    'WorkerPool',
    'DownloadTask',
    'TaskStatus',
    
    # Progress (legacy)
    'ProgressTracker',
    'progress_tracker',
    
    # Progress V2 (production)
    'ProgressState',
    'ProgressPublisher',
    'ProgressManager',
    'progress_manager',
    'TransferStatus',
    
    # Resume
    'ResumableDownloader',
    'ResumeDownloadManager',
    'ResumeState',
    'download_with_resume',
]
