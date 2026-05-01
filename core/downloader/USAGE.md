# Adaptive Download Engine - Usage Guide

## Overview

The Adaptive Download Engine is a production-grade media downloader for Telegram with:
- **Session stability first** - never starves Pyrogram PingTask
- **Low I/O** - minimizes disk writes based on file size
- **RAM-aware** - enforces global memory budget
- **Resumable** - only when strictly required

## Size-Based Strategies

| File Size | Mode | RAM Usage | Disk I/O | Resume |
|-----------|------|-----------|----------|--------|
| ≤ 100 MB | RAM-ONLY | Full file | 1 write | Disabled |
| 100-500 MB | RAM-FIRST | 8-16 MB buffer | Sparse checkpoints | On interrupt |
| ≥ 500 MB | RESUME-REQUIRED | 16 MB buffer | Buffered writes | Mandatory |

## Quick Start

```python
from pyrogram import Client
from core.downloader import get_adaptive_engine

# Get or create global engine
engine = await get_adaptive_engine(user_client)

# Download with automatic strategy selection
result = await engine.download(
    message=msg,
    progress_callback=lambda cur, tot: print(f"{cur}/{tot}"),
    user_id=12345,
    dest_dir="downloads/"
)

if result.success:
    print(f"Downloaded: {result.file_path}")
    print(f"Mode used: {result.mode.value}")
else:
    print(f"Failed: {result.error}")
```

## Integration with Existing Code

```python
from core.downloader import (
    AdaptiveDownloadEngine,
    SessionSafeWorkerPool,
    DownloadMode,
    RAMBudget
)

class MyDownloadHandler:
    def __init__(self, client):
        self.client = client
        self.engine = AdaptiveDownloadEngine(
            client=client,
            download_dir="downloads/temp",
            max_workers=8,
            min_workers=2
        )
        self.worker_pool = SessionSafeWorkerPool(
            initial_workers=4,
            per_user_limit=3
        )
    
    async def start(self):
        await self.engine.start()
        await self.worker_pool.start()
    
    async def download_file(self, message, user_id):
        # Submit to session-safe worker pool
        result = await self.worker_pool.submit(
            job_id=f"dl_{message.id}",
            user_id=user_id,
            handler=self.engine.download,
            kwargs={
                "message": message,
                "user_id": user_id
            }
        )
        return result
    
    async def shutdown(self):
        await self.worker_pool.shutdown()
        await self.engine.shutdown()
```

## RAM Budget

The engine automatically calculates RAM budget based on system memory:

| System RAM | Download Budget |
|------------|-----------------|
| ≤ 2 GB | 128 MB |
| ≤ 4 GB | 256 MB |
| > 4 GB | 512 MB |

Check current usage:
```python
from core.downloader import RAMBudget

budget = RAMBudget.get_instance()
print(f"Budget: {budget.max_bytes / 1024 / 1024:.0f} MB")
print(f"Used: {budget.used_bytes / 1024 / 1024:.0f} MB")
print(f"Pressure: {budget.pressure:.1%}")
```

## Worker Pool Health

The session-safe worker pool monitors:
- **Disk latency** (scales down if > 100ms)
- **Network RTT** (scales down if > 500ms)
- **Queue depth** (scales up/down based on load)

```python
pool = SessionSafeWorkerPool()
await pool.start()

# Record metrics for adaptive scaling
pool.record_disk_latency(latency_ms)
pool.record_network_rtt(rtt_ms)

# Check health
stats = pool.get_stats()
print(f"Health: {stats['health']}")
print(f"Workers: {stats['workers_active']}/{stats['workers_total']}")
```

## Error Handling

The engine handles these errors gracefully:
- `FloodWait` - waits and retries
- `FileReferenceExpired` - returns for caller to refresh
- `Timeout` - exponential backoff retry
- `ConnectionError` - retry with saved state

```python
result = await engine.download(message)

if not result.success:
    if result.error == "FILE_REFERENCE_EXPIRED":
        # Refresh message and retry
        fresh_msg = await client.get_messages(chat_id, msg_id)
        result = await engine.download(fresh_msg)
    elif "Cancelled" in result.error:
        # User cancelled
        pass
    else:
        # Log error
        logger.error(f"Download failed: {result.error}")
```

## Best Practices

1. **One engine per client** - Don't create multiple engines for same client
2. **Use worker pool** - For concurrent downloads, use `SessionSafeWorkerPool`
3. **Monitor health** - Log `get_stats()` periodically
4. **Handle file references** - Always handle `FILE_REFERENCE_EXPIRED`
5. **Respect RAM budget** - Don't bypass budget checks

## Forbidden Patterns

❌ Writing to disk on every chunk  
❌ Resume for files ≤ 100 MB  
❌ Aggressive retry loops  
❌ Worker oversubscription (>16 workers)  
❌ Session starvation  
❌ Assuming uninterrupted network  
