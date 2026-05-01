"""
Event-Loop-Safe /clean Command

ARCHITECTURAL DESIGN:
=====================
This module replaces ALL scheduled background cleanup tasks with an explicit
owner-only /clean command that runs cleanup deterministically in the SAME
event loop as the bot.

WHY THIS IS SAFE:
-----------------
1. NO background tasks - cleanup runs only when /clean is invoked
2. NO threads - all async operations run in the current event loop
3. NO import-time side effects - nothing is created at module load
4. NO global asyncio primitives - locks/semaphores created lazily
5. IDEMPOTENT - safe to run multiple times, safe after crashes

CLEANUP SCOPE:
--------------
- Temporary directories (downloads/temp/*)
- Orphaned session files (sessions/*.session except bot session)
- In-memory task registries and caches
- Per-user runtime state
- Expired resources tracked by cleanup_manager

INTEGRATION:
------------
Import this module in TechVJ/__init__.py or rely on Pyrogram's plugin auto-discovery.
The handler is automatically registered via @Client.on_message decorator.
"""

import asyncio
import os
import shutil
import logging
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Any

from pyrogram import Client, filters
from pyrogram.types import Message

from config import OWNER_ID, TEMP_DOWNLOAD_DIR

logger = logging.getLogger(__name__)

# Base paths for cleanup
TEMP_BASE_DIR = "downloads/temp"
SESSIONS_DIR = "sessions"
BOT_SESSION_NAME = "techvj_login"  # Do NOT delete the bot's own session


async def cleanup_temp_directories() -> Tuple[int, int, List[str]]:
    """
    Clean all temporary directories.
    
    Returns:
        Tuple of (directories_cleaned, bytes_freed, details)
    """
    cleaned = 0
    bytes_freed = 0
    details = []
    
    if not os.path.exists(TEMP_BASE_DIR):
        return 0, 0, ["Temp directory does not exist"]
    
    try:
        for dirname in os.listdir(TEMP_BASE_DIR):
            dir_path = os.path.join(TEMP_BASE_DIR, dirname)
            if os.path.isdir(dir_path):
                try:
                    # Calculate size before deletion
                    dir_size = sum(
                        os.path.getsize(os.path.join(dirpath, filename))
                        for dirpath, _, filenames in os.walk(dir_path)
                        for filename in filenames
                    )
                    shutil.rmtree(dir_path)
                    cleaned += 1
                    bytes_freed += dir_size
                    details.append(f"Removed: {dirname} ({dir_size / 1024:.1f} KB)")
                except Exception as e:
                    details.append(f"Failed: {dirname} - {e}")
    except Exception as e:
        details.append(f"Error scanning temp dir: {e}")
    
    return cleaned, bytes_freed, details


async def cleanup_orphaned_sessions() -> Tuple[int, List[str]]:
    """
    Clean orphaned session files (*.session files not belonging to bot).
    
    SAFETY: Never deletes the bot's own session file.
    
    Returns:
        Tuple of (files_cleaned, details)
    """
    cleaned = 0
    details = []
    
    if not os.path.exists(SESSIONS_DIR):
        return 0, ["Sessions directory does not exist"]
    
    try:
        for filename in os.listdir(SESSIONS_DIR):
            # Skip bot's own session
            if filename.startswith(BOT_SESSION_NAME):
                details.append(f"Preserved: {filename} (bot session)")
                continue
            
            # Only clean .session and .session-journal files
            if filename.endswith('.session') or filename.endswith('.session-journal'):
                file_path = os.path.join(SESSIONS_DIR, filename)
                try:
                    # Check if file is old (not modified in last 10 minutes)
                    mtime = os.path.getmtime(file_path)
                    age = datetime.now().timestamp() - mtime
                    
                    if age > 600:  # 10 minutes
                        os.remove(file_path)
                        cleaned += 1
                        details.append(f"Removed: {filename} (age: {age/60:.0f} min)")
                    else:
                        details.append(f"Skipped: {filename} (recently modified)")
                except Exception as e:
                    details.append(f"Failed: {filename} - {e}")
    except Exception as e:
        details.append(f"Error scanning sessions: {e}")
    
    return cleaned, details


async def cleanup_task_manager() -> Tuple[int, List[str]]:
    """
    Clean in-memory task registries and caches.
    
    Returns:
        Tuple of (users_cleaned, details)
    """
    cleaned = 0
    details = []
    
    try:
        from TechVJ.task_manager import task_manager
        
        # Get all user contexts
        users_with_contexts = list(task_manager._user_contexts.keys())
        
        for user_id in users_with_contexts:
            context = task_manager._user_contexts.get(user_id)
            if not context:
                continue
            
            # Only clean users with no active tasks
            active_tasks = [t for t in context.tasks if not t.done()]
            if not active_tasks:
                # Clean temp directory
                if context.temp_dir and os.path.exists(context.temp_dir):
                    try:
                        shutil.rmtree(context.temp_dir)
                        details.append(f"User {user_id}: temp dir cleaned")
                    except Exception as e:
                        details.append(f"User {user_id}: temp dir error - {e}")
                
                # Clear album tracking
                albums_count = len(context.processed_albums)
                context.processed_albums.clear()
                context.temp_dir = None
                cleaned += 1
                details.append(f"User {user_id}: cleared {albums_count} albums")
            else:
                details.append(f"User {user_id}: skipped ({len(active_tasks)} active tasks)")
    except ImportError:
        details.append("Task manager not available")
    except Exception as e:
        details.append(f"Task manager error: {e}")
    
    return cleaned, details


async def cleanup_state_manager() -> Tuple[int, List[str]]:
    """
    Clean per-user runtime state from state manager.
    
    Returns:
        Tuple of (users_cleaned, details)
    """
    cleaned = 0
    details = []
    
    try:
        from database.state_manager import state_manager
        
        # Sync cache to MongoDB before cleanup
        try:
            await state_manager.sync_to_mongodb()
            details.append("State synced to MongoDB")
        except Exception as e:
            details.append(f"MongoDB sync warning: {e}")
        
        # Clear local cache for inactive users
        if hasattr(state_manager, '_local_cache'):
            cache_size = len(state_manager._local_cache)
            # Don't clear active users
            active_users = set()
            try:
                from TechVJ.task_manager import task_manager
                active_users = task_manager.get_active_users()
            except:
                pass
            
            for user_id in list(state_manager._local_cache.keys()):
                if user_id not in active_users:
                    del state_manager._local_cache[user_id]
                    cleaned += 1
            
            details.append(f"Cache cleaned: {cleaned}/{cache_size} users")
    except ImportError:
        details.append("State manager not available")
    except Exception as e:
        details.append(f"State manager error: {e}")
    
    return cleaned, details


async def cleanup_cleanup_manager() -> Tuple[int, List[str]]:
    """
    Clean expired resources from cleanup_manager.
    
    Returns:
        Tuple of (resources_cleaned, details)
    """
    cleaned = 0
    details = []
    
    try:
        from TechVJ.cleanup_manager import cleanup_manager
        
        # Get stats before cleanup
        stats_before = await cleanup_manager.get_stats()
        details.append(f"Before: {stats_before['total_resources']} resources, {stats_before['users_with_resources']} users")
        
        # Clean expired resources
        expired_cleaned = await cleanup_manager.cleanup_expired()
        cleaned += expired_cleaned
        details.append(f"Expired resources cleaned: {expired_cleaned}")
        
        # Get stats after
        stats_after = await cleanup_manager.get_stats()
        details.append(f"After: {stats_after['total_resources']} resources, {stats_after['users_with_resources']} users")
    except ImportError:
        details.append("Cleanup manager not available")
    except Exception as e:
        details.append(f"Cleanup manager error: {e}")
    
    return cleaned, details


async def cleanup_session_validation_cache() -> Tuple[int, List[str]]:
    """
    Clear session validation cache from session_manager.
    
    Returns:
        Tuple of (entries_cleared, details)
    """
    cleaned = 0
    details = []
    
    try:
        from database.session_manager import session_manager
        
        cache_size = len(session_manager._validation_cache)
        session_manager.clear_cache()
        cleaned = cache_size
        details.append(f"Validation cache cleared: {cache_size} entries")
    except ImportError:
        details.append("Session manager not available")
    except Exception as e:
        details.append(f"Session manager error: {e}")
    
    return cleaned, details


async def perform_full_cleanup() -> Dict[str, Any]:
    """
    Perform full cleanup across all subsystems.
    
    This is the main cleanup function called by /clean command.
    All operations run in the CURRENT event loop - no threads, no secondary loops.
    
    Returns:
        Dictionary with cleanup results for each subsystem
    """
    start_time = datetime.now()
    results = {
        'timestamp': start_time.isoformat(),
        'temp_dirs': {},
        'sessions': {},
        'task_manager': {},
        'state_manager': {},
        'cleanup_manager': {},
        'validation_cache': {},
        'summary': {}
    }
    
    # 1. Clean temporary directories
    dirs_cleaned, bytes_freed, dir_details = await cleanup_temp_directories()
    results['temp_dirs'] = {
        'cleaned': dirs_cleaned,
        'bytes_freed': bytes_freed,
        'details': dir_details
    }
    
    # 2. Clean orphaned session files
    sessions_cleaned, session_details = await cleanup_orphaned_sessions()
    results['sessions'] = {
        'cleaned': sessions_cleaned,
        'details': session_details
    }
    
    # 3. Clean task manager
    tasks_cleaned, task_details = await cleanup_task_manager()
    results['task_manager'] = {
        'cleaned': tasks_cleaned,
        'details': task_details
    }
    
    # 4. Clean state manager
    states_cleaned, state_details = await cleanup_state_manager()
    results['state_manager'] = {
        'cleaned': states_cleaned,
        'details': state_details
    }
    
    # 5. Clean cleanup_manager expired resources
    resources_cleaned, resource_details = await cleanup_cleanup_manager()
    results['cleanup_manager'] = {
        'cleaned': resources_cleaned,
        'details': resource_details
    }
    
    # 6. Clear validation cache
    cache_cleaned, cache_details = await cleanup_session_validation_cache()
    results['validation_cache'] = {
        'cleaned': cache_cleaned,
        'details': cache_details
    }
    
    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    results['summary'] = {
        'total_dirs_cleaned': dirs_cleaned,
        'total_bytes_freed': bytes_freed,
        'total_sessions_cleaned': sessions_cleaned,
        'total_users_cleaned': tasks_cleaned + states_cleaned,
        'total_resources_cleaned': resources_cleaned,
        'total_cache_cleared': cache_cleaned,
        'elapsed_seconds': elapsed
    }
    
    return results


def format_cleanup_report(results: Dict[str, Any]) -> str:
    """Format cleanup results for Telegram message."""
    summary = results['summary']
    
    report = [
        "🧹 **Cleanup Complete**",
        "",
        f"⏱ Duration: {summary['elapsed_seconds']:.2f}s",
        "",
        "**Results:**",
        f"📁 Temp dirs: {summary['total_dirs_cleaned']} ({summary['total_bytes_freed'] / 1024 / 1024:.2f} MB)",
        f"🔑 Sessions: {summary['total_sessions_cleaned']} orphaned files",
        f"👤 Users: {summary['total_users_cleaned']} caches cleared",
        f"📦 Resources: {summary['total_resources_cleaned']} expired",
        f"💾 Cache: {summary['total_cache_cleared']} validation entries",
    ]
    
    return "\n".join(report)


# =============================================================================
# PYROGRAM COMMAND HANDLER
# =============================================================================

@Client.on_message(filters.command("clean") & filters.private)
async def clean_command_handler(client: Client, message: Message):
    """
    /clean - Owner-only cleanup command.
    
    EVENT LOOP SAFETY:
    - This handler runs in the same event loop as the bot
    - All cleanup operations are awaited directly
    - No threads, no run_in_executor, no asyncio.run()
    - Safe across restarts and redeployments
    """
    user_id = message.from_user.id
    
    # STRICT OWNER CHECK
    if user_id != OWNER_ID:
        logger.warning(f"Unauthorized /clean attempt by user {user_id}")
        await message.reply_text(
            "⛔ This command is restricted to the bot owner.",
            quote=True
        )
        return
    
    # Send initial status
    status_msg = await message.reply_text(
        "🧹 Starting cleanup...\n\n"
        "This may take a few seconds.",
        quote=True
    )
    
    try:
        # Run cleanup in CURRENT event loop (no threads!)
        results = await perform_full_cleanup()
        
        # Format and send report
        report = format_cleanup_report(results)
        await status_msg.edit_text(report)
        
        # Log detailed results
        logger.info(f"Cleanup completed by owner {user_id}: {results['summary']}")
        
    except Exception as e:
        error_msg = f"❌ Cleanup failed: {str(e)}"
        logger.error(f"Cleanup error: {e}", exc_info=True)
        await status_msg.edit_text(error_msg)


@Client.on_message(filters.command("cleanstatus") & filters.private)
async def clean_status_handler(client: Client, message: Message):
    """
    /cleanstatus - Check current resource status (owner only).
    """
    user_id = message.from_user.id
    
    if user_id != OWNER_ID:
        await message.reply_text("⛔ Owner only.", quote=True)
        return
    
    status_lines = ["📊 **Resource Status**", ""]
    
    # Temp directories
    try:
        if os.path.exists(TEMP_BASE_DIR):
            temp_dirs = [d for d in os.listdir(TEMP_BASE_DIR) if os.path.isdir(os.path.join(TEMP_BASE_DIR, d))]
            total_size = sum(
                sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fn in os.walk(os.path.join(TEMP_BASE_DIR, d)) for f in fn)
                for d in temp_dirs
            )
            status_lines.append(f"📁 Temp dirs: {len(temp_dirs)} ({total_size / 1024 / 1024:.2f} MB)")
        else:
            status_lines.append("📁 Temp dirs: 0")
    except Exception as e:
        status_lines.append(f"📁 Temp dirs: error - {e}")
    
    # Session files
    try:
        if os.path.exists(SESSIONS_DIR):
            session_files = [f for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]
            status_lines.append(f"🔑 Session files: {len(session_files)}")
        else:
            status_lines.append("🔑 Session files: 0")
    except Exception as e:
        status_lines.append(f"🔑 Session files: error - {e}")
    
    # Task manager
    try:
        from TechVJ.task_manager import task_manager
        active_users = task_manager.get_active_users()
        total_contexts = len(task_manager._user_contexts)
        status_lines.append(f"👤 Task contexts: {total_contexts} ({len(active_users)} active)")
    except Exception as e:
        status_lines.append(f"👤 Task contexts: error - {e}")
    
    # Cleanup manager
    try:
        from TechVJ.cleanup_manager import cleanup_manager
        stats = await cleanup_manager.get_stats()
        status_lines.append(f"📦 Tracked resources: {stats['total_resources']}")
    except Exception as e:
        status_lines.append(f"📦 Tracked resources: error - {e}")
    
    await message.reply_text("\n".join(status_lines), quote=True)
