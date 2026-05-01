"""
Per-User State Manager with MongoDB + Local Cache Fallback

Features:
- Automatic MongoDB connection monitoring
- Local cache fallback when MongoDB is unavailable
- Automatic sync on MongoDB reconnect
- Race-condition safe with async locks
- File persistence for local cache
- Full integration with album pipeline

This module provides a unified interface for state management that
seamlessly switches between MongoDB and local cache based on connectivity.
"""

import asyncio
import os
import json
import time
from datetime import datetime
from typing import Dict, Set, Optional, Any, List
from dataclasses import dataclass, field, asdict
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# Local cache directory
CACHE_DIR = "data/cache"
CACHE_FILE = "sent_albums_cache.json"
SYNC_STATE_FILE = "sync_state.json"

# MongoDB check interval
MONGO_CHECK_INTERVAL = 30  # seconds
MONGO_PING_TIMEOUT = 5  # seconds


@dataclass
class SentAlbumRecord:
    """Record of a sent album"""
    user_id: int
    media_group_id: str
    source_chat_id: Optional[int] = None
    sent_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    synced_to_mongo: bool = False
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'SentAlbumRecord':
        return cls(**data)


class LocalCache:
    """
    Thread-safe local cache with file persistence.
    Used as fallback when MongoDB is unavailable.
    """
    
    def __init__(self):
        # Structure: {user_id: {media_group_id: SentAlbumRecord}}
        self._sent_albums: Dict[int, Dict[str, SentAlbumRecord]] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._bound_loop_id: Optional[int] = None
        self._dirty = False  # Track if cache needs saving
        
        # Ensure cache directory exists
        os.makedirs(CACHE_DIR, exist_ok=True)
        
        # Load from file on init
        self._load_from_file()
    
    def _get_cache_path(self) -> str:
        return os.path.join(CACHE_DIR, CACHE_FILE)
    
    def _get_lock(self) -> asyncio.Lock:
        """Get or create lock bound to current event loop."""
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
        except RuntimeError:
            # No running loop - return fresh lock (not stored)
            return asyncio.Lock()
        
        if self._bound_loop_id is not None and self._bound_loop_id != current_loop_id:
            logger.warning("LocalCache: Event loop changed, resetting lock")
            self._lock = None
        
        self._bound_loop_id = current_loop_id
        
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
    
    def _load_from_file(self) -> None:
        """Load cache from file (sync, called once on init)"""
        cache_path = self._get_cache_path()
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r') as f:
                    data = json.load(f)
                
                for user_id_str, albums in data.items():
                    user_id = int(user_id_str)
                    self._sent_albums[user_id] = {}
                    for media_group_id, record_data in albums.items():
                        self._sent_albums[user_id][media_group_id] = SentAlbumRecord.from_dict(record_data)
                
                logger.info(f"Loaded local cache: {sum(len(a) for a in self._sent_albums.values())} albums")
            except Exception as e:
                logger.error(f"Error loading cache from file: {e}")
                self._sent_albums = {}
    
    async def save_to_file(self) -> None:
        """Save cache to file (async-safe)"""
        async with self._get_lock():
            if not self._dirty:
                return
            
            try:
                cache_path = self._get_cache_path()
                data = {}
                for user_id, albums in self._sent_albums.items():
                    data[str(user_id)] = {
                        mg_id: record.to_dict() 
                        for mg_id, record in albums.items()
                    }
                
                # Write to temp file first, then rename (atomic)
                temp_path = cache_path + ".tmp"
                with open(temp_path, 'w') as f:
                    json.dump(data, f, indent=2)
                os.replace(temp_path, cache_path)
                
                self._dirty = False
                logger.debug("Cache saved to file")
            except Exception as e:
                logger.error(f"Error saving cache to file: {e}")
    
    async def is_album_sent(self, user_id: int, media_group_id: str) -> bool:
        """Check if album was sent (async-safe)"""
        async with self._get_lock():
            if user_id not in self._sent_albums:
                return False
            return media_group_id in self._sent_albums[user_id]
    
    async def mark_album_sent(
        self, 
        user_id: int, 
        media_group_id: str, 
        source_chat_id: Optional[int] = None,
        synced: bool = False
    ) -> None:
        """Mark album as sent (async-safe)"""
        async with self._get_lock():
            if user_id not in self._sent_albums:
                self._sent_albums[user_id] = {}
            
            self._sent_albums[user_id][media_group_id] = SentAlbumRecord(
                user_id=user_id,
                media_group_id=media_group_id,
                source_chat_id=source_chat_id,
                synced_to_mongo=synced
            )
            self._dirty = True
        
        # Save to file periodically (non-blocking)
        asyncio.create_task(self.save_to_file())
    
    async def get_unsynced_records(self) -> List[SentAlbumRecord]:
        """Get all records not yet synced to MongoDB"""
        async with self._get_lock():
            unsynced = []
            for user_albums in self._sent_albums.values():
                for record in user_albums.values():
                    if not record.synced_to_mongo:
                        unsynced.append(record)
            return unsynced
    
    async def mark_as_synced(self, user_id: int, media_group_id: str) -> None:
        """Mark a record as synced to MongoDB"""
        async with self._get_lock():
            if user_id in self._sent_albums and media_group_id in self._sent_albums[user_id]:
                self._sent_albums[user_id][media_group_id].synced_to_mongo = True
                self._dirty = True
    
    async def get_user_albums(self, user_id: int) -> Set[str]:
        """Get all album IDs for a user"""
        async with self._get_lock():
            if user_id not in self._sent_albums:
                return set()
            return set(self._sent_albums[user_id].keys())
    
    async def clear_user(self, user_id: int) -> None:
        """Clear all albums for a user"""
        async with self._get_lock():
            if user_id in self._sent_albums:
                del self._sent_albums[user_id]
                self._dirty = True
    
    async def merge_from_mongo(self, user_id: int, mongo_albums: List[dict]) -> None:
        """Merge MongoDB data into local cache (for consistency)"""
        async with self._get_lock():
            if user_id not in self._sent_albums:
                self._sent_albums[user_id] = {}
            
            for album_data in mongo_albums:
                media_group_id = album_data.get('media_group_id')
                if media_group_id and media_group_id not in self._sent_albums[user_id]:
                    self._sent_albums[user_id][media_group_id] = SentAlbumRecord(
                        user_id=user_id,
                        media_group_id=media_group_id,
                        source_chat_id=album_data.get('source_chat_id'),
                        sent_at=album_data.get('sent_at', datetime.utcnow()).isoformat() 
                               if isinstance(album_data.get('sent_at'), datetime)
                               else str(album_data.get('sent_at', '')),
                        synced_to_mongo=True
                    )
            self._dirty = True


class MongoConnectionMonitor:
    """Monitors MongoDB connection status"""
    
    def __init__(self, mongo_client):
        self.mongo_client = mongo_client
        self._is_connected = False
        self._last_check = 0
        self._lock: Optional[asyncio.Lock] = None
        self._bound_loop_id: Optional[int] = None
        self._check_task: Optional[asyncio.Task] = None
        self._callbacks_on_reconnect: List[callable] = []
    
    def _get_lock(self) -> asyncio.Lock:
        """Get or create lock bound to current event loop."""
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
        except RuntimeError:
            # No running loop - return fresh lock (not stored)
            return asyncio.Lock()
        
        if self._bound_loop_id is not None and self._bound_loop_id != current_loop_id:
            logger.warning("MongoConnectionMonitor: Event loop changed, resetting lock")
            self._lock = None
        
        self._bound_loop_id = current_loop_id
        
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
    
    async def check_connection(self) -> bool:
        """Check if MongoDB is connected"""
        try:
            # Ping the server with timeout
            await asyncio.wait_for(
                self.mongo_client.admin.command('ping'),
                timeout=MONGO_PING_TIMEOUT
            )
            return True
        except Exception as e:
            logger.debug(f"MongoDB ping failed: {e}")
            return False
    
    async def get_status(self) -> bool:
        """Get current connection status (cached for performance)"""
        current_time = time.time()
        
        # Use cached status if recent
        if current_time - self._last_check < MONGO_CHECK_INTERVAL:
            return self._is_connected
        
        async with self._get_lock():
            # Double-check after acquiring lock
            if current_time - self._last_check < MONGO_CHECK_INTERVAL:
                return self._is_connected
            
            was_connected = self._is_connected
            self._is_connected = await self.check_connection()
            self._last_check = current_time
            
            # Trigger reconnect callbacks if we just reconnected
            if not was_connected and self._is_connected:
                logger.info("MongoDB reconnected!")
                for callback in self._callbacks_on_reconnect:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            asyncio.create_task(callback())
                        else:
                            callback()
                    except Exception as e:
                        logger.error(f"Error in reconnect callback: {e}")
            
            return self._is_connected
    
    def on_reconnect(self, callback: callable) -> None:
        """Register a callback to be called when MongoDB reconnects"""
        self._callbacks_on_reconnect.append(callback)
    
    async def start_monitoring(self) -> None:
        """Start background connection monitoring"""
        async def monitor_loop():
            while True:
                await self.get_status()
                await asyncio.sleep(MONGO_CHECK_INTERVAL)
        
        self._check_task = asyncio.create_task(monitor_loop())
    
    async def stop_monitoring(self) -> None:
        """Stop background monitoring"""
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass


class StateManager:
    """
    Unified state manager with MongoDB primary + local cache fallback.
    
    This is the main interface for all state operations.
    It automatically handles:
    - MongoDB connection monitoring
    - Fallback to local cache when MongoDB is unavailable
    - Automatic sync when MongoDB reconnects
    - Race-condition safe operations
    - Active user tracking for cleanup protection
    """
    
    def __init__(self):
        self._local_cache = LocalCache()
        self._mongo_monitor: Optional[MongoConnectionMonitor] = None
        self._sync_lock: Optional[asyncio.Lock] = None
        self._syncing = False
        self._initialized = False
        self._bound_loop_id: Optional[int] = None
        
        # Active users tracking (for cleanup protection)
        self._active_users: Set[int] = set()
        self._active_users_lock: Optional[asyncio.Lock] = None
        
        # Import MongoDB collections - use lazy getters
        try:
            from database.async_db import get_mongo_client, get_sent_albums_collection
            self._get_mongo_client = get_mongo_client
            self._get_sent_albums = get_sent_albums_collection
            self._mongo_available = True
        except ImportError:
            logger.warning("MongoDB not available, using local cache only")
            self._get_mongo_client = None
            self._get_sent_albums = None
            self._mongo_available = False
    
    def _check_loop_change(self) -> bool:
        """Check if event loop changed and reset locks."""
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
        except RuntimeError:
            return False
        
        if self._bound_loop_id is not None and self._bound_loop_id != current_loop_id:
            logger.warning("StateManager: Event loop changed, resetting locks")
            self._sync_lock = None
            self._active_users_lock = None
            self._bound_loop_id = current_loop_id
            return True
        
        self._bound_loop_id = current_loop_id
        return False
    
    def _get_sync_lock(self) -> asyncio.Lock:
        """Get or create sync lock bound to current event loop."""
        self._check_loop_change()
        if self._sync_lock is None:
            self._sync_lock = asyncio.Lock()
        return self._sync_lock
    
    def _get_active_users_lock(self) -> asyncio.Lock:
        """Get or create active users lock bound to current event loop."""
        self._check_loop_change()
        if self._active_users_lock is None:
            self._active_users_lock = asyncio.Lock()
        return self._active_users_lock
    
    # ==================== Active User Tracking ====================
    
    async def mark_user_active(self, user_id: int) -> None:
        """Mark user as active (protects from cleanup during operations)"""
        async with self._get_active_users_lock():
            self._active_users.add(user_id)
    
    async def mark_user_inactive(self, user_id: int) -> None:
        """Mark user as inactive"""
        async with self._get_active_users_lock():
            self._active_users.discard(user_id)
    
    def get_active_users(self) -> Set[int]:
        """Get set of currently active users (sync version for cleanup)"""
        return self._active_users.copy()
    
    async def initialize(self) -> None:
        """Initialize the state manager (call once on startup)"""
        if self._initialized:
            return
        
        if self._mongo_available and self._get_mongo_client:
            mongo_client = self._get_mongo_client()
            self._mongo_monitor = MongoConnectionMonitor(mongo_client)
            self._mongo_monitor.on_reconnect(self._on_mongo_reconnect)
            await self._mongo_monitor.start_monitoring()
            
            # Initial sync from MongoDB to local cache
            if await self._mongo_monitor.get_status():
                await self._sync_mongo_to_local()
        
        self._initialized = True
        logger.info("StateManager initialized")
    
    async def _is_mongo_available(self) -> bool:
        """Check if MongoDB is currently available"""
        if not self._mongo_available or not self._mongo_monitor:
            return False
        return await self._mongo_monitor.get_status()
    
    async def _on_mongo_reconnect(self) -> None:
        """Called when MongoDB reconnects - sync local cache to MongoDB"""
        await self._sync_local_to_mongo()
    
    async def _sync_mongo_to_local(self) -> None:
        """Sync MongoDB data to local cache (initial load)"""
        if not await self._is_mongo_available():
            return
        
        async with self._get_sync_lock():
            try:
                logger.info("Syncing MongoDB to local cache...")
                
                # Get all sent albums from MongoDB
                sent_albums_collection = self._get_sent_albums()
                cursor = sent_albums_collection.find({})
                albums = await cursor.to_list(length=None)
                
                # Group by user_id
                by_user: Dict[int, List[dict]] = {}
                for album in albums:
                    user_id = album.get('user_id')
                    if user_id:
                        if user_id not in by_user:
                            by_user[user_id] = []
                        by_user[user_id].append(album)
                
                # Merge into local cache
                for user_id, user_albums in by_user.items():
                    await self._local_cache.merge_from_mongo(user_id, user_albums)
                
                await self._local_cache.save_to_file()
                logger.info(f"Synced {len(albums)} albums from MongoDB to local cache")
                
            except Exception as e:
                logger.error(f"Error syncing MongoDB to local: {e}")
    
    async def _sync_local_to_mongo(self) -> None:
        """Sync local cache to MongoDB (on reconnect)"""
        if not await self._is_mongo_available():
            return
        
        async with self._get_sync_lock():
            if self._syncing:
                return
            self._syncing = True
            
            try:
                logger.info("Syncing local cache to MongoDB...")
                
                # Get unsynced records
                unsynced = await self._local_cache.get_unsynced_records()
                
                if not unsynced:
                    logger.info("No unsynced records to sync")
                    return
                
                sent_albums_collection = self._get_sent_albums()
                synced_count = 0
                for record in unsynced:
                    try:
                        # Check if already exists in MongoDB (don't overwrite)
                        existing = await sent_albums_collection.find_one({
                            'user_id': record.user_id,
                            'media_group_id': record.media_group_id
                        })
                        
                        if existing:
                            # Already in MongoDB, just mark as synced locally
                            await self._local_cache.mark_as_synced(
                                record.user_id, record.media_group_id
                            )
                        else:
                            # Insert new record to MongoDB
                            await sent_albums_collection.update_one(
                                {
                                    'user_id': record.user_id,
                                    'media_group_id': record.media_group_id
                                },
                                {'$set': {
                                    'user_id': record.user_id,
                                    'media_group_id': record.media_group_id,
                                    'source_chat_id': record.source_chat_id,
                                    'sent_at': datetime.fromisoformat(record.sent_at) 
                                              if record.sent_at else datetime.utcnow(),
                                    'synced_from_local': True
                                }},
                                upsert=True
                            )
                            await self._local_cache.mark_as_synced(
                                record.user_id, record.media_group_id
                            )
                        
                        synced_count += 1
                        
                    except Exception as e:
                        logger.error(f"Error syncing record: {e}")
                
                await self._local_cache.save_to_file()
                logger.info(f"Synced {synced_count}/{len(unsynced)} records to MongoDB")
                
            except Exception as e:
                logger.error(f"Error during local to MongoDB sync: {e}")
            finally:
                self._syncing = False
    
    # ==================== Public API ====================
    
    async def is_album_sent(self, user_id: int, media_group_id: str) -> bool:
        """
        Check if an album was already sent to a user.
        Checks both local cache and MongoDB.
        """
        # Always check local cache first (fastest)
        if await self._local_cache.is_album_sent(user_id, media_group_id):
            return True
        
        # Check MongoDB if available
        if await self._is_mongo_available():
            try:
                sent_albums_collection = self._get_sent_albums()
                result = await sent_albums_collection.find_one({
                    'user_id': user_id,
                    'media_group_id': media_group_id
                })
                if result:
                    # Add to local cache for future fast lookups
                    await self._local_cache.mark_album_sent(
                        user_id, media_group_id,
                        result.get('source_chat_id'),
                        synced=True
                    )
                    return True
            except Exception as e:
                logger.warning(f"Error checking MongoDB: {e}")
        
        return False
    
    async def mark_album_sent(
        self, 
        user_id: int, 
        media_group_id: str, 
        source_chat_id: Optional[int] = None
    ) -> None:
        """
        Mark an album as sent to a user.
        Stores in both local cache and MongoDB (if available).
        """
        mongo_available = await self._is_mongo_available()
        
        # Always add to local cache
        await self._local_cache.mark_album_sent(
            user_id, media_group_id, source_chat_id,
            synced=mongo_available
        )
        
        # Try to add to MongoDB
        if mongo_available:
            try:
                sent_albums_collection = self._get_sent_albums()
                await sent_albums_collection.update_one(
                    {'user_id': user_id, 'media_group_id': media_group_id},
                    {'$set': {
                        'user_id': user_id,
                        'media_group_id': media_group_id,
                        'source_chat_id': source_chat_id,
                        'sent_at': datetime.utcnow()
                    }},
                    upsert=True
                )
            except Exception as e:
                logger.warning(f"Error writing to MongoDB: {e}")
                # Mark as unsynced in local cache
                await self._local_cache.mark_album_sent(
                    user_id, media_group_id, source_chat_id,
                    synced=False
                )
    
    async def get_user_sent_albums(self, user_id: int) -> Set[str]:
        """Get all album IDs sent to a user"""
        albums = await self._local_cache.get_user_albums(user_id)
        
        # Also check MongoDB for any we might have missed
        if await self._is_mongo_available():
            try:
                sent_albums_collection = self._get_sent_albums()
                cursor = sent_albums_collection.find({'user_id': user_id})
                mongo_albums = await cursor.to_list(length=None)
                for album in mongo_albums:
                    mg_id = album.get('media_group_id')
                    if mg_id:
                        albums.add(mg_id)
                        # Add to local cache if missing
                        if not await self._local_cache.is_album_sent(user_id, mg_id):
                            await self._local_cache.mark_album_sent(
                                user_id, mg_id,
                                album.get('source_chat_id'),
                                synced=True
                            )
            except Exception as e:
                logger.warning(f"Error querying MongoDB: {e}")
        
        return albums
    
    async def clear_user_albums(self, user_id: int) -> None:
        """Clear all sent album records for a user"""
        await self._local_cache.clear_user(user_id)
        
        if await self._is_mongo_available():
            try:
                sent_albums_collection = self._get_sent_albums()
                await sent_albums_collection.delete_many({'user_id': user_id})
            except Exception as e:
                logger.warning(f"Error clearing MongoDB: {e}")
    
    async def force_sync(self) -> None:
        """Force a sync between local cache and MongoDB"""
        if await self._is_mongo_available():
            await self._sync_local_to_mongo()
    
    async def get_status(self) -> dict:
        """Get current status of the state manager"""
        mongo_status = await self._is_mongo_available()
        unsynced = await self._local_cache.get_unsynced_records()
        
        return {
            'mongo_available': mongo_status,
            'unsynced_records': len(unsynced),
            'syncing': self._syncing,
            'initialized': self._initialized
        }
    
    async def shutdown(self) -> None:
        """Shutdown the state manager (save cache, stop monitoring)"""
        logger.info("Shutting down StateManager...")
        
        # Final sync attempt
        if await self._is_mongo_available():
            await self._sync_local_to_mongo()
        
        # Save local cache
        await self._local_cache.save_to_file()
        
        # Stop monitoring
        if self._mongo_monitor:
            await self._mongo_monitor.stop_monitoring()
        
        logger.info("StateManager shutdown complete")


# Global state manager instance
state_manager = StateManager()


# ==================== Convenience Functions ====================

async def init_state_manager() -> None:
    """Initialize the global state manager"""
    await state_manager.initialize()


async def is_album_sent(user_id: int, media_group_id: str) -> bool:
    """Check if album was sent (convenience function)"""
    return await state_manager.is_album_sent(user_id, media_group_id)


async def mark_album_sent(user_id: int, media_group_id: str, source_chat_id: int = None) -> None:
    """Mark album as sent (convenience function)"""
    await state_manager.mark_album_sent(user_id, media_group_id, source_chat_id)


async def get_user_albums(user_id: int) -> Set[str]:
    """Get user's sent albums (convenience function)"""
    return await state_manager.get_user_sent_albums(user_id)
