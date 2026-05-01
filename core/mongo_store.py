"""
MongoDB Store - Sessions and Blocked Users ONLY

This module handles ONLY:
1. Session string storage (tg_sessions collection)
2. Blocked users (blocked_users collection)

ALL download state is stored in SQLite (state_store.py).

Uses motor for async MongoDB operations, with pymongo fallback.
"""

import asyncio
import logging
from typing import Optional, List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

# Try motor (async), fallback to pymongo with to_thread
try:
    from motor.motor_asyncio import AsyncIOMotorClient
    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False
    logger.warning("motor not available, using pymongo with asyncio.to_thread")

try:
    import pymongo
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    logger.error("pymongo not available!")


class MongoStore:
    """
    MongoDB store for session strings and blocked users.
    
    Collections:
    - tg_sessions: {"_id": user_id, "session_string": "...", ...}
    - blocked_users: {"_id": user_id, "reason": "...", "blocked_by": int, ...}
    
    Features:
    - Async operations (motor or pymongo+to_thread)
    - Connection pooling
    - Automatic index creation
    - Graceful fallback if MongoDB unavailable
    """
    
    def __init__(self, uri: str, db_name: str = "telegram_bot"):
        self.uri = uri
        self.db_name = db_name
        self._client = None
        self._db = None
        self._connected = False
        self._using_motor = False
    
    async def connect(self) -> bool:
        """
        Connect to MongoDB.
        
        Returns True if connected, False otherwise.
        """
        try:
            if MOTOR_AVAILABLE:
                self._client = AsyncIOMotorClient(
                    self.uri,
                    serverSelectionTimeoutMS=10000,
                    connectTimeoutMS=10000,
                    maxPoolSize=10,
                )
                # Verify connection
                await self._client.admin.command('ping')
                self._db = self._client[self.db_name]
                self._using_motor = True
                logger.info("Connected to MongoDB using motor (async)")
                
            elif PYMONGO_AVAILABLE:
                # Sync pymongo - wrap operations with to_thread
                self._client = pymongo.MongoClient(
                    self.uri,
                    serverSelectionTimeoutMS=10000,
                    connectTimeoutMS=10000,
                    maxPoolSize=10,
                )
                self._client.admin.command('ping')
                self._db = self._client[self.db_name]
                self._using_motor = False
                logger.info("Connected to MongoDB using pymongo (sync with to_thread)")
                
            else:
                logger.error("No MongoDB driver available")
                return False
            
            self._connected = True
            
            # Create indexes
            await self._create_indexes()
            
            return True
            
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            self._connected = False
            return False
    
    async def _create_indexes(self) -> None:
        """Create required indexes."""
        try:
            if self._using_motor:
                await self._db.tg_sessions.create_index("user_id", unique=True)
                await self._db.blocked_users.create_index("user_id", unique=True)
            else:
                await asyncio.to_thread(
                    self._db.tg_sessions.create_index, "user_id", unique=True
                )
                await asyncio.to_thread(
                    self._db.blocked_users.create_index, "user_id", unique=True
                )
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")
    
    async def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        if self._client:
            self._client.close()
            self._connected = False
            logger.info("MongoDB disconnected")
    
    def is_connected(self) -> bool:
        """Check connection status."""
        return self._connected
    
    # ==================== SESSION OPERATIONS ====================
    
    async def save_session(
        self,
        user_id: int,
        session_string: str,
        metadata: Optional[Dict] = None
    ) -> bool:
        """
        Save or update user session string.
        
        Args:
            user_id: Telegram user ID
            session_string: Pyrogram session string
            metadata: Optional additional data (username, name, etc.)
        """
        if not self._connected:
            logger.error("MongoDB not connected")
            return False
        
        try:
            doc = {
                "user_id": user_id,
                "session_string": session_string,
                "updated_at": datetime.utcnow(),
            }
            if metadata:
                doc.update(metadata)
            
            if self._using_motor:
                await self._db.tg_sessions.update_one(
                    {"user_id": user_id},
                    {
                        "$set": doc,
                        "$setOnInsert": {"created_at": datetime.utcnow()}
                    },
                    upsert=True
                )
            else:
                await asyncio.to_thread(
                    self._db.tg_sessions.update_one,
                    {"user_id": user_id},
                    {
                        "$set": doc,
                        "$setOnInsert": {"created_at": datetime.utcnow()}
                    },
                    upsert=True
                )
            
            logger.info(f"Session saved for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save session: {e}")
            return False
    
    async def get_session(self, user_id: int) -> Optional[str]:
        """
        Get user session string.
        
        Returns session string or None if not found.
        """
        if not self._connected:
            return None
        
        try:
            if self._using_motor:
                doc = await self._db.tg_sessions.find_one({"user_id": user_id})
            else:
                doc = await asyncio.to_thread(
                    self._db.tg_sessions.find_one, {"user_id": user_id}
                )
            
            return doc.get("session_string") if doc else None
            
        except Exception as e:
            logger.error(f"Failed to get session: {e}")
            return None
    
    async def delete_session(self, user_id: int) -> bool:
        """Delete user session (logout)."""
        if not self._connected:
            return False
        
        try:
            if self._using_motor:
                result = await self._db.tg_sessions.delete_one({"user_id": user_id})
            else:
                result = await asyncio.to_thread(
                    self._db.tg_sessions.delete_one, {"user_id": user_id}
                )
            
            deleted = result.deleted_count > 0
            if deleted:
                logger.info(f"Session deleted for user {user_id}")
            return deleted
            
        except Exception as e:
            logger.error(f"Failed to delete session: {e}")
            return False
    
    async def get_all_sessions(self) -> List[Dict]:
        """
        Get all stored sessions.
        
        Returns list of session documents.
        """
        if not self._connected:
            return []
        
        try:
            if self._using_motor:
                cursor = self._db.tg_sessions.find({})
                return await cursor.to_list(length=None)
            else:
                def _get_all():
                    return list(self._db.tg_sessions.find({}))
                return await asyncio.to_thread(_get_all)
                
        except Exception as e:
            logger.error(f"Failed to get sessions: {e}")
            return []
    
    async def session_exists(self, user_id: int) -> bool:
        """Check if user has a session stored."""
        if not self._connected:
            return False
        
        try:
            if self._using_motor:
                count = await self._db.tg_sessions.count_documents(
                    {"user_id": user_id},
                    limit=1
                )
            else:
                count = await asyncio.to_thread(
                    self._db.tg_sessions.count_documents,
                    {"user_id": user_id},
                    limit=1
                )
            return count > 0
        except Exception as e:
            logger.error(f"Failed to check session: {e}")
            return False
    
    # ==================== BLOCKED USERS ====================
    
    async def is_blocked(self, user_id: int) -> bool:
        """Check if user is blocked."""
        if not self._connected:
            return False  # Fail open if DB is down
        
        try:
            if self._using_motor:
                doc = await self._db.blocked_users.find_one({"user_id": user_id})
            else:
                doc = await asyncio.to_thread(
                    self._db.blocked_users.find_one, {"user_id": user_id}
                )
            
            return doc is not None
            
        except Exception as e:
            logger.error(f"Failed to check blocked status: {e}")
            return False
    
    async def block_user(
        self,
        user_id: int,
        blocked_by: int,
        reason: str = ""
    ) -> bool:
        """Block a user."""
        if not self._connected:
            return False
        
        try:
            doc = {
                "user_id": user_id,
                "blocked_by": blocked_by,
                "reason": reason,
                "blocked_at": datetime.utcnow(),
            }
            
            if self._using_motor:
                await self._db.blocked_users.update_one(
                    {"user_id": user_id},
                    {"$set": doc},
                    upsert=True
                )
            else:
                await asyncio.to_thread(
                    self._db.blocked_users.update_one,
                    {"user_id": user_id},
                    {"$set": doc},
                    upsert=True
                )
            
            logger.info(f"User {user_id} blocked by {blocked_by}: {reason}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to block user: {e}")
            return False
    
    async def unblock_user(self, user_id: int) -> bool:
        """Unblock a user."""
        if not self._connected:
            return False
        
        try:
            if self._using_motor:
                result = await self._db.blocked_users.delete_one({"user_id": user_id})
            else:
                result = await asyncio.to_thread(
                    self._db.blocked_users.delete_one, {"user_id": user_id}
                )
            
            unblocked = result.deleted_count > 0
            if unblocked:
                logger.info(f"User {user_id} unblocked")
            return unblocked
            
        except Exception as e:
            logger.error(f"Failed to unblock user: {e}")
            return False
    
    async def get_blocked_users(self) -> List[Dict]:
        """Get all blocked users with details."""
        if not self._connected:
            return []
        
        try:
            if self._using_motor:
                cursor = self._db.blocked_users.find({})
                return await cursor.to_list(length=None)
            else:
                def _get_blocked():
                    return list(self._db.blocked_users.find({}))
                return await asyncio.to_thread(_get_blocked)
                
        except Exception as e:
            logger.error(f"Failed to get blocked users: {e}")
            return []
    
    async def get_blocked_user_ids(self) -> List[int]:
        """Get list of blocked user IDs only."""
        if not self._connected:
            return []
        
        try:
            if self._using_motor:
                cursor = self._db.blocked_users.find({}, {"user_id": 1})
                docs = await cursor.to_list(length=None)
            else:
                def _get_ids():
                    return list(self._db.blocked_users.find({}, {"user_id": 1}))
                docs = await asyncio.to_thread(_get_ids)
            
            return [d["user_id"] for d in docs]
            
        except Exception as e:
            logger.error(f"Failed to get blocked user IDs: {e}")
            return []


# Global instance
_mongo_store: Optional[MongoStore] = None


async def init_mongo_store(
    uri: str,
    db_name: str = "telegram_bot"
) -> MongoStore:
    """Initialize the global MongoDB store."""
    global _mongo_store
    _mongo_store = MongoStore(uri, db_name)
    await _mongo_store.connect()
    return _mongo_store


def get_mongo_store() -> Optional[MongoStore]:
    """Get the global MongoDB store instance."""
    return _mongo_store
