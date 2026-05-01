"""
MongoDB Schema - MINIMAL USE

MongoDB is used ONLY for:
1. User sessions (Pyrogram session strings)
2. Blocked users list

All other state is managed locally.
"""

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import Optional, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MongoSessionStore:
    """
    MongoDB session storage - minimal operations.
    
    CRITICAL: Only used for:
    - Login/logout operations
    - Session retrieval at operation start
    - Blocked user checks
    
    NOT used for:
    - Download progress
    - Queue state
    - Album tracking
    - Any runtime state
    """
    
    def __init__(self, uri: str, db_name: str = "telegram_bot"):
        self._client: AsyncIOMotorClient = None
        self._db: AsyncIOMotorDatabase = None
        self._uri = uri
        self._db_name = db_name
        self._connected = False
    
    async def connect(self) -> bool:
        try:
            self._client = AsyncIOMotorClient(
                self._uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                maxPoolSize=10,
            )
            
            await self._client.admin.command('ping')
            
            self._db = self._client[self._db_name]
            self._connected = True
            
            await self._create_indexes()
            
            logger.info("MongoDB connected")
            return True
            
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            self._connected = False
            return False
    
    async def _create_indexes(self) -> None:
        try:
            await self._db.users.create_index("user_id", unique=True)
            await self._db.blocked_users.create_index("user_id", unique=True)
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")
    
    async def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._connected = False
            logger.info("MongoDB disconnected")
    
    def is_connected(self) -> bool:
        return self._connected
    
    async def save_session(
        self,
        user_id: int,
        session_string: str,
        username: str = None,
        first_name: str = None
    ) -> bool:
        if not self._connected:
            logger.error("MongoDB not connected")
            return False
        
        try:
            await self._db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "session": session_string,
                        "logged_in": True,
                        "updated_at": datetime.utcnow(),
                        "username": username,
                        "first_name": first_name,
                    },
                    "$setOnInsert": {
                        "created_at": datetime.utcnow(),
                    }
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Failed to save session: {e}")
            return False
    
    async def get_session(self, user_id: int) -> Optional[str]:
        if not self._connected:
            return None
        
        try:
            user = await self._db.users.find_one(
                {"user_id": user_id, "logged_in": True}
            )
            return user.get("session") if user else None
        except Exception as e:
            logger.error(f"Failed to get session: {e}")
            return None
    
    async def get_user_data(self, user_id: int) -> Optional[dict]:
        if not self._connected:
            return None
        
        try:
            return await self._db.users.find_one({"user_id": user_id})
        except Exception as e:
            logger.error(f"Failed to get user data: {e}")
            return None
    
    async def delete_session(self, user_id: int) -> bool:
        if not self._connected:
            return False
        
        try:
            await self._db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "session": None,
                        "logged_in": False,
                        "updated_at": datetime.utcnow(),
                    }
                }
            )
            return True
        except Exception as e:
            logger.error(f"Failed to delete session: {e}")
            return False
    
    async def is_blocked(self, user_id: int) -> bool:
        if not self._connected:
            return False
        
        try:
            blocked = await self._db.blocked_users.find_one({"user_id": user_id})
            return blocked is not None
        except Exception as e:
            logger.error(f"Failed to check blocked status: {e}")
            return False
    
    async def block_user(self, user_id: int, blocked_by: int, reason: str = None) -> bool:
        if not self._connected:
            return False
        
        try:
            await self._db.blocked_users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "blocked_by": blocked_by,
                        "blocked_at": datetime.utcnow(),
                        "reason": reason,
                    }
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Failed to block user: {e}")
            return False
    
    async def unblock_user(self, user_id: int) -> bool:
        if not self._connected:
            return False
        
        try:
            await self._db.blocked_users.delete_one({"user_id": user_id})
            return True
        except Exception as e:
            logger.error(f"Failed to unblock user: {e}")
            return False
    
    async def get_blocked_users(self) -> List[int]:
        if not self._connected:
            return []
        
        try:
            cursor = self._db.blocked_users.find({}, {"user_id": 1})
            blocked = await cursor.to_list(length=None)
            return [b["user_id"] for b in blocked]
        except Exception as e:
            logger.error(f"Failed to get blocked users: {e}")
            return []
