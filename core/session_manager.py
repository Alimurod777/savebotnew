"""
User Session Lifecycle Manager

Handles:
- Session creation and validation
- Connection pooling
- Reconnection with proper delays
- Clean disconnection
- AUTH_KEY_DUPLICATED prevention
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Optional, Dict
from dataclasses import dataclass, field
from pyrogram import Client
from pyrogram.errors import (
    AuthKeyDuplicated, AuthKeyUnregistered, SessionRevoked,
    UserDeactivated, FloodWait, RPCError
)
import logging

logger = logging.getLogger(__name__)

SESSION_POOL_SIZE = 5
SESSION_IDLE_TIMEOUT = 300
RECONNECT_DELAY = 5.0
MAX_RECONNECT_ATTEMPTS = 3


class SessionInvalidError(Exception):
    """Session needs re-login."""
    pass


class SessionConnectionError(Exception):
    """Failed to connect session."""
    pass


@dataclass
class PooledSession:
    client: Client
    user_id: int
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    in_use: bool = False
    connection_errors: int = 0


class SessionManager:
    def __init__(self, api_id: int, api_hash: str):
        self._api_id = api_id
        self._api_hash = api_hash
        
        self._pools: Dict[int, list] = {}
        
        self._lock: asyncio.Lock = None
        self._lock_loop_id: int = None
        
        self._cleanup_task: asyncio.Task = None
    
    def _get_lock(self) -> asyncio.Lock:
        try:
            current_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return asyncio.Lock()
        
        if self._lock_loop_id != current_id:
            self._lock = asyncio.Lock()
            self._lock_loop_id = current_id
        return self._lock
    
    async def start(self) -> None:
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Session manager started")
    
    async def stop(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        for user_id in list(self._pools.keys()):
            await self._close_user_sessions(user_id)
        
        logger.info("Session manager stopped")
    
    @asynccontextmanager
    async def get_session(self, user_id: int, session_string: str):
        session = await self._acquire_session(user_id, session_string)
        try:
            yield session.client
        except (AuthKeyDuplicated, AuthKeyUnregistered, SessionRevoked) as e:
            logger.error(f"Session invalidated: {e}")
            session.connection_errors = MAX_RECONNECT_ATTEMPTS
            raise SessionInvalidError(str(e))
        except Exception as e:
            session.connection_errors += 1
            raise
        finally:
            await self._release_session(user_id, session)
    
    async def _acquire_session(self, user_id: int, session_string: str) -> PooledSession:
        async with self._get_lock():
            if user_id in self._pools:
                for session in self._pools[user_id]:
                    if not session.in_use and session.connection_errors < MAX_RECONNECT_ATTEMPTS:
                        if session.client.is_connected:
                            session.in_use = True
                            session.last_used = time.time()
                            return session
            
            session = await self._create_session(user_id, session_string)
            
            if user_id not in self._pools:
                self._pools[user_id] = []
            self._pools[user_id].append(session)
            
            return session
    
    async def _release_session(self, user_id: int, session: PooledSession) -> None:
        async with self._get_lock():
            session.in_use = False
            session.last_used = time.time()
            
            if session.connection_errors >= MAX_RECONNECT_ATTEMPTS:
                asyncio.create_task(self._remove_session(user_id, session))
    
    async def _create_session(self, user_id: int, session_string: str) -> PooledSession:
        import uuid
        from config import get_client_params
        
        # SECURITY: Use validated platform-consistent fingerprint
        fp = get_client_params(user_id)
        
        client = Client(
            name=f"user_{user_id}_{uuid.uuid4().hex[:8]}",
            api_id=self._api_id,
            api_hash=self._api_hash,
            session_string=session_string,
            in_memory=True,
            no_updates=True,
            workers=4,
            sleep_threshold=60,
            max_concurrent_transmissions=10,
            device_model=fp['device_model'],
            system_version=fp['system_version'],
            app_version=fp['app_version'],
            lang_code=fp['lang_code'],
        )
        
        for attempt in range(MAX_RECONNECT_ATTEMPTS):
            try:
                await client.start()
                logger.debug(f"Session created for user {user_id}")
                return PooledSession(client=client, user_id=user_id, in_use=True)
                
            except FloodWait as e:
                wait = getattr(e, 'value', getattr(e, 'x', 30))
                logger.warning(f"FloodWait on session create: {wait}s")
                if attempt < MAX_RECONNECT_ATTEMPTS - 1:
                    await asyncio.sleep(min(wait, 60))
                    continue
                raise
                
            except (AuthKeyDuplicated, AuthKeyUnregistered, SessionRevoked) as e:
                logger.error(f"Session invalid: {e}")
                raise SessionInvalidError(str(e))
                
            except Exception as e:
                if attempt < MAX_RECONNECT_ATTEMPTS - 1:
                    await asyncio.sleep(RECONNECT_DELAY * (attempt + 1))
                    continue
                raise SessionConnectionError(str(e))
        
        raise SessionConnectionError("Failed to create session after retries")
    
    async def _remove_session(self, user_id: int, session: PooledSession) -> None:
        try:
            if session.client.is_connected:
                await asyncio.wait_for(session.client.stop(), timeout=10.0)
                await asyncio.sleep(RECONNECT_DELAY)
        except:
            pass
        
        async with self._get_lock():
            if user_id in self._pools:
                try:
                    self._pools[user_id].remove(session)
                except ValueError:
                    pass
    
    async def _close_user_sessions(self, user_id: int) -> None:
        async with self._get_lock():
            sessions = self._pools.pop(user_id, [])
        
        for session in sessions:
            try:
                if session.client.is_connected:
                    await asyncio.wait_for(session.client.stop(), timeout=10.0)
            except:
                pass
    
    async def _cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(60)
                
                now = time.time()
                async with self._get_lock():
                    for user_id in list(self._pools.keys()):
                        sessions = self._pools[user_id]
                        
                        to_remove = []
                        for session in sessions:
                            if (not session.in_use and
                                now - session.last_used > SESSION_IDLE_TIMEOUT and
                                len(sessions) > 1):
                                to_remove.append(session)
                        
                        for session in to_remove:
                            asyncio.create_task(self._remove_session(user_id, session))
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Session cleanup error: {e}")
    
    async def invalidate_user_sessions(self, user_id: int) -> None:
        await self._close_user_sessions(user_id)
        logger.info(f"Sessions invalidated for user {user_id}")


@asynccontextmanager
async def create_user_session(session_string: str, user_id: int, api_id: int = None, api_hash: str = None):
    """Create a temporary user session for one-off operations."""
    from config import API_ID, API_HASH, get_client_params
    
    api_id = api_id or API_ID
    api_hash = api_hash or API_HASH
    
    # SECURITY: Use validated platform-consistent fingerprint
    fp = get_client_params(user_id)
    
    import uuid
    client = Client(
        name=f"temp_{user_id}_{uuid.uuid4().hex[:8]}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
        workers=4,
        sleep_threshold=60,
        max_concurrent_transmissions=10,
        device_model=fp['device_model'],
        system_version=fp['system_version'],
        app_version=fp['app_version'],
        lang_code=fp['lang_code'],
    )
    
    try:
        await client.start()
        yield client
    finally:
        if client.is_connected:
            try:
                await asyncio.wait_for(client.stop(), timeout=10.0)
            except:
                pass
        await asyncio.sleep(RECONNECT_DELAY)
