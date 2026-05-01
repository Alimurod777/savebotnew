"""
Session Ownership Model - Preventing AUTH_KEY_DUPLICATED

This module enforces the rule:
    ONE session_string = ONE Client = ONE connection

ARCHITECTURE:
- Bot Client: Permanent, file-based session, lives for process lifetime
- User Sessions: Ephemeral, in-memory, task-scoped with guaranteed cleanup

CRITICAL RULES:
1. NEVER create two Clients from the same session_string
2. NEVER pass session_string to child functions - pass Client reference
3. ALWAYS use context managers for session lifecycle
4. ALWAYS wait for full disconnect (10s) before process exit
"""

import asyncio
import uuid
import logging
from typing import Optional, Dict, Set
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from weakref import WeakValueDictionary

from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    AuthKeyUnregistered,
    AuthKeyInvalid,
    AuthKeyDuplicated,
    SessionExpired,
    SessionRevoked,
    UserDeactivated,
)

from config import API_ID, API_HASH

logger = logging.getLogger(__name__)

# Timeouts
SESSION_START_TIMEOUT = 30.0
SESSION_STOP_TIMEOUT = 10.0  # MUST be > TCP FIN-WAIT

# Fatal errors
FATAL_ERRORS = (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionExpired,
    SessionRevoked,
    UserDeactivated,
)


class SessionOwnershipError(Exception):
    """Raised when session ownership rules are violated."""
    pass


class SessionInvalidError(Exception):
    """Session is permanently invalid."""
    def __init__(self, message: str, error_type: str = "unknown"):
        super().__init__(message)
        self.error_type = error_type


class SessionConnectionError(Exception):
    """Temporary connection error."""
    pass


@dataclass
class ActiveSession:
    """Tracks an active session."""
    session_hash: str  # Hash of session_string (for logging, not the actual string)
    user_id: int
    client_name: str
    created_at: float
    task_id: str


class SessionRegistry:
    """
    Global registry to prevent duplicate sessions.
    
    Enforces: ONE session_string = ONE Client at any time
    
    Thread-safe for asyncio (single-threaded but reentrant).
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance
    
    def _init(self):
        # Hash -> ActiveSession mapping
        self._active: Dict[str, ActiveSession] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._loop_id: Optional[int] = None
    
    def _get_lock(self) -> asyncio.Lock:
        """Get loop-safe lock."""
        try:
            current_id = id(asyncio.get_running_loop())
        except RuntimeError:
            return asyncio.Lock()
        
        if self._loop_id != current_id:
            self._lock = asyncio.Lock()
            self._loop_id = current_id
        
        return self._lock
    
    def _hash_session(self, session_string: str) -> str:
        """Create a hash of session_string for tracking."""
        import hashlib
        return hashlib.sha256(session_string.encode()).hexdigest()[:16]
    
    async def acquire(
        self,
        session_string: str,
        user_id: int,
        task_id: str
    ) -> str:
        """
        Acquire ownership of a session.
        
        Raises SessionOwnershipError if session is already in use.
        
        Returns:
            Client name to use
        """
        session_hash = self._hash_session(session_string)
        
        lock = self._get_lock()
        async with lock:
            if session_hash in self._active:
                existing = self._active[session_hash]
                raise SessionOwnershipError(
                    f"Session already in use by task {existing.task_id} "
                    f"(client: {existing.client_name})"
                )
            
            client_name = f"user_{user_id}_{uuid.uuid4().hex[:8]}"
            
            import time
            self._active[session_hash] = ActiveSession(
                session_hash=session_hash,
                user_id=user_id,
                client_name=client_name,
                created_at=time.time(),
                task_id=task_id
            )
            
            logger.debug(f"Session acquired: {session_hash[:8]}... by task {task_id}")
            return client_name
    
    async def release(self, session_string: str) -> None:
        """Release ownership of a session."""
        session_hash = self._hash_session(session_string)
        
        lock = self._get_lock()
        async with lock:
            if session_hash in self._active:
                del self._active[session_hash]
                logger.debug(f"Session released: {session_hash[:8]}...")
    
    async def is_active(self, session_string: str) -> bool:
        """Check if session is currently in use."""
        session_hash = self._hash_session(session_string)
        
        lock = self._get_lock()
        async with lock:
            return session_hash in self._active
    
    def get_active_count(self) -> int:
        """Get count of active sessions."""
        return len(self._active)


# Global registry instance
session_registry = SessionRegistry()


@asynccontextmanager
async def owned_session(
    session_string: str,
    user_id: int,
    task_id: Optional[str] = None,
    timeout: float = SESSION_START_TIMEOUT
):
    """
    Context manager that enforces session ownership.
    
    GUARANTEES:
    1. Only ONE Client per session_string at any time
    2. Client is fully stopped on exit (10s timeout)
    3. Session is released from registry on exit
    
    Args:
        session_string: User's session string
        user_id: User ID for naming
        task_id: Optional task ID for tracking
        timeout: Start timeout
    
    Yields:
        Connected Pyrogram Client
    
    Raises:
        SessionOwnershipError: Session already in use
        SessionInvalidError: Session is invalid
        SessionConnectionError: Connection failed
    
    Usage:
        async with owned_session(session_str, user_id) as client:
            messages = await client.get_messages(...)
        # Client is guaranteed stopped and session released
    """
    if task_id is None:
        task_id = uuid.uuid4().hex[:8]
    
    client = None
    client_name = None
    
    try:
        # Step 1: Acquire ownership (will raise if already in use)
        client_name = await session_registry.acquire(session_string, user_id, task_id)
        
        # Step 2: Create client with validated fingerprint
        from config import get_client_params
        fp = get_client_params()
        client = Client(
            client_name,
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True,
            no_updates=True,
            sleep_threshold=30,
            max_concurrent_transmissions=10,
            device_model=fp['device_model'],
            system_version=fp['system_version'],
            app_version=fp['app_version'],
            lang_code=fp['lang_code'],
        )
        
        # Step 3: Start with timeout and retries
        started = False
        last_error = None
        
        for attempt in range(2):
            try:
                await asyncio.wait_for(client.start(), timeout=timeout)
                started = True
                break
                
            except AuthKeyDuplicated:
                # This should NOT happen if registry is working
                logger.error("AUTH_KEY_DUPLICATED despite registry - waiting...")
                await asyncio.sleep(10.0)
                continue
                
            except FATAL_ERRORS as e:
                raise SessionInvalidError(
                    f"Session invalid: {type(e).__name__}",
                    error_type=type(e).__name__
                )
                
            except asyncio.TimeoutError:
                last_error = "Connection timeout"
                if attempt < 1:
                    await asyncio.sleep(2.0)
                    continue
                    
            except FloodWait as e:
                wait = min(e.value, 30)
                if attempt < 1:
                    await asyncio.sleep(wait)
                    continue
                last_error = f"Rate limited for {e.value}s"
                
            except Exception as e:
                last_error = str(e)
                if attempt < 1:
                    await asyncio.sleep(1.0)
                    continue
        
        if not started:
            raise SessionConnectionError(f"Failed to connect: {last_error}")
        
        # Step 4: Validate session
        try:
            await asyncio.wait_for(client.get_me(), timeout=10.0)
        except FATAL_ERRORS as e:
            raise SessionInvalidError(
                f"Session validation failed: {type(e).__name__}",
                error_type=type(e).__name__
            )
        except Exception as e:
            raise SessionConnectionError(f"Validation failed: {e}")
        
        yield client
        
    except FATAL_ERRORS as e:
        raise SessionInvalidError(
            f"Session error: {type(e).__name__}",
            error_type=type(e).__name__
        )
        
    finally:
        # CRITICAL: Full cleanup with sufficient timeout
        if client:
            try:
                if client.is_connected:
                    await asyncio.wait_for(
                        client.stop(),
                        timeout=SESSION_STOP_TIMEOUT
                    )
            except asyncio.TimeoutError:
                logger.warning(f"Session stop timeout for {client_name}")
            except Exception as e:
                logger.debug(f"Session stop error: {e}")
        
        # Release ownership
        await session_registry.release(session_string)


# =============================================================================
# WHAT MUST NEVER BE DONE
# =============================================================================

"""
NEVER DO THIS:

1. NEVER create two Clients from the same session_string:
   
   # WRONG:
   client1 = Client("a", session_string=session_str)
   client2 = Client("b", session_string=session_str)  # <-- AUTH_KEY_DUPLICATED
   
2. NEVER pass session_string to child functions that create clients:
   
   # WRONG:
   async def process_album(session_string, ...):
       async with create_session(session_string) as client:  # <-- Creates SECOND client!
           ...
   
   async def main():
       async with create_session(session_string) as client:
           await process_album(session_string, ...)  # <-- WRONG!
   
   # CORRECT:
   async def process_album(client, ...):  # Receive client reference
       await client.get_messages(...)
   
   async def main():
       async with create_session(session_string) as client:
           await process_album(client, ...)  # Pass reference!

3. NEVER reconnect by creating a new Client:
   
   # WRONG:
   try:
       await client.start()
   except:
       client = Client(...)  # <-- Creates new instance with same session!
       await client.start()  # <-- AUTH_KEY_DUPLICATED
   
   # CORRECT:
   try:
       await client.start()
   except:
       await safe_disconnect(client)
       await asyncio.sleep(5)  # Wait for Telegram to release
       await client.start()    # Reuse SAME instance

4. NEVER use short disconnect timeouts:
   
   # WRONG:
   await asyncio.wait_for(client.stop(), timeout=1.0)  # Too short!
   
   # CORRECT:
   await asyncio.wait_for(client.stop(), timeout=10.0)  # TCP FIN-WAIT needs time

5. NEVER have background tasks that reconnect:
   
   # WRONG (what main.py was doing):
   async def monitor():
       while True:
           if not connected:
               await client.disconnect()
               await asyncio.sleep(1)  # Too short!
               await client.connect()   # May get AUTH_KEY_DUPLICATED
   
   # CORRECT:
   async def monitor():
       while True:
           if failures > 3:
               await safe_reconnect(client)  # Uses proper delays
"""


# =============================================================================
# CORRECT PATTERNS
# =============================================================================

async def process_with_existing_session(
    client: Client,  # Receive reference, not session_string!
    chat_id: int,
    message_id: int
) -> bool:
    """
    CORRECT pattern: Receive client reference, don't create new session.
    """
    try:
        messages = await client.get_messages(chat_id, message_id)
        return messages is not None
    except Exception as e:
        logger.error(f"Error: {e}")
        return False


async def task_with_owned_session(
    session_string: str,
    user_id: int,
    chat_id: int,
    message_ids: list
) -> int:
    """
    CORRECT pattern: Single session for entire task, passed to helpers.
    """
    processed = 0
    
    async with owned_session(session_string, user_id) as client:
        for msg_id in message_ids:
            # Pass CLIENT, not session_string
            success = await process_with_existing_session(client, chat_id, msg_id)
            if success:
                processed += 1
    
    return processed
