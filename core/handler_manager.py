"""
Safe Handler Registration Manager for Pyrofork

Fixes handler lifecycle issues:
- "list.remove(x): x not in list" errors
- Double registration of handlers
- Race conditions during add/remove
- Handler orphans on reconnect/shutdown

Thread-safe, async-safe handler management.
"""

import asyncio
import logging
import weakref
from typing import Dict, Any, Optional, Set, Callable
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@dataclass
class HandlerEntry:
    """Registered handler entry."""
    handler: Any
    group: int
    handler_id: str
    registered_at: float = field(default_factory=lambda: __import__('time').time())


class SafeHandlerManager:
    """
    Thread-safe handler registration manager.
    
    Prevents:
    - Double registration of handlers
    - "list.remove(x): x not in list" errors
    - Race conditions on add/remove
    - Handler leaks on disconnect/reconnect
    
    Usage:
        manager = SafeHandlerManager()
        
        # Add handler
        await manager.add_handler(client, handler, "my_handler", group=0)
        
        # Remove handler
        await manager.remove_handler(client, "my_handler")
        
        # Clear all on shutdown
        await manager.clear_all(client)
    """
    
    def __init__(self):
        self._handlers: Dict[str, HandlerEntry] = {}
        self._lock = asyncio.Lock()
        self._client_handlers: Dict[int, Set[str]] = {}  # client_id -> handler_ids
    
    async def add_handler(
        self,
        client: Any,
        handler: Any,
        handler_id: str,
        group: int = 0
    ) -> bool:
        """
        Safely add a handler, preventing duplicates.
        
        Args:
            client: Pyrogram Client
            handler: Handler object (MessageHandler, CallbackQueryHandler, etc.)
            handler_id: Unique identifier for this handler
            group: Handler group (default: 0)
        
        Returns:
            True if added successfully, False if already exists
        """
        async with self._lock:
            # Check for duplicate
            if handler_id in self._handlers:
                logger.debug(f"Handler '{handler_id}' already registered, skipping")
                return False
            
            try:
                # Add to Pyrogram
                client.add_handler(handler, group)
                
                # Track in our registry
                entry = HandlerEntry(
                    handler=handler,
                    group=group,
                    handler_id=handler_id
                )
                self._handlers[handler_id] = entry
                
                # Track per-client
                client_id = id(client)
                if client_id not in self._client_handlers:
                    self._client_handlers[client_id] = set()
                self._client_handlers[client_id].add(handler_id)
                
                logger.debug(f"Handler '{handler_id}' registered in group {group}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to add handler '{handler_id}': {e}")
                return False
    
    async def remove_handler(
        self,
        client: Any,
        handler_id: str
    ) -> bool:
        """
        Safely remove a handler.
        
        Args:
            client: Pyrogram Client
            handler_id: Handler identifier to remove
        
        Returns:
            True if removed successfully, False if not found
        """
        async with self._lock:
            if handler_id not in self._handlers:
                logger.debug(f"Handler '{handler_id}' not found for removal")
                return False
            
            entry = self._handlers[handler_id]
            
            try:
                # Remove from Pyrogram
                client.remove_handler(entry.handler, entry.group)
            except ValueError:
                # Already removed from Pyrogram (not in list)
                logger.debug(f"Handler '{handler_id}' already removed from Pyrogram")
            except Exception as e:
                logger.warning(f"Error removing handler '{handler_id}' from Pyrogram: {e}")
            
            # Always clean up our tracking
            del self._handlers[handler_id]
            
            # Clean per-client tracking
            client_id = id(client)
            if client_id in self._client_handlers:
                self._client_handlers[client_id].discard(handler_id)
            
            logger.debug(f"Handler '{handler_id}' removed")
            return True
    
    async def clear_all(self, client: Any) -> int:
        """
        Remove all managed handlers from a client.
        
        Args:
            client: Pyrogram Client
        
        Returns:
            Number of handlers removed
        """
        async with self._lock:
            client_id = id(client)
            handler_ids = list(self._client_handlers.get(client_id, set()))
            
            removed = 0
            for handler_id in handler_ids:
                if handler_id in self._handlers:
                    entry = self._handlers[handler_id]
                    try:
                        client.remove_handler(entry.handler, entry.group)
                    except Exception:
                        pass
                    del self._handlers[handler_id]
                    removed += 1
            
            if client_id in self._client_handlers:
                del self._client_handlers[client_id]
            
            logger.info(f"Cleared {removed} handlers for client")
            return removed
    
    async def clear_all_global(self) -> int:
        """Clear all handlers from all clients (for shutdown)."""
        async with self._lock:
            count = len(self._handlers)
            self._handlers.clear()
            self._client_handlers.clear()
            logger.info(f"Cleared {count} handlers globally")
            return count
    
    def is_registered(self, handler_id: str) -> bool:
        """Check if a handler is registered."""
        return handler_id in self._handlers
    
    def get_handler_count(self, client: Any = None) -> int:
        """Get number of registered handlers."""
        if client:
            client_id = id(client)
            return len(self._client_handlers.get(client_id, set()))
        return len(self._handlers)
    
    def list_handlers(self, client: Any = None) -> list:
        """List all registered handler IDs."""
        if client:
            client_id = id(client)
            return list(self._client_handlers.get(client_id, set()))
        return list(self._handlers.keys())


# Global instance
_global_manager: Optional[SafeHandlerManager] = None


def get_handler_manager() -> SafeHandlerManager:
    """Get or create the global handler manager."""
    global _global_manager
    if _global_manager is None:
        _global_manager = SafeHandlerManager()
    return _global_manager


@asynccontextmanager
async def temporary_handler(
    client: Any,
    handler: Any,
    handler_id: str,
    group: int = 0
):
    """
    Context manager for temporary handlers.
    
    Ensures handler is removed even on exception.
    
    Usage:
        async with temporary_handler(client, my_handler, "temp_handler"):
            # Handler is active
            await some_operation()
        # Handler is automatically removed
    """
    manager = get_handler_manager()
    added = await manager.add_handler(client, handler, handler_id, group)
    
    try:
        yield added
    finally:
        if added:
            await manager.remove_handler(client, handler_id)


class OneTimeHandler:
    """
    Handler wrapper that auto-removes after first trigger.
    
    Usage:
        async def my_callback(client, message):
            # Handle message
            pass
        
        handler = OneTimeHandler(my_callback, "one_time_handler")
        await handler.register(client, filters.text)
        # Handler will auto-remove after first message
    """
    
    def __init__(self, callback: Callable, handler_id: str):
        self.callback = callback
        self.handler_id = handler_id
        self._triggered = False
        self._client = None
        self._handler = None
    
    async def register(
        self,
        client: Any,
        filters: Any = None,
        group: int = 0
    ) -> bool:
        """Register the one-time handler."""
        from pyrogram.handlers import MessageHandler
        
        self._client = client
        
        async def wrapper(client, message):
            if self._triggered:
                return
            
            self._triggered = True
            
            # Remove handler first
            manager = get_handler_manager()
            await manager.remove_handler(client, self.handler_id)
            
            # Then call callback
            return await self.callback(client, message)
        
        self._handler = MessageHandler(wrapper, filters)
        
        manager = get_handler_manager()
        return await manager.add_handler(client, self._handler, self.handler_id, group)
    
    async def cancel(self) -> bool:
        """Cancel (remove) the handler without triggering."""
        if self._client and not self._triggered:
            manager = get_handler_manager()
            return await manager.remove_handler(self._client, self.handler_id)
        return False


class WaitingHandler:
    """
    Handler for waiting for user responses.
    
    Replaces pyromod-style listen() with safe handler management.
    
    Usage:
        waiter = WaitingHandler(client, chat_id, timeout=300)
        message = await waiter.wait()
    """
    
    def __init__(
        self,
        client: Any,
        chat_id: int,
        timeout: float = 300,
        filters: Any = None
    ):
        self.client = client
        self.chat_id = chat_id
        self.timeout = timeout
        self.filters = filters
        self._future: Optional[asyncio.Future] = None
        self._handler_id = f"wait_{chat_id}_{id(self)}"
    
    async def wait(self) -> Optional[Any]:
        """
        Wait for a message from the user.
        
        Returns:
            Message object or None on timeout
        """
        from pyrogram import filters as pyro_filters
        from pyrogram.handlers import MessageHandler
        
        loop = asyncio.get_running_loop()
        self._future = loop.create_future()
        
        # Build filters
        combined_filters = pyro_filters.chat(self.chat_id) & ~pyro_filters.me
        if self.filters:
            combined_filters = combined_filters & self.filters
        
        async def handler(client, message):
            if not self._future.done():
                self._future.set_result(message)
        
        msg_handler = MessageHandler(handler, combined_filters)
        manager = get_handler_manager()
        
        await manager.add_handler(self.client, msg_handler, self._handler_id, group=-999)
        
        try:
            return await asyncio.wait_for(self._future, timeout=self.timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            await manager.remove_handler(self.client, self._handler_id)
    
    def cancel(self):
        """Cancel waiting."""
        if self._future and not self._future.done():
            self._future.cancel()


async def cleanup_on_disconnect(client: Any):
    """
    Clean up all handlers for a client on disconnect.
    
    Call this before client.stop() to prevent orphan handlers.
    """
    manager = get_handler_manager()
    await manager.clear_all(client)


async def cleanup_global():
    """
    Global cleanup on bot shutdown.
    
    Call this in bot's stop() method.
    """
    manager = get_handler_manager()
    await manager.clear_all_global()
