"""
Safe Reconnect Patterns for Pyrogram

CRITICAL: These patterns prevent AUTH_KEY_DUPLICATED errors.

Rules:
1. NEVER create a new Client instance to "reconnect"
2. ALWAYS wait for full disconnect before reconnecting
3. Bot Client is PERMANENT - only restart process if truly needed
4. User sessions are TASK-SCOPED - pass reference, don't recreate
"""

import asyncio
import logging
from typing import Optional
from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    AuthKeyUnregistered,
    AuthKeyInvalid,
    AuthKeyDuplicated,
    SessionExpired,
    SessionRevoked,
)

logger = logging.getLogger(__name__)

# Minimum wait time after disconnect before reconnect
# This MUST be longer than TCP FIN-WAIT timeout + Telegram server cleanup
# CRITICAL: Too short = AUTH_KEY_DUPLICATED
MIN_RECONNECT_DELAY = 10.0  # Increased from 5.0 to prevent AUTH_KEY_DUPLICATED

# Maximum wait time for disconnect to complete
DISCONNECT_TIMEOUT = 15.0  # Increased from 10.0 for reliability

# Errors that mean session is permanently dead
FATAL_SESSION_ERRORS = (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionExpired,
    SessionRevoked,
)


class ReconnectError(Exception):
    """Raised when reconnect fails in a way that requires process restart."""
    pass


async def safe_disconnect(client: Client, timeout: float = DISCONNECT_TIMEOUT) -> bool:
    """
    Safely disconnect a Pyrogram client.
    
    CRITICAL: Waits for full disconnect, not just signal sent.
    
    Args:
        client: Pyrogram client to disconnect
        timeout: Maximum time to wait for disconnect
    
    Returns:
        True if disconnected successfully
    """
    if not client:
        return True
    
    try:
        # Check if already disconnected
        if not client.is_connected:
            return True
        
        # Disable updates to prevent callbacks during disconnect
        if hasattr(client, 'no_updates'):
            client.no_updates = True
        
        # Wait for disconnect with timeout
        await asyncio.wait_for(client.stop(), timeout=timeout)
        
        # Extra wait for TCP to fully close and Telegram to acknowledge
        # CRITICAL: Must be sufficient to prevent AUTH_KEY_DUPLICATED
        await asyncio.sleep(3.0)  # Increased from 1.0
        
        return True
        
    except asyncio.TimeoutError:
        logger.warning(f"Disconnect timeout after {timeout}s")
        return False
    except Exception as e:
        logger.warning(f"Disconnect error: {e}")
        return False


async def safe_reconnect(
    client: Client,
    max_attempts: int = 3,
    initial_delay: float = MIN_RECONNECT_DELAY
) -> bool:
    """
    Safely reconnect a Pyrogram client WITHOUT recreating it.
    
    CRITICAL RULES:
    1. Uses EXISTING Client instance - never creates new one
    2. Waits sufficient time between disconnect and connect
    3. Handles FloodWait properly
    4. Returns False on fatal errors (caller should restart process)
    
    Args:
        client: EXISTING Pyrogram client to reconnect
        max_attempts: Maximum reconnection attempts
        initial_delay: Initial delay between disconnect and connect
    
    Returns:
        True if reconnected successfully
        False if reconnection failed (may need process restart)
    """
    for attempt in range(max_attempts):
        try:
            # Step 1: Full disconnect
            logger.info(f"Reconnect attempt {attempt + 1}/{max_attempts}: disconnecting...")
            
            if client.is_connected:
                disconnect_ok = await safe_disconnect(client)
                if not disconnect_ok:
                    logger.warning("Disconnect did not complete cleanly")
            
            # Step 2: Wait for Telegram to acknowledge disconnect
            # This is CRITICAL - too short = AUTH_KEY_DUPLICATED
            delay = initial_delay * (attempt + 1)
            logger.info(f"Waiting {delay}s before reconnect...")
            await asyncio.sleep(delay)
            
            # Step 3: Reconnect using SAME client instance
            logger.info("Reconnecting...")
            await asyncio.wait_for(client.start(), timeout=30.0)
            
            # Step 4: Verify connection
            await client.get_me()
            
            logger.info("Reconnect successful")
            return True
            
        except AuthKeyDuplicated:
            # This means we reconnected too fast or another client used same session
            logger.error("AUTH_KEY_DUPLICATED - reconnected too fast, waiting longer...")
            # Exponential backoff: 15s, 30s, 45s
            await asyncio.sleep(15.0 * (attempt + 1))
            continue
            
        except FATAL_SESSION_ERRORS as e:
            # Session is dead - cannot reconnect, need new session
            logger.error(f"Fatal session error: {type(e).__name__}")
            return False
            
        except FloodWait as e:
            wait_time = getattr(e, 'value', getattr(e, 'x', 30))
            logger.warning(f"FloodWait: waiting {wait_time}s")
            await asyncio.sleep(wait_time)
            continue
            
        except asyncio.TimeoutError:
            logger.warning("Reconnect timeout")
            continue
            
        except Exception as e:
            logger.error(f"Reconnect error: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(initial_delay * (attempt + 1))
                continue
            return False
    
    logger.error(f"Reconnect failed after {max_attempts} attempts")
    return False


class ConnectionMonitor:
    """
    Safe connection monitor that doesn't cause AUTH_KEY_DUPLICATED.
    
    REPLACES the broken _connection_monitor() in main.py
    
    Key differences:
    1. Uses safe_reconnect() instead of disconnect+connect
    2. Longer delays between checks
    3. Stops monitoring on fatal errors
    4. No infinite loops
    """
    
    def __init__(self, client: Client):
        self.client = client
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._consecutive_failures = 0
        self._max_failures = 3
    
    def start(self) -> None:
        """Start monitoring in background."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
    
    def stop(self) -> None:
        """Stop monitoring."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
    
    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        check_interval = 60  # Check every 60 seconds
        
        while self._running:
            try:
                await asyncio.sleep(check_interval)
                
                if not self._running:
                    break
                
                # Check connection
                try:
                    await asyncio.wait_for(self.client.get_me(), timeout=10.0)
                    self._consecutive_failures = 0
                    
                except asyncio.TimeoutError:
                    self._consecutive_failures += 1
                    logger.warning(f"Connection check timeout ({self._consecutive_failures}/{self._max_failures})")
                    
                except Exception as e:
                    self._consecutive_failures += 1
                    logger.warning(f"Connection check failed: {e} ({self._consecutive_failures}/{self._max_failures})")
                
                # Attempt reconnect if needed
                if self._consecutive_failures >= self._max_failures:
                    logger.warning("Too many failures, attempting safe reconnect...")
                    
                    success = await safe_reconnect(self.client)
                    
                    if success:
                        self._consecutive_failures = 0
                    else:
                        # Fatal - stop monitoring, let process restart handle it
                        logger.error("Safe reconnect failed - stopping monitor")
                        self._running = False
                        break
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(check_interval)
        
        logger.info("Connection monitor stopped")


def create_connection_monitor(client: Client) -> ConnectionMonitor:
    """Create and return a connection monitor for a client."""
    return ConnectionMonitor(client)
