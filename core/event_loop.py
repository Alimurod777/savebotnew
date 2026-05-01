"""
Event Loop Configuration

Ensures proper asyncio event loop policy BEFORE any async code runs.
Must be imported FIRST in main entry point.

Windows: WindowsSelectorEventLoopPolicy (avoids Proactor issues)
Linux: Default policy (optionally uvloop for performance)

Compatible with Python 3.10-3.13. Uses feature detection, not version checks.
"""

import asyncio
import platform
import logging

logger = logging.getLogger(__name__)

_loop_configured = False


def configure_event_loop() -> None:
    """
    Configure event loop policy.
    
    MUST be called before ANY asyncio operations or imports
    that might create event loops.
    
    This function is idempotent - safe to call multiple times.
    Uses feature detection (hasattr) for forward compatibility.
    """
    global _loop_configured
    
    if _loop_configured:
        return
    
    system = platform.system()
    
    if system == "Windows":
        if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
            current_policy = asyncio.get_event_loop_policy()
            if not isinstance(current_policy, asyncio.WindowsSelectorEventLoopPolicy):
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
                logger.info("Configured WindowsSelectorEventLoopPolicy for Windows")
    else:
        try:
            import uvloop
            if hasattr(uvloop, 'install'):
                uvloop.install()
            else:
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.info("Installed uvloop for improved async performance")
        except ImportError:
            logger.debug("uvloop not available, using default event loop")
    
    _loop_configured = True


def get_running_loop_safe() -> asyncio.AbstractEventLoop:
    """
    Get the running event loop, or create a new one if none exists.
    
    Safe replacement for asyncio.get_event_loop() which is deprecated
    in Python 3.10+ and raises RuntimeError in 3.12+ when no loop exists.
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def is_loop_running() -> bool:
    """Check if an event loop is currently running."""
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False
