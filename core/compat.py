"""
Python 3.10-3.13 Compatibility Layer

Provides version-safe utilities for:
- Event loop access (replaces deprecated get_event_loop)
- Universal async runner (works in Colab/Kaggle/nested environments)
- Event loop policy configuration
- Monotonic time access via loop.time() replacement

This module uses FEATURE DETECTION, not version checks, for forward compatibility.
"""

import asyncio
import platform
import logging

logger = logging.getLogger(__name__)


def get_event_loop_safe() -> asyncio.AbstractEventLoop:
    """
    Get the running event loop, or create a new one if none exists.

    Replaces the deprecated asyncio.get_event_loop() which emits
    DeprecationWarning on 3.10+ and raises RuntimeError on 3.12+
    when no current event loop exists.
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def get_monotonic_time() -> float:
    """
    Get monotonic time from the running event loop.

    Safe replacement for asyncio.get_event_loop().time().
    Falls back to time.monotonic() when no loop is running.
    """
    try:
        return asyncio.get_running_loop().time()
    except RuntimeError:
        import time
        return time.monotonic()


def configure_event_loop_policy() -> None:
    """
    Configure the event loop policy for the current platform.

    Windows: Uses WindowsSelectorEventLoopPolicy (avoids Proactor issues).
    Linux/macOS: Installs uvloop if available.

    Uses feature detection (hasattr) instead of version checks.
    Idempotent - safe to call multiple times.
    """
    system = platform.system()

    if system == "Windows":
        if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            current = asyncio.get_event_loop_policy()
            if not isinstance(current, asyncio.WindowsSelectorEventLoopPolicy):
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
                logger.info("Configured WindowsSelectorEventLoopPolicy for Windows")
    else:
        try:
            import uvloop
            if hasattr(uvloop, "install"):
                uvloop.install()
            else:
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.info("uvloop installed for improved async performance")
        except ImportError:
            logger.debug("uvloop not available, using default event loop")


def detect_environment() -> str:
    """
    Auto-detect execution environment.
    
    Returns one of: 'colab', 'kaggle', 'docker', 'linux', 'windows', 'macos'
    Used for environment-specific behavior (temp dirs, event loop, etc.).
    """
    import os
    
    # Google Colab
    if os.environ.get('COLAB_GPU') or os.environ.get('COLAB_RELEASE_TAG'):
        return 'colab'
    try:
        import google.colab  # noqa: F401
        return 'colab'
    except ImportError:
        pass
    
    # Kaggle
    if os.environ.get('KAGGLE_KERNEL_RUN_TYPE') or os.path.exists('/kaggle'):
        return 'kaggle'
    
    # Docker
    if os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER'):
        return 'docker'
    
    system = platform.system()
    if system == 'Windows':
        return 'windows'
    elif system == 'Darwin':
        return 'macos'
    return 'linux'


def get_temp_dir(subdir: str = "downloads/temp") -> str:
    """
    Get a cross-platform temp directory, lazily created.
    
    Uses project-relative paths to avoid OS-specific temp locations.
    """
    import os
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base, subdir)
    os.makedirs(path, exist_ok=True)
    return path


def run_async(coro):
    """
    Universal async runner that works in all environments:
    - Standard scripts
    - Google Colab (where a loop may already be running)
    - Kaggle notebooks
    - Nested async contexts

    Uses asyncio.Runner if available (3.11+), otherwise falls back.
    """
    if hasattr(asyncio, "Runner"):
        try:
            with asyncio.Runner() as runner:
                return runner.run(coro)
        except RuntimeError:
            pass

    try:
        loop = asyncio.get_running_loop()
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            return pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        pass

    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
