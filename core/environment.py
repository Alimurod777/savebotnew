"""
core/environment.py - Runtime environment detection and path management.

Automatically detects:
- Google Colab
- Kaggle Notebooks
- Local Linux/Windows

All filesystem paths must derive from get_base_path() to ensure
portability across environments without hardcoded paths.
"""

import os
import logging
from typing import Optional
from enum import Enum

logger = logging.getLogger(__name__)


class RuntimeEnvironment(Enum):
    COLAB = "colab"
    KAGGLE = "kaggle"
    LOCAL = "local"


_detected_env: Optional[RuntimeEnvironment] = None
_base_path: Optional[str] = None


def detect_environment() -> RuntimeEnvironment:
    """Detect the current runtime environment."""
    global _detected_env

    if _detected_env is not None:
        return _detected_env

    if "COLAB_GPU" in os.environ or "COLAB_RELEASE_TAG" in os.environ:
        _detected_env = RuntimeEnvironment.COLAB
        logger.info("Environment detected: Google Colab")
    elif "KAGGLE_KERNEL_RUN_TYPE" in os.environ or "KAGGLE_DATA_PROXY_TOKEN" in os.environ:
        _detected_env = RuntimeEnvironment.KAGGLE
        logger.info("Environment detected: Kaggle Notebook")
    else:
        _detected_env = RuntimeEnvironment.LOCAL
        logger.info("Environment detected: Local")

    return _detected_env


def get_base_path() -> str:
    """
    Get the base path for all file operations.

    Returns:
        - Colab: /content
        - Kaggle: /kaggle/working
        - Local: project root (parent of core/)
    """
    global _base_path

    if _base_path is not None:
        return _base_path

    env = detect_environment()

    if env == RuntimeEnvironment.COLAB:
        _base_path = "/content"
    elif env == RuntimeEnvironment.KAGGLE:
        _base_path = "/kaggle/working"
    else:
        # Local: use the project root (parent of this file's directory)
        _base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    os.makedirs(_base_path, exist_ok=True)
    return _base_path


def get_sessions_path() -> str:
    """Get the directory for Pyrogram session files."""
    path = os.path.join(get_base_path(), "sessions")
    os.makedirs(path, exist_ok=True)
    return path


def get_downloads_path() -> str:
    """Get the directory for temporary downloads."""
    path = os.path.join(get_base_path(), "downloads")
    os.makedirs(path, exist_ok=True)
    return path


def get_temp_path() -> str:
    """Get the directory for temporary files."""
    path = os.path.join(get_base_path(), "downloads", "temp")
    os.makedirs(path, exist_ok=True)
    return path


def get_local_db_path() -> str:
    """Get the directory for local SQLite database fallback."""
    path = os.path.join(get_base_path(), "local_db")
    os.makedirs(path, exist_ok=True)
    return path


def get_logs_path() -> str:
    """Get the directory for log files."""
    path = os.path.join(get_base_path(), "logs")
    os.makedirs(path, exist_ok=True)
    return path


def is_colab() -> bool:
    return detect_environment() == RuntimeEnvironment.COLAB


def is_kaggle() -> bool:
    return detect_environment() == RuntimeEnvironment.KAGGLE


def is_local() -> bool:
    return detect_environment() == RuntimeEnvironment.LOCAL


def get_memory_aware_strategy(file_size_bytes: int) -> str:
    """
    Determine download strategy based on file size and environment.

    Returns:
        'memory': in-memory buffer (<100MB)
        'streamed': streamed temp file (100MB-2GB)
        'chunked': chunked streaming (>2GB)
    """
    MB = 1024 * 1024
    GB = 1024 * MB

    if file_size_bytes < 100 * MB:
        return "memory"
    elif file_size_bytes < 2 * GB:
        return "streamed"
    else:
        return "chunked"
