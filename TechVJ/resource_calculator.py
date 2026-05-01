"""
Resource Calculator for Pyrogram Client

Calculates optimal worker and concurrent transmission settings based on
available system resources. MUST be called BEFORE Pyrogram Client initialization.

Design Decisions:
- Workers: Pyrogram recommends 4x CPU cores, capped for stability
- Memory: Scale down if less than 4GB available
- Windows: Cap concurrent transmissions due to selector event loop limits
- Minimum thresholds ensure basic functionality on limited systems

Author: WOODcraft Refactored
"""

import os
import logging
import platform
from typing import Tuple

logger = logging.getLogger(__name__)

# Try to import psutil for accurate memory detection
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not available - using conservative memory estimates")


# ============================================================================
# CONSTANTS
# ============================================================================

# Worker limits
MIN_WORKERS = 8
MAX_WORKERS = 64
DEFAULT_WORKERS = 16

# Concurrent transmission limits
MIN_CONCURRENT = 4
MAX_CONCURRENT = 32  # Windows selector loop has 512 fd limit, stay safe
DEFAULT_CONCURRENT = 8

# Memory thresholds (in GB)
LOW_MEMORY_THRESHOLD = 2.0   # Below this, use minimum settings
NORMAL_MEMORY_THRESHOLD = 4.0  # Below this, reduce settings
HIGH_MEMORY_THRESHOLD = 8.0   # Above this, allow higher settings

# Memory per worker (estimated MB)
MEMORY_PER_WORKER = 5  # Rough estimate for Pyrogram worker memory
MEMORY_PER_TRANSMISSION = 50  # Estimated memory for active file transfer


# ============================================================================
# RESOURCE DETECTION
# ============================================================================

def get_cpu_count() -> int:
    """
    Get the number of CPU cores available.
    
    Returns:
        Number of CPU cores (minimum 1)
    """
    try:
        cpu_count = os.cpu_count()
        if cpu_count is None or cpu_count < 1:
            cpu_count = 1
        return cpu_count
    except Exception as e:
        logger.warning(f"Failed to get CPU count: {e}")
        return 1


def get_available_memory_gb() -> float:
    """
    Get available system memory in gigabytes.
    
    Uses psutil if available, otherwise returns a conservative estimate.
    
    Returns:
        Available memory in GB
    """
    if PSUTIL_AVAILABLE:
        try:
            mem = psutil.virtual_memory()
            # Use available memory, not total
            available_gb = mem.available / (1024 ** 3)
            return available_gb
        except Exception as e:
            logger.warning(f"Failed to get memory info via psutil: {e}")
    
    # Conservative fallback - assume 4GB typical Windows system
    return 4.0


def is_windows() -> bool:
    """Check if running on Windows."""
    return platform.system() == "Windows"


# ============================================================================
# WORKER CALCULATION
# ============================================================================

def calculate_workers(cpu_count: int, memory_gb: float) -> int:
    """
    Calculate optimal Pyrogram worker count.
    
    Formula:
    - Base: 4 * CPU cores (Pyrogram recommendation)
    - Scale by memory availability
    - Cap at MAX_WORKERS for stability
    
    Args:
        cpu_count: Number of CPU cores
        memory_gb: Available memory in GB
    
    Returns:
        Optimal worker count
    """
    # Base calculation: 4 workers per CPU core
    base_workers = cpu_count * 4
    
    # Memory-based limits
    if memory_gb < LOW_MEMORY_THRESHOLD:
        # Very limited memory - use minimum
        memory_factor = 0.5
    elif memory_gb < NORMAL_MEMORY_THRESHOLD:
        # Limited memory - scale down
        memory_factor = 0.75
    elif memory_gb >= HIGH_MEMORY_THRESHOLD:
        # Plenty of memory - allow higher
        memory_factor = 1.25
    else:
        # Normal memory
        memory_factor = 1.0
    
    # Apply memory factor
    workers = int(base_workers * memory_factor)
    
    # Enforce limits
    workers = max(MIN_WORKERS, min(workers, MAX_WORKERS))
    
    return workers


def calculate_concurrent_transmissions(cpu_count: int, memory_gb: float) -> int:
    """
    Calculate optimal max_concurrent_transmissions for Pyrogram.
    
    Formula:
    - Base: 2 * CPU cores
    - Scale by memory (each transmission buffers file data)
    - Windows: Cap lower due to selector limitations
    
    Args:
        cpu_count: Number of CPU cores
        memory_gb: Available memory in GB
    
    Returns:
        Optimal concurrent transmission count
    """
    # Base calculation: 2 concurrent per CPU core
    base_concurrent = cpu_count * 2
    
    # Memory-based calculation: estimate how many transfers we can buffer
    memory_concurrent = int((memory_gb * 1024) / MEMORY_PER_TRANSMISSION / 2)
    
    # Take the minimum of CPU-based and memory-based
    concurrent = min(base_concurrent, memory_concurrent)
    
    # Windows has stricter limits on concurrent I/O
    if is_windows():
        concurrent = min(concurrent, MAX_CONCURRENT // 2)
    
    # Enforce limits
    concurrent = max(MIN_CONCURRENT, min(concurrent, MAX_CONCURRENT))
    
    return concurrent


# ============================================================================
# PUBLIC API
# ============================================================================

def calculate_optimal_workers() -> Tuple[int, int]:
    """
    Calculate optimal Pyrogram client settings based on system resources.
    
    IMPORTANT: Call this BEFORE creating the Pyrogram Client instance.
    The returned values should be passed to Client.__init__.
    
    Returns:
        Tuple of (workers, max_concurrent_transmissions)
    
    Example:
        workers, max_concurrent = calculate_optimal_workers()
        client = Client(
            "bot",
            workers=workers,
            max_concurrent_transmissions=max_concurrent
        )
    """
    # Gather system info
    cpu_count = get_cpu_count()
    memory_gb = get_available_memory_gb()
    
    # Calculate optimal settings
    workers = calculate_workers(cpu_count, memory_gb)
    max_concurrent = calculate_concurrent_transmissions(cpu_count, memory_gb)
    
    # Log the decision
    logger.info(
        f"Resource calculation: "
        f"CPUs={cpu_count}, "
        f"RAM={memory_gb:.1f}GB, "
        f"workers={workers}, "
        f"max_concurrent={max_concurrent}"
    )
    
    return workers, max_concurrent


def get_system_info() -> dict:
    """
    Get detailed system information for debugging.
    
    Returns:
        Dictionary with system details
    """
    cpu_count = get_cpu_count()
    memory_gb = get_available_memory_gb()
    workers, max_concurrent = calculate_optimal_workers()
    
    info = {
        "platform": platform.system(),
        "platform_version": platform.version(),
        "cpu_count": cpu_count,
        "memory_available_gb": round(memory_gb, 2),
        "calculated_workers": workers,
        "calculated_max_concurrent": max_concurrent,
        "psutil_available": PSUTIL_AVAILABLE,
    }
    
    if PSUTIL_AVAILABLE:
        try:
            mem = psutil.virtual_memory()
            info["memory_total_gb"] = round(mem.total / (1024 ** 3), 2)
            info["memory_percent_used"] = mem.percent
        except Exception:
            pass
    
    return info


# Module-level calculation for easy import
# Usage: from resource_calculator import OPTIMAL_WORKERS, OPTIMAL_CONCURRENT
_workers, _concurrent = calculate_optimal_workers()
OPTIMAL_WORKERS = _workers
OPTIMAL_CONCURRENT = _concurrent
