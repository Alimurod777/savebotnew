"""
Adaptive Resource Calculator for Pyrogram Client Configuration

Calculates optimal worker and concurrent transmission settings based on
available system resources. MUST be called BEFORE Pyrogram Client initialization.

Author: WOODcraft Production-Grade Refactor
Python: 3.10+
Platform: Windows-optimized (but works cross-platform)

DESIGN PRINCIPLES:
1. Calculate ONCE at startup, before Client initialization
2. No runtime worker mutation (Pyrogram doesn't support it)
3. Conservative defaults that work on limited systems
4. Windows-specific optimizations for selector event loop

PYROGRAM SETTINGS EXPLAINED:
- workers: Number of threads for handling updates/callbacks
  - More workers = more concurrent message handling
  - Too many wastes memory, too few causes processing delay
  - Recommendation: 4 * CPU cores, capped at 64

- max_concurrent_transmissions: Parallel file transfers
  - More = faster when downloading/uploading multiple files
  - Each uses network bandwidth and memory for buffers
  - Windows selector loop has FD limit, so cap at 16
"""

import os
import logging
import platform
from typing import Tuple, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# ============================================================================
# TRY TO IMPORT PSUTIL
# ============================================================================

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not available - using conservative resource estimates")


# ============================================================================
# CONSTANTS - Tuned for production stability
# ============================================================================

# Worker limits
MIN_WORKERS = 4
MAX_WORKERS = 64
DEFAULT_WORKERS = 16

# Concurrent transmission limits
MIN_CONCURRENT = 2
MAX_CONCURRENT_LINUX = 32
MAX_CONCURRENT_WINDOWS = 16  # Windows selector has 512 FD limit

# Memory thresholds (in GB)
CRITICAL_MEMORY = 1.0   # Below this: minimum everything
LOW_MEMORY = 2.0        # Below this: reduced settings
NORMAL_MEMORY = 4.0     # Below this: standard settings
HIGH_MEMORY = 8.0       # Above this: allow maximum settings

# Estimated memory usage per component (MB)
MEMORY_PER_WORKER_MB = 3
MEMORY_PER_TRANSMISSION_MB = 30  # Buffer per active transfer


# ============================================================================
# RESULT DATACLASS
# ============================================================================

@dataclass
class ResourceConfig:
    """
    Calculated resource configuration for Pyrogram.
    
    Pass these values to Client() initialization.
    """
    workers: int
    max_concurrent_transmissions: int
    
    # Metadata (for logging/debugging)
    cpu_count: int = 0
    memory_available_gb: float = 0.0
    memory_total_gb: float = 0.0
    platform: str = ""
    calculation_notes: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Client kwargs."""
        return {
            "workers": self.workers,
            "max_concurrent_transmissions": self.max_concurrent_transmissions
        }
    
    def __str__(self) -> str:
        return (
            f"ResourceConfig(workers={self.workers}, "
            f"max_concurrent={self.max_concurrent_transmissions}, "
            f"cpu={self.cpu_count}, mem={self.memory_available_gb:.1f}GB)"
        )


# ============================================================================
# SYSTEM DETECTION FUNCTIONS
# ============================================================================

def get_cpu_count() -> int:
    """
    Get the number of CPU cores available.
    
    Returns:
        Number of logical CPU cores (minimum 1)
    """
    try:
        count = os.cpu_count()
        return max(1, count or 1)
    except Exception as e:
        logger.warning(f"Failed to get CPU count: {e}")
        return 1


def get_memory_info() -> Tuple[float, float]:
    """
    Get available and total memory in GB.
    
    Returns:
        Tuple of (available_gb, total_gb)
    """
    if PSUTIL_AVAILABLE:
        try:
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024 ** 3)
            total_gb = mem.total / (1024 ** 3)
            return available_gb, total_gb
        except Exception as e:
            logger.warning(f"Failed to get memory info: {e}")
    
    # Conservative fallback - assume 4GB total, 2GB available
    return 2.0, 4.0


def is_windows() -> bool:
    """Check if running on Windows."""
    return platform.system() == "Windows"


def is_low_end_system(cpu_count: int, memory_gb: float) -> bool:
    """
    Check if this is a low-end system that needs conservative settings.
    """
    return cpu_count <= 2 or memory_gb < LOW_MEMORY


# ============================================================================
# CALCULATION FUNCTIONS
# ============================================================================

def calculate_workers(cpu_count: int, memory_gb: float) -> int:
    """
    Calculate optimal Pyrogram worker count.
    
    Formula:
    - Base: 4 * CPU cores
    - Scale by available memory
    - Enforce min/max limits
    
    Args:
        cpu_count: Number of CPU cores
        memory_gb: Available memory in GB
        
    Returns:
        Optimal worker count
    """
    # Base calculation: 4 workers per CPU core
    base_workers = cpu_count * 4
    
    # Memory-based adjustment
    if memory_gb < CRITICAL_MEMORY:
        # Critical low memory - use absolute minimum
        return MIN_WORKERS
    elif memory_gb < LOW_MEMORY:
        # Low memory - use half of CPU-based recommendation
        workers = max(MIN_WORKERS, base_workers // 2)
    elif memory_gb < NORMAL_MEMORY:
        # Normal memory - use 75% of recommendation
        workers = int(base_workers * 0.75)
    elif memory_gb >= HIGH_MEMORY:
        # High memory - allow higher count
        workers = int(base_workers * 1.25)
    else:
        # Standard memory
        workers = base_workers
    
    # Calculate memory-based maximum
    # Assume we want to leave at least 1GB for the system and buffers
    usable_memory_mb = max(0, (memory_gb - 1.0) * 1024)
    memory_based_max = int(usable_memory_mb / MEMORY_PER_WORKER_MB)
    
    # Take the minimum of CPU-based and memory-based
    workers = min(workers, memory_based_max)
    
    # Enforce limits
    return max(MIN_WORKERS, min(workers, MAX_WORKERS))


def calculate_concurrent_transmissions(
    cpu_count: int, 
    memory_gb: float,
    on_windows: bool = False
) -> int:
    """
    Calculate optimal max_concurrent_transmissions.
    
    Formula:
    - Base: 2 * CPU cores
    - Scale by memory (each transfer needs buffer)
    - Cap lower on Windows due to selector limitations
    
    Args:
        cpu_count: Number of CPU cores
        memory_gb: Available memory in GB
        on_windows: Whether running on Windows
        
    Returns:
        Optimal concurrent transmission count
    """
    # Determine platform maximum
    platform_max = MAX_CONCURRENT_WINDOWS if on_windows else MAX_CONCURRENT_LINUX
    
    # Base calculation: 2 concurrent per CPU core
    base_concurrent = cpu_count * 2
    
    # Memory-based calculation
    if memory_gb < CRITICAL_MEMORY:
        return MIN_CONCURRENT
    
    # Calculate how many transfers we can buffer
    # Assume 1GB reserved for system, rest available for transfers
    usable_memory_mb = max(0, (memory_gb - 1.0) * 1024)
    memory_based_max = int(usable_memory_mb / MEMORY_PER_TRANSMISSION_MB)
    
    # Take minimum of all constraints
    concurrent = min(base_concurrent, memory_based_max, platform_max)
    
    # Enforce limits
    return max(MIN_CONCURRENT, min(concurrent, platform_max))


# ============================================================================
# MAIN API
# ============================================================================

def calculate_optimal_config() -> ResourceConfig:
    """
    Calculate optimal Pyrogram client configuration.
    
    IMPORTANT: Call this BEFORE creating the Pyrogram Client instance.
    
    Returns:
        ResourceConfig with calculated settings
        
    Example:
        config = calculate_optimal_config()
        client = Client(
            "bot",
            workers=config.workers,
            max_concurrent_transmissions=config.max_concurrent_transmissions
        )
    """
    # Gather system info
    cpu_count = get_cpu_count()
    available_gb, total_gb = get_memory_info()
    on_windows = is_windows()
    
    # Build notes for debugging
    notes = []
    
    if not PSUTIL_AVAILABLE:
        notes.append("psutil unavailable, using estimates")
    
    if is_low_end_system(cpu_count, available_gb):
        notes.append("low-end system detected")
    
    if on_windows:
        notes.append("Windows platform, limiting concurrent")
    
    # Calculate settings
    workers = calculate_workers(cpu_count, available_gb)
    max_concurrent = calculate_concurrent_transmissions(
        cpu_count, 
        available_gb, 
        on_windows
    )
    
    config = ResourceConfig(
        workers=workers,
        max_concurrent_transmissions=max_concurrent,
        cpu_count=cpu_count,
        memory_available_gb=available_gb,
        memory_total_gb=total_gb,
        platform=platform.system(),
        calculation_notes="; ".join(notes) if notes else "standard config"
    )
    
    logger.info(
        f"Resource calculation: {config.workers} workers, "
        f"{config.max_concurrent_transmissions} concurrent transmissions | "
        f"CPU: {cpu_count}, RAM: {available_gb:.1f}/{total_gb:.1f} GB"
    )
    
    return config


def calculate_optimal_workers() -> Tuple[int, int]:
    """
    Legacy function for backwards compatibility.
    
    Returns:
        Tuple of (workers, max_concurrent_transmissions)
    """
    config = calculate_optimal_config()
    return config.workers, config.max_concurrent_transmissions


def get_system_info() -> Dict[str, Any]:
    """
    Get detailed system information for debugging.
    
    Returns:
        Dictionary with system details
    """
    cpu_count = get_cpu_count()
    available_gb, total_gb = get_memory_info()
    config = calculate_optimal_config()
    
    info = {
        "platform": platform.system(),
        "platform_version": platform.version(),
        "python_version": platform.python_version(),
        "cpu_count": cpu_count,
        "memory_total_gb": round(total_gb, 2),
        "memory_available_gb": round(available_gb, 2),
        "calculated_workers": config.workers,
        "calculated_max_concurrent": config.max_concurrent_transmissions,
        "psutil_available": PSUTIL_AVAILABLE,
        "is_windows": is_windows(),
        "is_low_end": is_low_end_system(cpu_count, available_gb),
    }
    
    if PSUTIL_AVAILABLE:
        try:
            mem = psutil.virtual_memory()
            info["memory_percent_used"] = mem.percent
        except Exception:
            pass
    
    return info


# ============================================================================
# MODULE-LEVEL CONSTANTS FOR EASY IMPORT
# ============================================================================

# Calculate once at module load
# Usage: from resource_calculator_v2 import OPTIMAL_CONFIG
_config = calculate_optimal_config()
OPTIMAL_CONFIG = _config
OPTIMAL_WORKERS = _config.workers
OPTIMAL_CONCURRENT = _config.max_concurrent_transmissions


# ============================================================================
# CLI INTERFACE FOR TESTING
# ============================================================================

if __name__ == "__main__":
    import json
    
    print("=" * 60)
    print("System Resource Analysis")
    print("=" * 60)
    
    info = get_system_info()
    for key, value in info.items():
        print(f"  {key}: {value}")
    
    print("\n" + "=" * 60)
    print("Recommended Pyrogram Settings:")
    print("=" * 60)
    print(f"  workers={OPTIMAL_WORKERS}")
    print(f"  max_concurrent_transmissions={OPTIMAL_CONCURRENT}")
    print("=" * 60)
