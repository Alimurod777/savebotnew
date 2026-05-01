# File splitting utilities for handling files larger than 2GB
# Telegram has a 2GB file size limit, so we split large files into chunks

import os
import shutil
import math
import platform
import tempfile
import subprocess
import logging
import json

logger = logging.getLogger(__name__)

# Try to import py7zr for 7-zip support (cross-platform)
try:
    import py7zr
    HAS_PY7ZR = True
except ImportError:
    HAS_PY7ZR = False

# Maximum file size for Telegram (2000 MiB - Telegram's actual limit)
MAX_TELEGRAM_FILE_SIZE = 2000 * 1024 * 1024  # 2000 MiB in bytes (Telegram's real limit)

# Chunk size (1900 MiB to leave safety margin)
DEFAULT_CHUNK_SIZE = 1900 * 1024 * 1024  # 1900 MiB in bytes

# Archive extensions that benefit from 7z splitting
ARCHIVE_EXTENSIONS = {'.zip', '.rar', '.tar', '.gz', '.7z', '.bz2', '.xz'}


def get_balanced_chunk_size(file_size, chunk_size=DEFAULT_CHUNK_SIZE):
    """
    Return a per-part size that stays under chunk_size and avoids tiny tail parts.

    Example:
    - Old behavior for 3.81 GB with 1.90 GB chunks -> 1.90 + 1.90 + 0.01 GB
    - New behavior -> 1.27 + 1.27 + 1.27 GB
    """
    if file_size <= 0:
        return chunk_size
    if file_size <= chunk_size:
        return file_size

    num_chunks = math.ceil(file_size / chunk_size)
    return math.ceil(file_size / num_chunks)


def get_file_size(file_path):
    """
    Get the size of a file in bytes.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File size in bytes, or 0 if file doesn't exist
    """
    try:
        return os.path.getsize(file_path)
    except OSError:
        return 0


def needs_splitting(file_path):
    """
    Check if a file needs to be split for Telegram upload.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if file is larger than MAX_TELEGRAM_FILE_SIZE
    """
    return get_file_size(file_path) > MAX_TELEGRAM_FILE_SIZE


def get_chunk_count(file_path, chunk_size=DEFAULT_CHUNK_SIZE):
    """
    Calculate how many chunks a file will be split into.
    
    Args:
        file_path: Path to the file
        chunk_size: Size of each chunk in bytes
        
    Returns:
        Number of chunks needed
    """
    file_size = get_file_size(file_path)
    if file_size == 0:
        return 0
    effective_chunk_size = get_balanced_chunk_size(file_size, chunk_size)
    return math.ceil(file_size / effective_chunk_size)


def split_file(file_path, chunk_size=DEFAULT_CHUNK_SIZE):
    """
    Split a file into chunks smaller than the specified size.
    
    Uses streaming I/O with 8 MB buffer - never loads full chunk into RAM.
    Safe for 2 GB+ files on Colab/Kaggle (limited RAM).
    
    Args:
        file_path: Path to the file to split
        chunk_size: Maximum size of each chunk in bytes (default: 1.9GB)
        
    Returns:
        List of paths to the chunk files
        
    Raises:
        FileNotFoundError: If the source file doesn't exist
        IOError: If there's an error reading/writing files
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_size = get_file_size(file_path)
    
    # If file is smaller than chunk size, no splitting needed
    if file_size <= chunk_size:
        return [file_path]
    
    # Get file name and extension
    file_dir = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    name, ext = os.path.splitext(file_name)
    
    effective_chunk_size = get_balanced_chunk_size(file_size, chunk_size)

    # Calculate number of chunks
    num_chunks = math.ceil(file_size / effective_chunk_size)
    
    # Streaming buffer size (8 MB - balances RAM usage and I/O speed)
    BUFFER_SIZE = 8 * 1024 * 1024
    
    chunk_paths = []
    
    with open(file_path, 'rb') as source_file:
        for i in range(num_chunks):
            chunk_name = f"{name}.part{i + 1}{ext}"
            chunk_path = os.path.join(file_dir, chunk_name) if file_dir else chunk_name
            
            bytes_written = 0
            with open(chunk_path, 'wb') as chunk_file:
                while bytes_written < effective_chunk_size:
                    to_read = min(BUFFER_SIZE, effective_chunk_size - bytes_written)
                    data = source_file.read(to_read)
                    if not data:
                        break
                    chunk_file.write(data)
                    bytes_written += len(data)
            
            chunk_paths.append(chunk_path)
    
    return chunk_paths


def split_file_iter(file_path, chunk_size=DEFAULT_CHUNK_SIZE):
    """
    Generator: bitta chunk yaratadi -> yield qiladi -> keyingisiga o'tadi.
    
    split_file() dan farqi: HAMMA chunk'ni oldindan yaratmaydi.
    Caller har bir chunk'ni upload qilib, o'chirib, keyin keyingisini so'raydi.
    Disk: original + 1 chunk (eski usulda original + BARCHA chunk).
    
    File handle yield paytida YOPIQ turadi - upload 10 daqiqa davom
    etsa ham fayl qulflanmaydi (Windows antivirus, Colab timeout xavfsiz).
    
    Yields:
        (chunk_path, part_number, total_parts) tuple
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_size = get_file_size(file_path)
    
    if file_size <= chunk_size:
        yield file_path, 1, 1
        return
    
    file_dir = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    name, ext = os.path.splitext(file_name)
    effective_chunk_size = get_balanced_chunk_size(file_size, chunk_size)
    num_chunks = math.ceil(file_size / effective_chunk_size)
    
    BUFFER_SIZE = 8 * 1024 * 1024
    offset = 0
    
    for i in range(num_chunks):
        chunk_name = f"{name}.part{i + 1}{ext}"
        chunk_path = os.path.join(file_dir, chunk_name) if file_dir else chunk_name
        
        # Faylni FAQAT chunk yozish uchun ochamiz, yozib yopamiz
        bytes_written = 0
        with open(file_path, 'rb') as source_file:
            source_file.seek(offset)
            with open(chunk_path, 'wb') as chunk_file:
                while bytes_written < effective_chunk_size:
                    to_read = min(BUFFER_SIZE, effective_chunk_size - bytes_written)
                    data = source_file.read(to_read)
                    if not data:
                        break
                    chunk_file.write(data)
                    bytes_written += len(data)
        
        offset += bytes_written
        
        # File handle YOPIQ - upload qancha vaqt ketsa ham xavfsiz
        yield chunk_path, i + 1, num_chunks


def get_chunk_info(file_path, chunk_size=DEFAULT_CHUNK_SIZE):
    """
    Get information about how a file will be split.
    
    Args:
        file_path: Path to the file
        chunk_size: Size of each chunk in bytes
        
    Returns:
        Dictionary with file info:
        {
            'file_path': original file path,
            'file_size': total file size in bytes,
            'file_size_readable': human-readable file size,
            'chunk_size': chunk size in bytes,
            'chunk_count': number of chunks,
            'needs_splitting': whether file needs to be split
        }
    """
    file_size = get_file_size(file_path)
    
    effective_chunk_size = get_balanced_chunk_size(file_size, chunk_size)

    return {
        'file_path': file_path,
        'file_size': file_size,
        'file_size_readable': format_size(file_size),
        'chunk_size': effective_chunk_size,
        'chunk_size_readable': format_size(effective_chunk_size),
        'chunk_count': get_chunk_count(file_path, chunk_size),
        'needs_splitting': file_size > MAX_TELEGRAM_FILE_SIZE
    }


def format_size(size_bytes):
    """
    Convert bytes to human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Human-readable string (e.g., "1.5 GB")
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ("B", "KB", "MB", "GB", "TB")
    i = 0
    size = float(size_bytes)
    
    while size >= 1024 and i < len(size_names) - 1:
        size /= 1024
        i += 1
    
    return f"{size:.2f} {size_names[i]}"


def cleanup_chunks(chunk_paths):
    """
    Delete chunk files after successful upload.
    
    Args:
        chunk_paths: List of chunk file paths to delete
    """
    for chunk_path in chunk_paths:
        try:
            if os.path.exists(chunk_path):
                os.remove(chunk_path)
        except OSError:
            pass  # Ignore errors when cleaning up


def is_archive(file_path):
    """
    Check if a file is an archive based on extension.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if file has archive extension
    """
    _, ext = os.path.splitext(file_path.lower())
    return ext in ARCHIVE_EXTENSIONS


def split_file_7z(file_path, chunk_size=DEFAULT_CHUNK_SIZE):
    """
    Split a file into 7z archive volumes (cross-platform).
    Uses py7zr library which works on Windows, Linux, and macOS.
    
    Args:
        file_path: Path to the file to split
        chunk_size: Maximum size of each chunk in bytes (default: 1.9GB)
        
    Returns:
        List of paths to the archive volume files
        
    Raises:
        ImportError: If py7zr is not installed
        FileNotFoundError: If the source file doesn't exist
    """
    if not HAS_PY7ZR:
        raise ImportError("py7zr is required for 7z splitting. Install with: pip install py7zr")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_size = get_file_size(file_path)
    
    # If file is smaller than chunk size, no splitting needed
    if file_size <= chunk_size:
        return [file_path]
    
    # Get file info
    file_dir = os.path.dirname(file_path) or '.'
    file_name = os.path.basename(file_path)
    name, _ = os.path.splitext(file_name)
    
    # Create output archive name
    archive_base = os.path.join(file_dir, f"{name}.7z")
    
    # Calculate number of volumes needed
    num_volumes = math.ceil(file_size / chunk_size)
    
    # Create multi-volume 7z archive
    with py7zr.SevenZipFile(archive_base, 'w') as archive:
        archive.write(file_path, file_name)
    
    # py7zr doesn't support volume splitting directly, so we use binary splitting
    # for the created archive if it's still too large
    archive_size = get_file_size(archive_base)
    
    if archive_size > chunk_size:
        # Split the 7z file using binary method
        chunk_paths = split_file(archive_base, chunk_size)
        # Remove the original archive
        if os.path.exists(archive_base) and archive_base not in chunk_paths:
            os.remove(archive_base)
        return chunk_paths
    
    return [archive_base]


def split_large_file(file_path, chunk_size=DEFAULT_CHUNK_SIZE, use_7z=False):
    """
    Smart file splitting that chooses the best method.
    
    Args:
        file_path: Path to the file to split
        chunk_size: Maximum size of each chunk in bytes
        use_7z: Force using 7z compression (useful for archives)
        
    Returns:
        List of paths to the chunk/archive files
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_size = get_file_size(file_path)
    
    # If file is smaller than chunk size, no splitting needed
    if file_size <= chunk_size:
        return [file_path]
    
    # Use 7z for archives if available and requested
    if use_7z and HAS_PY7ZR:
        try:
            return split_file_7z(file_path, chunk_size)
        except Exception:
            # Fall back to binary splitting
            pass
    
    # Default to binary splitting (works for all file types)
    return split_file(file_path, chunk_size)


def get_system_info():
    """
    Get system information for debugging.
    
    Returns:
        Dictionary with system info
    """
    return {
        'platform': platform.system(),
        'platform_release': platform.release(),
        'platform_version': platform.version(),
        'architecture': platform.machine(),
        'python_version': platform.python_version(),
        'has_py7zr': HAS_PY7ZR
    }


# ==================== FFMPEG-BASED VIDEO SPLITTING ====================

def get_video_duration(video_path: str) -> float:
    """Get video duration using ffprobe."""
    try:
        cmd = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'json', video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            return float(data.get('format', {}).get('duration', 0))
    except Exception as e:
        logger.warning(f"ffprobe error: {e}")
    return 0


def is_video_file(file_path: str) -> bool:
    """Check if file is a video based on extension."""
    video_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.webm', '.flv', '.wmv', '.m4v'}
    _, ext = os.path.splitext(file_path.lower())
    return ext in video_extensions


def split_video_ffmpeg(
    video_path: str,
    max_size_bytes: int = DEFAULT_CHUNK_SIZE,
    output_dir: str = None
) -> list:
    """
    Split video into parts using ffmpeg (preserves quality).
    
    Unlike binary split, this creates playable video files.
    Based on chelaxian/tg-ytdlp-bot implementation.
    
    Args:
        video_path: Path to video file
        max_size_bytes: Maximum size per part in bytes
        output_dir: Output directory (default: same as video)
    
    Returns:
        List of paths to video parts
    """
    if not os.path.exists(video_path):
        raise FileNotFoundError(f"Video not found: {video_path}")
    
    file_size = get_file_size(video_path)
    
    # No split needed
    if file_size <= max_size_bytes:
        return [video_path]
    
    # Get video duration
    duration = get_video_duration(video_path)
    if duration <= 0:
        logger.warning("Could not get video duration, falling back to binary split")
        return split_file(video_path, max_size_bytes)
    
    # Calculate balanced number of parts to avoid a tiny last segment
    effective_chunk_size = get_balanced_chunk_size(file_size, max_size_bytes)
    num_parts = math.ceil(file_size / effective_chunk_size)
    part_duration = duration / num_parts
    
    # Prepare paths
    if output_dir is None:
        output_dir = os.path.dirname(video_path)
    
    video_name = os.path.splitext(os.path.basename(video_path))[0]
    ext = os.path.splitext(video_path)[1] or '.mp4'
    
    part_paths = []
    
    logger.info(
        "Splitting video into %d balanced parts (~%s each)",
        num_parts,
        format_size(effective_chunk_size),
    )
    
    for i in range(num_parts):
        start_time = i * part_duration
        end_time = min((i + 1) * part_duration, duration)
        part_name = f"{video_name}.part{i + 1}{ext}"
        part_path = os.path.join(output_dir, part_name)
        
        try:
            # Fast copy only - no re-encoding (too slow for large files)
            success = _ffmpeg_extract_part(
                video_path, part_path,
                start_time, end_time - start_time
            )
            
            if success and os.path.exists(part_path) and os.path.getsize(part_path) > 0:
                # Check if part is still too large
                part_size = os.path.getsize(part_path)
                if part_size > max_size_bytes:
                    logger.warning(f"Part {i + 1} still too large ({format_size(part_size)}), may need further splitting")
                
                part_paths.append(part_path)
                logger.info(f"Created part {i + 1}/{num_parts}: {format_size(part_size)}")
            else:
                logger.error(f"Failed to create part {i + 1}")
                
        except Exception as e:
            logger.error(f"Error creating part {i + 1}: {e}")
    
    if not part_paths:
        logger.warning("FFmpeg split failed, falling back to binary split")
        return split_file(video_path, max_size_bytes)
    
    return part_paths


def get_ffmpeg_path() -> str:
    """Get ffmpeg path - use static-ffmpeg if available."""
    try:
        import static_ffmpeg
        static_ffmpeg.add_paths()
        return 'ffmpeg'
    except ImportError:
        pass
    
    # Fallback to system ffmpeg
    import shutil
    ffmpeg = shutil.which('ffmpeg')
    return ffmpeg if ffmpeg else 'ffmpeg'


def _ffmpeg_extract_part(
    input_path: str,
    output_path: str,
    start_time: float,
    duration: float
) -> bool:
    """
    Extract a part of video using ffmpeg with -c copy (NO re-encoding).
    
    This is fast and preserves quality. Re-encoding is avoided because:
    - Takes 10-30x longer
    - Not necessary for splitting
    """
    try:
        ffmpeg = get_ffmpeg_path()
        
        # ONLY use -c copy - fast, no quality loss
        cmd = [
            ffmpeg, '-y',
            '-ss', str(start_time),  # Seek BEFORE input (faster)
            '-i', input_path,
            '-t', str(duration),
            '-c', 'copy',
            '-avoid_negative_ts', 'make_zero',
            output_path
        ]
        
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=120  # 2 minutes max (copy is fast)
        )
        
        return result.returncode == 0 and os.path.exists(output_path)
        
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg timed out")
        return False
    except Exception as e:
        logger.error(f"FFmpeg error: {e}")
        return False


def split_video_ffmpeg_segment(
    video_path: str,
    max_size_bytes: int = DEFAULT_CHUNK_SIZE,
    output_dir: str = None
) -> list:
    """
    Split video using ffmpeg -f segment with -c copy (NO re-encoding).
    
    Each segment is independently playable.
    Uses lossless stream-based splitting: -c copy -map 0 -f segment.
    
    Args:
        video_path: Path to video file
        max_size_bytes: Maximum size per segment
        output_dir: Output directory (default: same as input)
    
    Returns:
        List of paths to playable video segments
    """
    if not os.path.exists(video_path):
        raise FileNotFoundError(f"Video not found: {video_path}")
    
    file_size = get_file_size(video_path)
    if file_size <= max_size_bytes:
        return [video_path]
    
    duration = get_video_duration(video_path)
    if duration <= 0:
        logger.warning("Could not determine video duration for segment splitting")
        return split_file(video_path, max_size_bytes)
    
    if output_dir is None:
        output_dir = os.path.dirname(video_path) or '.'
    
    video_name = os.path.splitext(os.path.basename(video_path))[0]
    ext = os.path.splitext(video_path)[1] or '.mp4'
    
    # Estimate segment duration from a balanced target size
    effective_chunk_size = get_balanced_chunk_size(file_size, max_size_bytes)
    num_parts = math.ceil(file_size / effective_chunk_size)
    segment_duration = max(1, int(round(duration / num_parts)))
    
    output_pattern = os.path.join(output_dir, f"{video_name}_seg%03d{ext}")
    
    try:
        ffmpeg_bin = get_ffmpeg_path()
        
        cmd = [
            ffmpeg_bin, '-y',
            '-i', video_path,
            '-c', 'copy',        # No re-encoding — lossless
            '-map', '0',          # Copy all streams
            '-f', 'segment',      # Segment muxer
            '-segment_time', str(segment_duration),
            '-reset_timestamps', '1',  # Each segment starts at t=0
            '-break_non_keyframes', '0',  # Split only at keyframes
            output_pattern
        ]
        
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=300  # 5 minutes max for segmenting
        )
        
        if result.returncode != 0:
            logger.warning(f"ffmpeg segment failed: {result.stderr[:500]}")
            return split_video_ffmpeg(video_path, max_size_bytes, output_dir)
        
        # Collect generated segment files
        import glob as glob_mod
        pattern = os.path.join(output_dir, f"{video_name}_seg*{ext}")
        segments = sorted(glob_mod.glob(pattern))
        
        if not segments:
            logger.warning("No segments produced, falling back to time-based split")
            return split_video_ffmpeg(video_path, max_size_bytes, output_dir)
        
        # Verify segments are valid (non-empty)
        valid_segments = [s for s in segments if os.path.getsize(s) > 0]
        
        if not valid_segments:
            logger.warning("All segments empty, falling back to binary split")
            return split_file(video_path, max_size_bytes)
        
        logger.info(f"Video split into {len(valid_segments)} segments via ffmpeg -f segment")
        return valid_segments
        
    except subprocess.TimeoutExpired:
        logger.error("ffmpeg segment timed out")
        return split_file(video_path, max_size_bytes)
    except FileNotFoundError:
        logger.warning("ffmpeg not found, using binary split")
        return split_file(video_path, max_size_bytes)
    except Exception as e:
        logger.error(f"ffmpeg segment error: {e}")
        return split_file(video_path, max_size_bytes)


def split_large_video(
    video_path: str,
    max_size_bytes: int = DEFAULT_CHUNK_SIZE
) -> list:
    """
    Smart video splitting — prefers balanced time-based ffmpeg parts.
    Falls back to segment split, then binary split.
    
    Args:
        video_path: Path to video file
        max_size_bytes: Maximum size per part
    
    Returns:
        List of paths to video parts
    """
    if not os.path.exists(video_path):
        raise FileNotFoundError(f"File not found: {video_path}")
    
    file_size = get_file_size(video_path)
    
    if file_size <= max_size_bytes:
        return [video_path]
    
    # Check if ffmpeg is available
    ffmpeg_bin = get_ffmpeg_path()
    has_ffmpeg = False
    if ffmpeg_bin:
        try:
            result = subprocess.run(
                [ffmpeg_bin, '-version'], capture_output=True, timeout=5
            )
            has_ffmpeg = result.returncode == 0
        except Exception:
            pass
    
    if has_ffmpeg and is_video_file(video_path):
        try:
            return split_video_ffmpeg(video_path, max_size_bytes)
        except Exception as e:
            logger.warning(f"FFmpeg time-based split failed: {e}, trying segment-based")
        try:
            return split_video_ffmpeg_segment(video_path, max_size_bytes)
        except Exception as e:
            logger.warning(f"FFmpeg segment split failed: {e}, using binary split")
    
    # Fallback to binary split
    return split_file(video_path, max_size_bytes)
