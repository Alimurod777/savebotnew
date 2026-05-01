# FFmpeg utilities with static binary support
# Supports Windows, Linux, and macOS without requiring system ffmpeg installation
# Uses static-ffmpeg for automatic binary download

import os
import sys
import platform
import subprocess
import shutil

# Try to import ffmpeg-python
try:
    import ffmpeg
    HAS_FFMPEG_PYTHON = True
except ImportError:
    HAS_FFMPEG_PYTHON = False

# Try to import static-ffmpeg for automatic binary download
try:
    import static_ffmpeg
    HAS_STATIC_FFMPEG = True
except ImportError:
    HAS_STATIC_FFMPEG = False

# Global ffmpeg path cache
_ffmpeg_path = None
_ffprobe_path = None
_static_ffmpeg_initialized = False


def _init_static_ffmpeg():
    """
    Initialize static-ffmpeg and download binaries if needed.
    This is called once on first use.
    """
    global _static_ffmpeg_initialized
    
    if _static_ffmpeg_initialized:
        return
    
    if HAS_STATIC_FFMPEG:
        try:
            # This downloads ffmpeg/ffprobe if not already present
            static_ffmpeg.add_paths()
            _static_ffmpeg_initialized = True
        except Exception:
            pass


def get_ffmpeg_path():
    """
    Get the path to ffmpeg binary.
    Searches in order:
    1. static-ffmpeg bundled binary (cross-platform, auto-downloads)
    2. System ffmpeg
    3. Local ffmpeg in project directory
    
    Returns:
        Path to ffmpeg binary or None if not found
    """
    global _ffmpeg_path
    
    if _ffmpeg_path is not None:
        return _ffmpeg_path
    
    # Try static-ffmpeg first (auto-downloads if needed)
    if HAS_STATIC_FFMPEG:
        try:
            _init_static_ffmpeg()
            # static-ffmpeg adds to PATH, so check which again
            ffmpeg_exe = shutil.which('ffmpeg')
            if ffmpeg_exe:
                _ffmpeg_path = ffmpeg_exe
                return _ffmpeg_path
        except Exception:
            pass
    
    # Try system ffmpeg
    system_ffmpeg = shutil.which('ffmpeg')
    if system_ffmpeg:
        _ffmpeg_path = system_ffmpeg
        return _ffmpeg_path
    
    # Try local ffmpeg in project directory
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    if platform.system() == 'Windows':
        local_paths = [
            os.path.join(project_dir, 'ffmpeg', 'ffmpeg.exe'),
            os.path.join(project_dir, 'ffmpeg.exe'),
            os.path.join(project_dir, 'bin', 'ffmpeg.exe'),
        ]
    else:
        local_paths = [
            os.path.join(project_dir, 'ffmpeg', 'ffmpeg'),
            os.path.join(project_dir, 'ffmpeg'),
            os.path.join(project_dir, 'bin', 'ffmpeg'),
            '/usr/bin/ffmpeg',
            '/usr/local/bin/ffmpeg',
        ]
    
    for path in local_paths:
        if os.path.exists(path) and os.access(path, os.X_OK):
            _ffmpeg_path = path
            return _ffmpeg_path
    
    return None


def get_ffprobe_path():
    """
    Get the path to ffprobe binary.
    
    Returns:
        Path to ffprobe binary or None if not found
    """
    global _ffprobe_path
    
    if _ffprobe_path is not None:
        return _ffprobe_path
    
    # Initialize static-ffmpeg first (adds both ffmpeg and ffprobe to PATH)
    if HAS_STATIC_FFMPEG:
        try:
            _init_static_ffmpeg()
        except Exception:
            pass
    
    # Try system ffprobe
    system_ffprobe = shutil.which('ffprobe')
    if system_ffprobe:
        _ffprobe_path = system_ffprobe
        return _ffprobe_path
    
    # Try to derive from ffmpeg path
    ffmpeg_path = get_ffmpeg_path()
    if ffmpeg_path:
        ffmpeg_dir = os.path.dirname(ffmpeg_path)
        if platform.system() == 'Windows':
            ffprobe_path = os.path.join(ffmpeg_dir, 'ffprobe.exe')
        else:
            ffprobe_path = os.path.join(ffmpeg_dir, 'ffprobe')
        
        if os.path.exists(ffprobe_path):
            _ffprobe_path = ffprobe_path
            return _ffprobe_path
    
    # Try local paths
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    if platform.system() == 'Windows':
        local_paths = [
            os.path.join(project_dir, 'ffmpeg', 'ffprobe.exe'),
            os.path.join(project_dir, 'ffprobe.exe'),
            os.path.join(project_dir, 'bin', 'ffprobe.exe'),
        ]
    else:
        local_paths = [
            os.path.join(project_dir, 'ffmpeg', 'ffprobe'),
            os.path.join(project_dir, 'ffprobe'),
            os.path.join(project_dir, 'bin', 'ffprobe'),
            '/usr/bin/ffprobe',
            '/usr/local/bin/ffprobe',
        ]
    
    for path in local_paths:
        if os.path.exists(path) and os.access(path, os.X_OK):
            _ffprobe_path = path
            return _ffprobe_path
    
    return None


def is_ffmpeg_available():
    """
    Check if ffmpeg is available.
    
    Returns:
        True if ffmpeg is available
    """
    return get_ffmpeg_path() is not None


def get_video_info(file_path):
    """
    Get video file information using ffprobe or ffmpeg.
    
    Args:
        file_path: Path to video file
        
    Returns:
        Dictionary with video info (duration, width, height, codec, etc.)
        or None if failed
    """
    if not os.path.exists(file_path):
        return None
    
    # Try ffmpeg-python first
    if HAS_FFMPEG_PYTHON:
        try:
            ffmpeg_path = get_ffmpeg_path()
            if ffmpeg_path:
                # Set ffmpeg path for ffmpeg-python
                os.environ['FFMPEG_BINARY'] = ffmpeg_path
            
            probe = ffmpeg.probe(file_path)
            video_stream = next(
                (s for s in probe['streams'] if s['codec_type'] == 'video'),
                None
            )
            audio_stream = next(
                (s for s in probe['streams'] if s['codec_type'] == 'audio'),
                None
            )
            
            info = {
                'duration': float(probe['format'].get('duration', 0)),
                'size': int(probe['format'].get('size', 0)),
                'bit_rate': int(probe['format'].get('bit_rate', 0)),
                'format_name': probe['format'].get('format_name', ''),
            }
            
            if video_stream:
                info.update({
                    'width': video_stream.get('width', 0),
                    'height': video_stream.get('height', 0),
                    'video_codec': video_stream.get('codec_name', ''),
                    'fps': eval(video_stream.get('r_frame_rate', '0/1')) if '/' in video_stream.get('r_frame_rate', '') else 0,
                })
            
            if audio_stream:
                info.update({
                    'audio_codec': audio_stream.get('codec_name', ''),
                    'audio_channels': audio_stream.get('channels', 0),
                    'sample_rate': int(audio_stream.get('sample_rate', 0)),
                })
            
            return info
        except Exception:
            pass
    
    # Fallback to direct ffprobe call
    ffprobe_path = get_ffprobe_path()
    if not ffprobe_path:
        return None
    
    try:
        cmd = [
            ffprobe_path,
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format',
            '-show_streams',
            file_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            import json
            data = json.loads(result.stdout)
            
            video_stream = next(
                (s for s in data.get('streams', []) if s.get('codec_type') == 'video'),
                None
            )
            
            info = {
                'duration': float(data['format'].get('duration', 0)),
                'size': int(data['format'].get('size', 0)),
            }
            
            if video_stream:
                info.update({
                    'width': video_stream.get('width', 0),
                    'height': video_stream.get('height', 0),
                })
            
            return info
    except Exception:
        pass
    
    return None


def extract_thumbnail(video_path, output_path=None, time_offset=1):
    """
    Extract a thumbnail from a video file.
    
    Args:
        video_path: Path to video file
        output_path: Path for output thumbnail (default: auto-generated)
        time_offset: Time in seconds to extract frame from
        
    Returns:
        Path to thumbnail file or None if failed
    """
    if not os.path.exists(video_path):
        return None
    
    ffmpeg_path = get_ffmpeg_path()
    if not ffmpeg_path:
        return None
    
    if output_path is None:
        base_name = os.path.splitext(video_path)[0]
        output_path = f"{base_name}_thumb.jpg"
    
    try:
        if HAS_FFMPEG_PYTHON:
            os.environ['FFMPEG_BINARY'] = ffmpeg_path
            (
                ffmpeg
                .input(video_path, ss=time_offset)
                .output(output_path, vframes=1, format='image2', vcodec='mjpeg')
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
        else:
            cmd = [
                ffmpeg_path,
                '-y',
                '-ss', str(time_offset),
                '-i', video_path,
                '-vframes', '1',
                '-f', 'image2',
                output_path
            ]
            subprocess.run(cmd, capture_output=True, timeout=30)
        
        if os.path.exists(output_path):
            return output_path
    except Exception:
        pass
    
    return None


def compress_video(input_path, output_path=None, target_size_mb=None, crf=23):
    """
    Compress a video file.
    
    Args:
        input_path: Path to input video
        output_path: Path for output video (default: auto-generated)
        target_size_mb: Target file size in MB (optional)
        crf: Constant Rate Factor (0-51, lower = better quality)
        
    Returns:
        Path to compressed video or None if failed
    """
    if not os.path.exists(input_path):
        return None
    
    ffmpeg_path = get_ffmpeg_path()
    if not ffmpeg_path:
        return None
    
    if output_path is None:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_compressed{ext}"
    
    try:
        if HAS_FFMPEG_PYTHON:
            os.environ['FFMPEG_BINARY'] = ffmpeg_path
            (
                ffmpeg
                .input(input_path)
                .output(output_path, vcodec='libx264', crf=crf, acodec='aac')
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
        else:
            cmd = [
                ffmpeg_path,
                '-y',
                '-i', input_path,
                '-vcodec', 'libx264',
                '-crf', str(crf),
                '-acodec', 'aac',
                output_path
            ]
            subprocess.run(cmd, capture_output=True, timeout=3600)
        
        if os.path.exists(output_path):
            return output_path
    except Exception:
        pass
    
    return None


def split_video_by_duration(input_path, segment_duration=3600, output_dir=None):
    """
    Split a video into segments by duration.
    
    Args:
        input_path: Path to input video
        segment_duration: Duration of each segment in seconds (default: 1 hour)
        output_dir: Directory for output files (default: same as input)
        
    Returns:
        List of paths to segment files or empty list if failed
    """
    if not os.path.exists(input_path):
        return []
    
    ffmpeg_path = get_ffmpeg_path()
    if not ffmpeg_path:
        return []
    
    if output_dir is None:
        output_dir = os.path.dirname(input_path) or '.'
    
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    ext = os.path.splitext(input_path)[1]
    output_pattern = os.path.join(output_dir, f"{base_name}_part%03d{ext}")
    
    try:
        cmd = [
            ffmpeg_path,
            '-y',
            '-i', input_path,
            '-c', 'copy',
            '-map', '0',
            '-segment_time', str(segment_duration),
            '-f', 'segment',
            '-reset_timestamps', '1',
            output_pattern
        ]
        
        result = subprocess.run(cmd, capture_output=True, timeout=3600)
        
        # Find all created segments
        segments = []
        for i in range(1000):  # Max 1000 segments
            segment_path = os.path.join(output_dir, f"{base_name}_part{i:03d}{ext}")
            if os.path.exists(segment_path):
                segments.append(segment_path)
            elif i > 0:
                break
        
        return segments
    except Exception:
        pass
    
    return []


def get_ffmpeg_info():
    """
    Get information about ffmpeg installation.
    
    Returns:
        Dictionary with ffmpeg info
    """
    info = {
        'ffmpeg_available': is_ffmpeg_available(),
        'ffmpeg_path': get_ffmpeg_path(),
        'ffprobe_path': get_ffprobe_path(),
        'has_ffmpeg_python': HAS_FFMPEG_PYTHON,
        'has_static_ffmpeg': HAS_STATIC_FFMPEG,
        'platform': platform.system(),
    }
    
    # Try to get ffmpeg version
    ffmpeg_path = get_ffmpeg_path()
    if ffmpeg_path:
        try:
            result = subprocess.run(
                [ffmpeg_path, '-version'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                first_line = result.stdout.split('\n')[0]
                info['ffmpeg_version'] = first_line
        except Exception:
            pass
    
    return info


def ensure_ffmpeg():
    """
    Ensure ffmpeg is available, downloading if necessary.
    Call this at startup to pre-download ffmpeg binaries.
    
    Returns:
        True if ffmpeg is available, False otherwise
    """
    if HAS_STATIC_FFMPEG:
        try:
            _init_static_ffmpeg()
        except Exception:
            pass
    
    return is_ffmpeg_available()
