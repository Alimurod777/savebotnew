"""
Core Module - Telegram Media Download Engine & Pyrofork Utilities

PUBLIC API (use these in TechVJ/):
    from core import DownloadService, DownloadRequest, DownloadResult
    from core import get_download_service, init_download_service
    from core import safe_send_message, safe_reply, safe_handler
    from core import split_message, split_caption, escape_markdown
    
    # Entity-safe messaging (prevents ENTITY_BOUNDS_INVALID)
    from core import entity_safe_send_message, entity_safe_send_photo
    from core import split_with_entities, split_caption_with_entities
    
    # Album handling
    from core import AlbumHandler, clone_album, safe_get_media_group
    
    # Retry utilities
    from core import with_retry, handle_floodwait, RetryContext

INTERNAL MODULES (do NOT import directly in TechVJ/):
    - download_manager, download_worker, download_queue
    - telegram_downloader, progress_manager
    - state_store, mongo_store, session_manager
    - models (internal), client_factory
"""

# ==================== PUBLIC API ====================
# Bot code should ONLY import these

# IMPORTANT: Import pyrofork_compat FIRST — it patches Pyrofork's
# utils.get_reply_to() parameter name bug on import.
import core.pyrofork_compat  # noqa: F401 — side-effect: monkey-patches get_reply_to

from core.api import (
    DownloadService,
    DownloadRequest,
    DownloadResult,
    DownloadStatus,
    ProgressEvent,
    get_download_service,
    init_download_service,
)

# Reply compatibility layer (version-agnostic reply wrapper)
from core.reply_compat import (
    build_reply_kwargs,
    build_reply_kwargs_from_message,
    build_link_preview_kwargs,
)

# Safe message sending with silent reply fallback
from core.safe_send import (
    safe_send_message,
    safe_reply,
    safe_edit_message,
    safe_send_photo,
    safe_send_video,
    safe_send_document,
    safe_handler,
    # Entity-safe variants (prevent ENTITY_BOUNDS_INVALID)
    entity_safe_send_message,
    entity_safe_send_photo,
    entity_safe_send_video,
    entity_safe_send_document,
)

# Message utilities
from core.message_utils import (
    MAX_MESSAGE_LENGTH,
    MAX_CAPTION_LENGTH,
    split_text,
    split_caption,
    escape_markdown,
    sanitize_markdown,
    normalize_poll_to_text,
)

# Handler management
from core.handler_manager import (
    get_handler_manager,
    cleanup_on_disconnect,
    cleanup_global,
)

# Entity validation and splitting
from core.entity_validator import (
    validate_entities,
    sanitize_entities,
    prepare_entities_for_send,
    split_with_entities,
    split_caption_with_entities,
    utf16_length,
    SplitChunk,
)

# Album handling
from core.album_handler import (
    AlbumHandler,
    AlbumCollector,
    Album,
    clone_album,
    safe_get_media_group,
)

# Pyrofork compatibility
from core.pyrofork_compat import (
    safe_get_media_group as pyrofork_safe_get_media_group,
    safe_send_media_group,
    is_pyrofork,
    get_pyrogram_version,
)

# Retry utilities
from core.retry_utils import (
    with_retry,
    handle_floodwait,
    RetryContext,
    retry_operation,
    is_transient_error,
    is_fatal_error,
)

# Safe message/media copying (entity-only, no Markdown)
from core.safe_copy import (
    safe_copy_text_message,
    safe_copy_media_message,
    safe_copy_album,
    safe_forward_or_copy,
    CopyResult,
)

# Entity-first splitter (production-grade, zero ENTITY_BOUNDS_INVALID)
from core.entity_splitter import (
    split_text_with_entities,
    split_caption_with_overflow,
    prepare_message_chunks,
    prepare_caption_kwargs,
    TextChunk,
    utf16_len,
)

# Entity-first sender (100% hyperlink-safe)
from core.entity_sender import (
    send_text_with_entities,
    send_media_with_entities,
    send_album_with_entities,
    copy_message_with_entities,
    send_long_text,
    send_photo_with_caption,
    send_video_with_caption,
    send_document_with_caption,
)

# Hardening guards (additive validation layer)
from core.guards import (
    # Error classification
    ErrorCategory,
    ErrorDiagnostic,
    classify_error,
    # Channel validation
    validate_channel_access,
    ChannelValidationResult,
    clear_channel_cache,
    # Message range safety
    safe_get_messages_range,
    MessageRangeResult,
    # Topic-aware retrieval
    get_topic_message_boundaries,
    safe_get_topic_messages,
    smart_get_topic_range,
    get_all_topic_messages,
    TopicRangeResult,
    is_message_in_topic,
    # Entity validation
    validate_entities_before_send,
    # Timeout-aware operations
    guarded_edit_message,
    guarded_delete_messages,
    # High-level wrapper
    guarded_copy_operation,
)

# Environment detection (Colab, Kaggle, local)
from core.environment import (
    detect_environment,
    RuntimeEnvironment,
    get_base_path,
    get_sessions_path,
    get_downloads_path,
    get_temp_path,
    get_local_db_path,
    is_colab,
    is_kaggle,
    is_local,
    get_memory_aware_strategy,
)

# Premium-aware limits
from core.premium_logic import (
    get_caption_limit,
    get_file_size_limit,
    PREMIUM_CAPTION_LIMIT,
    STANDARD_CAPTION_LIMIT,
    PREMIUM_FILE_SIZE_LIMIT,
    STANDARD_FILE_SIZE_LIMIT,
)

# Smart dual-mode renderer (Markdown vs Entity)
from core.smart_renderer import (
    SmartRenderer,
    RenderMode,
    from_text_and_entities,
    from_message,
    smart_send_message,
    smart_send_media,
)

# Migration engine (premium-aware repost system)
from core.migration_engine import (
    MigrationEngine,
    migration_engine,
)

# Per-user flood control
from core.flood_controller import (
    FloodController,
    flood_controller,
)

# Smart MongoDB cache (RAM-first, write-back)
from core.mongo_cache import (
    MongoCache,
    mongo_cache,
    CachedUser,
)

# ==================== INTERNAL (backwards compatibility) ====================
# These are for internal use - will be deprecated for direct import

from core.models import (
    DownloadTask,
    DownloadStatus as _InternalDownloadStatus,
    ProgressEvent as _InternalProgressEvent,
    DownloadResult as _InternalDownloadResult,
    SpeedTracker,
    SHUTDOWN_SENTINEL,
)

__all__ = [
    # Public API - Download service
    'DownloadService',
    'DownloadRequest',
    'DownloadResult',
    'DownloadStatus',
    'ProgressEvent',
    'get_download_service',
    'init_download_service',
    
    # Reply compatibility (version-agnostic)
    'build_reply_kwargs',
    'build_reply_kwargs_from_message',
    'build_link_preview_kwargs',
    
    # Safe message sending (silent reply fallback)
    'safe_send_message',
    'safe_reply',
    'safe_edit_message',
    'safe_send_photo',
    'safe_send_video',
    'safe_send_document',
    'safe_handler',
    
    # Entity-safe message sending (prevents ENTITY_BOUNDS_INVALID)
    'entity_safe_send_message',
    'entity_safe_send_photo',
    'entity_safe_send_video',
    'entity_safe_send_document',
    
    # Entity validation and splitting
    'validate_entities',
    'sanitize_entities',
    'prepare_entities_for_send',
    'split_with_entities',
    'split_caption_with_entities',
    'utf16_length',
    'SplitChunk',
    
    # Album handling
    'AlbumHandler',
    'AlbumCollector',
    'Album',
    'clone_album',
    'safe_get_media_group',
    'safe_send_media_group',
    
    # Pyrofork compatibility
    'is_pyrofork',
    'get_pyrogram_version',
    
    # Retry utilities
    'with_retry',
    'handle_floodwait',
    'RetryContext',
    'retry_operation',
    'is_transient_error',
    'is_fatal_error',
    
    # Safe copying (entity-only)
    'safe_copy_text_message',
    'safe_copy_media_message',
    'safe_copy_album',
    'safe_forward_or_copy',
    'CopyResult',
    
    # Entity-first splitter (production-grade)
    'split_text_with_entities',
    'split_caption_with_overflow',
    'prepare_message_chunks',
    'prepare_caption_kwargs',
    'TextChunk',
    'utf16_len',
    
    # Entity-first sender (100% hyperlink-safe)
    'send_text_with_entities',
    'send_media_with_entities',
    'send_album_with_entities',
    'copy_message_with_entities',
    'send_long_text',
    'send_photo_with_caption',
    'send_video_with_caption',
    'send_document_with_caption',
    
    # Message utilities
    'MAX_MESSAGE_LENGTH',
    'MAX_CAPTION_LENGTH',
    'split_text',
    'split_caption',
    'escape_markdown',
    'sanitize_markdown',
    'normalize_poll_to_text',
    
    # Handler management
    'get_handler_manager',
    'cleanup_on_disconnect',
    'cleanup_global',
    
    # Internal (backwards compatibility - avoid direct use)
    'DownloadTask',
    'SpeedTracker',
    'SHUTDOWN_SENTINEL',
    
    # Hardening guards (additive validation layer)
    'ErrorCategory',
    'ErrorDiagnostic',
    'classify_error',
    'validate_channel_access',
    'ChannelValidationResult',
    'clear_channel_cache',
    'safe_get_messages_range',
    'MessageRangeResult',
    'get_topic_message_boundaries',
    'safe_get_topic_messages',
    'validate_entities_before_send',
    'guarded_edit_message',
    'guarded_delete_messages',
    'guarded_copy_operation',
    
    # Environment detection
    'detect_environment',
    'RuntimeEnvironment',
    'get_base_path',
    'get_sessions_path',
    'get_downloads_path',
    'get_temp_path',
    'get_local_db_path',
    'is_colab',
    'is_kaggle',
    'is_local',
    'get_memory_aware_strategy',
    
    # Premium-aware limits
    'get_caption_limit',
    'get_file_size_limit',
    'PREMIUM_CAPTION_LIMIT',
    'STANDARD_CAPTION_LIMIT',
    'PREMIUM_FILE_SIZE_LIMIT',
    'STANDARD_FILE_SIZE_LIMIT',
    
    # Smart renderer
    'SmartRenderer',
    'RenderMode',
    'from_text_and_entities',
    'from_message',
    'smart_send_message',
    'smart_send_media',
    
    # Migration engine (premium-aware repost)
    'MigrationEngine',
    'migration_engine',
    
    # Per-user flood control
    'FloodController',
    'flood_controller',
    
    # Smart MongoDB cache
    'MongoCache',
    'mongo_cache',
    'CachedUser',
]
