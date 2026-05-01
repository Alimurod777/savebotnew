"""
Refactored Message Processing Pipeline

Key Architectural Changes:
1. Task-scoped sessions - created/destroyed per processing run
2. Album-aware iteration - skips to album end after processing
3. No MongoDB runtime sync - in-memory tracking only
4. No background schedulers - everything is request-driven
5. Deterministic timeouts on all Pyrogram calls
6. Clean separation: auth (database) vs runtime (in-memory)

This module provides the main processing functions that replace the
MongoDB-dependent implementations in save.py.
"""

import asyncio
import os
import shutil
import logging
from typing import Optional, List, Set, Tuple, Callable, Coroutine
from dataclasses import dataclass

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.enums import MessageMediaType, ParseMode

from TechVJ.session_handler import (
    create_user_session,
    SessionInvalidError,
    SessionConnectionError,
)
from TechVJ.album_collector_v2 import (
    AlbumAwareIterator,
    AlbumBoundary,
    detect_album_boundary,
    process_album_pipeline,
    fetch_message,
    download_media_file,
    FATAL_SESSION_ERRORS,
)

logger = logging.getLogger(__name__)

# Temp directory for downloads
TEMP_DOWNLOAD_DIR = "downloads/temp"


@dataclass
class ProcessingStats:
    """Statistics for a processing run."""
    total: int = 0
    processed: int = 0
    failed: int = 0
    albums_processed: int = 0
    skipped_album_messages: int = 0
    cancelled: bool = False
    error: Optional[str] = None


def get_message_type(msg: Message) -> str:
    """Detect message type."""
    if hasattr(msg, 'media') and msg.media:
        type_map = {
            MessageMediaType.PHOTO: "Photo",
            MessageMediaType.VIDEO: "Video",
            MessageMediaType.DOCUMENT: "Document",
            MessageMediaType.AUDIO: "Audio",
            MessageMediaType.VOICE: "Voice",
            MessageMediaType.VIDEO_NOTE: "VideoNote",
            MessageMediaType.ANIMATION: "Animation",
            MessageMediaType.STICKER: "Sticker",
            MessageMediaType.POLL: "Poll",
        }
        return type_map.get(msg.media, "Document")
    return "Text" if msg.text else "Unknown"


async def process_message_range(
    bot_client: Client,
    session_string: str,
    user_id: int,
    target_chat_id: int,
    source_chat_id: int,
    message_ids: List[int],
    reply_to_message_id: int,
    check_cancelled: Callable[[], Coroutine],
    status_callback: Optional[Callable[[str], Coroutine]] = None,
) -> ProcessingStats:
    """
    Process a range of messages with album-aware iteration.
    
    This is the main entry point for processing multiple messages.
    Handles albums atomically and skips to album end after processing.
    
    Args:
        bot_client: Bot client for sending messages
        session_string: User's session string
        user_id: User ID
        target_chat_id: Where to send processed messages
        source_chat_id: Source channel/chat ID
        message_ids: List of message IDs to process
        reply_to_message_id: Message ID to reply to
        check_cancelled: Async callable returning True if cancelled
        status_callback: Optional async callback for status updates
    
    Returns:
        ProcessingStats with results
        
    Architecture Notes:
        - Session is created ONCE for the entire range
        - Session is guaranteed to be stopped on return
        - Album detection happens inline during iteration
        - No persistent state - tracking is in-memory only
    """
    stats = ProcessingStats(total=len(message_ids))
    iterator = AlbumAwareIterator(message_ids)
    
    if not message_ids:
        return stats
    
    try:
        async with create_user_session(session_string, user_id) as acc:
            
            while True:
                # Check cancellation
                if await check_cancelled():
                    stats.cancelled = True
                    return stats
                
                # Get next message ID
                message_id = iterator.next()
                if message_id is None:
                    break  # Done
                
                # Status update
                if status_callback:
                    await status_callback(
                        f"Processing {iterator.current_position}/{iterator.total}\n"
                        f"ID: {message_id} | Done: {stats.processed} | Failed: {stats.failed}"
                    )
                
                try:
                    # Try to detect if this is part of an album
                    boundary = await detect_album_boundary(acc, source_chat_id, message_id)
                    
                    if boundary:
                        # This is an album - check if already processed
                        if iterator.is_album_processed(boundary.media_group_id):
                            # Skip - already processed this album
                            iterator.skip_to(boundary.next_message_id)
                            stats.skipped_album_messages += 1
                            continue
                        
                        # Process entire album
                        success, status, _ = await process_album_pipeline(
                            bot_client=bot_client,
                            session_string=session_string,
                            user_id=user_id,
                            target_chat_id=target_chat_id,
                            source_chat_id=source_chat_id,
                            message_id=message_id,
                            reply_to_message_id=reply_to_message_id,
                            check_cancelled=check_cancelled,
                            sent_albums=iterator.processed_albums,
                        )
                        
                        if success:
                            stats.albums_processed += 1
                            stats.processed += 1
                            iterator.mark_album_processed(boundary.media_group_id)
                        elif status == "cancelled":
                            stats.cancelled = True
                            return stats
                        elif status == "already_sent":
                            stats.processed += 1  # Count as success
                        else:
                            stats.failed += 1
                        
                        # Skip remaining album messages
                        iterator.skip_to(boundary.next_message_id)
                        
                    else:
                        # Not an album - process single message
                        success = await process_single_message(
                            bot_client=bot_client,
                            user_session=acc,
                            user_id=user_id,
                            target_chat_id=target_chat_id,
                            source_chat_id=source_chat_id,
                            message_id=message_id,
                            reply_to_message_id=reply_to_message_id,
                            check_cancelled=check_cancelled,
                        )
                        
                        if success:
                            stats.processed += 1
                        else:
                            stats.failed += 1
                    
                    # Rate limiting delay
                    await asyncio.sleep(1.5)
                    
                except SessionInvalidError as e:
                    stats.error = f"Session invalid: {e}"
                    return stats
                except asyncio.CancelledError:
                    stats.cancelled = True
                    return stats
                except Exception as e:
                    logger.warning(f"Error processing message {message_id}: {e}")
                    stats.failed += 1
                    continue
            
            return stats
            
    except SessionInvalidError as e:
        stats.error = f"Session invalid: {e}"
        return stats
    except SessionConnectionError as e:
        stats.error = f"Connection error: {e}"
        return stats
    except asyncio.CancelledError:
        stats.cancelled = True
        return stats
    except Exception as e:
        stats.error = f"Unexpected error: {e}"
        return stats


async def process_single_message(
    bot_client: Client,
    user_session: Client,
    user_id: int,
    target_chat_id: int,
    source_chat_id: int,
    message_id: int,
    reply_to_message_id: int,
    check_cancelled: Callable[[], Coroutine],
) -> bool:
    """
    Process a single non-album message.
    
    Args:
        bot_client: Bot client for sending
        user_session: Already-connected user session
        user_id: User ID
        target_chat_id: Where to send
        source_chat_id: Source chat
        message_id: Message to process
        reply_to_message_id: Reply target
        check_cancelled: Cancellation checker
    
    Returns:
        True if processed successfully
    """
    if await check_cancelled():
        return False
    
    # Fetch message
    msg = await fetch_message(user_session, source_chat_id, message_id)
    if not msg:
        return False
    
    msg_type = get_message_type(msg)
    
    # Handle text
    if msg_type == "Text":
        return await send_text_message(bot_client, target_chat_id, msg, reply_to_message_id)
    
    # Handle poll
    if msg_type == "Poll":
        return await send_poll_as_text(bot_client, target_chat_id, msg, reply_to_message_id)
    
    # Handle media
    return await download_and_send_media(
        bot_client, user_session, target_chat_id,
        msg, msg_type, reply_to_message_id, check_cancelled
    )


async def send_text_message(
    client: Client,
    chat_id: int,
    msg: Message,
    reply_to_message_id: int
) -> bool:
    """
    Send a text message with entity preservation (NEVER uses parse_mode).
    
    SAFE: Uses only MessageEntity to prevent ENTITY_BOUNDS_INVALID.
    """
    try:
        text = msg.text or ""
        if not text:
            return True
        
        # Get raw text and entities - NEVER convert to Markdown
        entities = list(msg.entities) if msg.entities else None
        
        from core.entity_validator import (
            prepare_entities_for_send,
            split_with_entities,
            utf16_length,
            MESSAGE_LIMIT
        )
        
        text_len = utf16_length(text)
        
        if text_len <= MESSAGE_LIMIT:
            # Short message - validate and send
            safe_entities = prepare_entities_for_send(text, entities) if entities else None
            kwargs = {'chat_id': chat_id, 'text': text, 'disable_web_page_preview': True}
            if safe_entities:
                kwargs['entities'] = safe_entities
            if reply_to_message_id:
                kwargs['reply_to_message_id'] = reply_to_message_id
            await client.send_message(**kwargs)
        else:
            # Long message - entity-aware splitting
            chunks = split_with_entities(text, entities or [], MESSAGE_LIMIT)
            for i, chunk in enumerate(chunks):
                kwargs = {'chat_id': chat_id, 'text': chunk.text, 'disable_web_page_preview': True}
                if chunk.entities:
                    kwargs['entities'] = chunk.entities
                if i == 0 and reply_to_message_id:
                    kwargs['reply_to_message_id'] = reply_to_message_id
                await client.send_message(**kwargs)
                if i < len(chunks) - 1:
                    await asyncio.sleep(0.3)
        
        return True
    except Exception as e:
        logger.warning(f"Error sending text: {e}")
        return False


async def send_poll_as_text(
    client: Client,
    chat_id: int,
    msg: Message,
    reply_to_message_id: int
) -> bool:
    """Send poll as formatted text."""
    try:
        poll = msg.poll
        if not poll:
            return False
        
        text = f"**{poll.question}**\n\n"
        for i, opt in enumerate(poll.options):
            prefix = "✅ " if hasattr(poll, 'correct_option_id') and i == poll.correct_option_id else "○ "
            text += f"{prefix}{opt.text}\n"
        
        if hasattr(poll, 'explanation') and poll.explanation:
            text += f"\n**Explanation:** {poll.explanation}"
        
        await client.send_message(
            chat_id, text,
            reply_to_message_id=reply_to_message_id,
            parse_mode=ParseMode.DISABLED
        )
        return True
    except Exception as e:
        logger.warning(f"Error sending poll: {e}")
        return False


async def download_and_send_media(
    bot_client: Client,
    user_session: Client,
    target_chat_id: int,
    msg: Message,
    msg_type: str,
    reply_to_message_id: int,
    check_cancelled: Callable,
) -> bool:
    """
    Download media from source and send via bot.
    
    Guarantees cleanup of downloaded file.
    """
    file_path = None
    thumb_path = None
    
    try:
        if await check_cancelled():
            return False
        
        # Create temp dir
        os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
        
        # Download
        file_path = await download_media_file(
            user_session, msg, TEMP_DOWNLOAD_DIR + "/"
        )
        
        if not file_path or not os.path.exists(file_path):
            return False
        
        if await check_cancelled():
            return False
        
        # Get caption and entities - NEVER convert to Markdown
        caption = msg.caption if msg.caption else None
        caption_entities = list(msg.caption_entities) if msg.caption_entities else None
        
        # Import entity-safe utilities
        from core.entity_validator import prepare_entities_for_send, utf16_length, CAPTION_LIMIT
        
        # Prepare caption kwargs
        caption_kwargs = {}
        if caption:
            cap_len = utf16_length(caption)
            if cap_len <= CAPTION_LIMIT:
                caption_kwargs['caption'] = caption
                if caption_entities:
                    safe_entities = prepare_entities_for_send(caption, caption_entities)
                    if safe_entities:
                        caption_kwargs['caption_entities'] = safe_entities
            else:
                # Truncate long caption (overflow handling would need separate message)
                caption_kwargs['caption'] = caption[:CAPTION_LIMIT-3] + "..."
        
        # Send based on type - NEVER use parse_mode with caption_entities
        if msg_type == "Photo":
            await bot_client.send_photo(target_chat_id, file_path, reply_to_message_id=reply_to_message_id, **caption_kwargs)
        elif msg_type == "Video":
            if msg.video and msg.video.thumbs:
                try:
                    thumb_path = await user_session.download_media(msg.video.thumbs[0].file_id)
                except Exception:
                    pass
            await bot_client.send_video(
                target_chat_id, file_path,
                duration=msg.video.duration if msg.video else None,
                thumb=thumb_path,
                reply_to_message_id=reply_to_message_id,
                **caption_kwargs
            )
        elif msg_type == "Audio":
            await bot_client.send_audio(target_chat_id, file_path, reply_to_message_id=reply_to_message_id, **caption_kwargs)
        elif msg_type == "Voice":
            await bot_client.send_voice(target_chat_id, file_path, reply_to_message_id=reply_to_message_id, **caption_kwargs)
        elif msg_type == "VideoNote":
            await bot_client.send_video_note(target_chat_id, file_path, reply_to_message_id=reply_to_message_id)
        elif msg_type == "Animation":
            await bot_client.send_animation(target_chat_id, file_path, reply_to_message_id=reply_to_message_id, **caption_kwargs)
        elif msg_type == "Sticker":
            await bot_client.send_sticker(target_chat_id, file_path, reply_to_message_id=reply_to_message_id)
        else:  # Document
            await bot_client.send_document(target_chat_id, file_path, reply_to_message_id=reply_to_message_id, **caption_kwargs)
        
        return True
        
    except FATAL_SESSION_ERRORS:
        raise  # Re-raise to signal invalid session
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"Error in download_and_send_media: {e}")
        return False
    finally:
        # GUARANTEED CLEANUP
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass
        if thumb_path and os.path.exists(thumb_path):
            try:
                os.remove(thumb_path)
            except Exception:
                pass


# =============================================================================
# TASK MANAGEMENT (Simplified, No MongoDB)
# =============================================================================

class TaskRegistry:
    """
    Simple in-memory task registry for cancellation support.
    No persistence, no cross-loop sharing.
    Event-loop-safe: resets lock on loop change.
    """
    
    def __init__(self):
        self._tasks: dict = {}  # user_id -> set of task names
        self._cancel_flags: dict = {}  # user_id -> bool
        self._lock: Optional[asyncio.Lock] = None
        self._bound_loop_id: Optional[int] = None
    
    def _get_lock(self) -> asyncio.Lock:
        """Get or create lock bound to current event loop."""
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
        except RuntimeError:
            # No running loop - return fresh lock (not stored)
            return asyncio.Lock()
        
        if self._bound_loop_id is not None and self._bound_loop_id != current_loop_id:
            self._lock = None
        
        self._bound_loop_id = current_loop_id
        
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
    
    async def register(self, user_id: int, task_name: str) -> None:
        """Register a task for a user."""
        async with self._get_lock():
            if user_id not in self._tasks:
                self._tasks[user_id] = set()
                self._cancel_flags[user_id] = False
            self._tasks[user_id].add(task_name)
    
    async def unregister(self, user_id: int, task_name: str) -> None:
        """Unregister a task."""
        async with self._get_lock():
            if user_id in self._tasks:
                self._tasks[user_id].discard(task_name)
    
    async def request_cancel(self, user_id: int) -> int:
        """
        Request cancellation for all user tasks.
        Returns count of active tasks.
        """
        async with self._get_lock():
            self._cancel_flags[user_id] = True
            count = len(self._tasks.get(user_id, set()))
            return count
    
    async def should_cancel(self, user_id: int) -> bool:
        """Check if cancellation was requested."""
        return self._cancel_flags.get(user_id, False)
    
    async def reset_cancel(self, user_id: int) -> None:
        """Reset cancel flag for new operations."""
        async with self._get_lock():
            self._cancel_flags[user_id] = False
    
    async def get_active_count(self, user_id: int) -> int:
        """Get count of active tasks for user."""
        return len(self._tasks.get(user_id, set()))
    
    async def cleanup_user(self, user_id: int) -> None:
        """Clean up all state for a user."""
        async with self._get_lock():
            self._tasks.pop(user_id, None)
            self._cancel_flags.pop(user_id, None)


# Global task registry
task_registry = TaskRegistry()


# Initialize temp directory
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
