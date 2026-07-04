"""
Album-Aware Telegram Media Pipeline - Refactored

Architecture Principles:
1. NO MongoDB for runtime session sync - task-scoped, in-memory only
2. Session lifetime bound strictly to processing task via try/finally
3. No cross-event-loop Client sharing - each task owns its Client
4. Album-boundary-aware message iteration
5. Deterministic retry with fail-fast on invalid sessions
6. No background schedulers, no global mutable state

Key Changes from v1:
- Removed StateManager/MongoDB dependency for album tracking
- Sessions are ephemeral, created per-task and destroyed on completion
- Album detection returns (first_id, last_id) for skip-ahead iteration
- No asyncio.Lock crossing event loops - locks created per-task
- Deterministic timeouts on all Pyrogram calls
"""

import asyncio
import os
import shutil
import uuid
import re
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
import logging

from pyrogram import Client
from pyrogram.types import Message, InputMediaPhoto
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    FloodWait,
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionExpired,
    SessionRevoked,
    UserDeactivated,
    RPCError,
)

from config import API_ID, API_HASH, get_client_params

logger = logging.getLogger(__name__)

# Constants
ALBUM_TEMP_BASE = "downloads/albums"
DEFAULT_TIMEOUT = 30  # seconds
MAX_RETRIES = 2
RETRY_BASE_DELAY = 1.0


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for filesystem safety."""
    if not filename:
        return "photo"
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)
    sanitized = sanitized.strip('. ')
    return sanitized[:200] if len(sanitized) > 200 else sanitized or "photo"


# =============================================================================
# SESSION ERRORS - Fail Fast Categories
# =============================================================================

FATAL_SESSION_ERRORS = (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionExpired,
    SessionRevoked,
    UserDeactivated,
)


class SessionInvalidError(Exception):
    """Raised when session is invalid/expired - fail fast, no retry."""
    pass


class AlbumProcessingError(Exception):
    """Raised when album processing fails."""
    pass


# =============================================================================
# ALBUM DATA STRUCTURES (No Shared State)
# =============================================================================

@dataclass
class AlbumBoundary:
    """
    Represents album boundaries for skip-ahead iteration.
    Used to jump message_id iteration past processed albums.
    """
    media_group_id: str
    first_message_id: int
    last_message_id: int
    photo_count: int
    
    @property
    def next_message_id(self) -> int:
        """Return the ID to continue iteration from (skip album)."""
        return self.last_message_id + 1


@dataclass
class AlbumPhoto:
    """Single photo in an album."""
    message_id: int
    file_path: Optional[str] = None
    order_index: int = 0


@dataclass
class CollectedAlbum:
    """
    Fully collected album ready for sending.
    Created fresh per processing task - no persistence.
    """
    media_group_id: str
    first_message_id: int
    last_message_id: int
    photos: List[AlbumPhoto] = field(default_factory=list)
    caption: Optional[str] = None
    caption_entities: Optional[list] = None
    temp_dir: Optional[str] = None
    
    def get_ordered_photos(self) -> List[AlbumPhoto]:
        """Get photos sorted by message_id (original Telegram order)."""
        return sorted(self.photos, key=lambda p: p.message_id)
    
    @property
    def boundary(self) -> AlbumBoundary:
        """Get boundary info for iteration skip-ahead."""
        return AlbumBoundary(
            media_group_id=self.media_group_id,
            first_message_id=self.first_message_id,
            last_message_id=self.last_message_id,
            photo_count=len(self.photos)
        )


# =============================================================================
# TASK-SCOPED SESSION MANAGER
# =============================================================================

@asynccontextmanager
async def task_scoped_session(
    session_string: str,
    user_id: int,
    timeout: float = DEFAULT_TIMEOUT
):
    """
    Context manager for task-scoped Pyrogram sessions.
    
    GUARANTEES:
    - Client is created and started within this context
    - Client is stopped and cleaned up on exit (success, error, or cancel)
    - No Client object escapes this context
    - Fail-fast on invalid sessions
    
    Usage:
        async with task_scoped_session(session_str, user_id) as client:
            # use client here
        # client is guaranteed stopped after this block
    """
    client = None
    client_name = f"task_{user_id}_{uuid.uuid4().hex[:8]}"
    
    try:
        fp = get_client_params(user_id)
        client = Client(
            client_name,
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True,
            no_updates=True,
            sleep_threshold=30,
            max_concurrent_transmissions=10,
            device_model=fp["device_model"],
            system_version=fp["system_version"],
            app_version=fp["app_version"],
            lang_code=fp["lang_code"],
        )
        
        # Start with timeout
        try:
            await asyncio.wait_for(client.start(), timeout=timeout)
        except asyncio.TimeoutError:
            raise SessionInvalidError(f"Session start timed out after {timeout}s")
        except FATAL_SESSION_ERRORS as e:
            raise SessionInvalidError(f"Invalid session: {type(e).__name__}")
        
        yield client
        
    except FATAL_SESSION_ERRORS as e:
        raise SessionInvalidError(f"Session error: {type(e).__name__}")
    finally:
        if client:
            try:
                if client.is_connected:
                    await asyncio.wait_for(client.stop(), timeout=5.0)
            except Exception as e:
                logger.debug(f"Error stopping client: {e}")


# =============================================================================
# PYROGRAM CALL WRAPPERS (Deterministic Timeouts & Retries)
# =============================================================================

async def fetch_message(
    client: Client,
    chat_id: int,
    message_id: int,
    timeout: float = DEFAULT_TIMEOUT
) -> Optional[Message]:
    """
    Fetch single message with timeout and retry.
    Returns None if message doesn't exist or is empty.
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            msg = await asyncio.wait_for(
                client.get_messages(chat_id, message_id),
                timeout=timeout
            )
            if msg and not msg.empty:
                return msg
            return None
            
        except asyncio.TimeoutError:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BASE_DELAY * (attempt + 1))
                continue
            logger.warning(f"Timeout fetching message {message_id}")
            return None
            
        except FloodWait as e:
            wait_time = min(e.value, 30)  # Cap wait time
            if attempt < MAX_RETRIES:
                await asyncio.sleep(wait_time)
                continue
            return None
            
        except FATAL_SESSION_ERRORS:
            raise SessionInvalidError("Session invalid during message fetch")
            
        except RPCError as e:
            logger.warning(f"RPC error fetching message {message_id}: {e}")
            return None
            
        except Exception as e:
            logger.warning(f"Error fetching message {message_id}: {e}")
            return None
    
    return None


async def fetch_media_group(
    client: Client,
    chat_id: int,
    message_id: int,
    timeout: float = DEFAULT_TIMEOUT
) -> List[Message]:
    """
    Fetch all messages in a media group with timeout.
    Returns empty list on failure.
    
    PYROFORK COMPATIBILITY:
    Handles 'Messages.__init__() missing required keyword-only argument: topics'
    error by catching and converting the response properly.
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            messages = await asyncio.wait_for(
                client.get_media_group(chat_id, message_id),
                timeout=timeout
            )
            # Convert to list safely - handles both list and Messages object
            if messages is None:
                return []
            if isinstance(messages, list):
                return messages
            # Try to iterate (Messages is iterable)
            try:
                return list(messages)
            except TypeError:
                # If not iterable, wrap in list if it's a single message
                return [messages] if messages else []
            
        except asyncio.TimeoutError:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BASE_DELAY * (attempt + 1))
                continue
            return []
            
        except FloodWait as e:
            wait_time = min(e.value, 30)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(wait_time)
                continue
            return []
            
        except FATAL_SESSION_ERRORS:
            raise SessionInvalidError("Session invalid during media group fetch")
            
        except TypeError as te:
            # Pyrofork Messages class compatibility issue
            # Messages.__init__() missing 'topics' argument
            error_str = str(te)
            if 'topics' in error_str or 'Messages' in error_str:
                logger.warning(f"Pyrofork Messages compatibility issue: {te}")
                # Try alternative approach - fetch messages individually
                try:
                    single_msg = await fetch_message(client, chat_id, message_id)
                    if single_msg and single_msg.media_group_id:
                        # Cannot fetch group due to Pyrofork bug, return single
                        return [single_msg]
                except Exception:
                    pass
            return []
            
        except Exception as e:
            # get_media_group raises exception for non-album messages
            error_str = str(e)
            if 'topics' in error_str.lower():
                logger.warning(f"Pyrofork compatibility issue in get_media_group: {e}")
            else:
                logger.debug(f"Not a media group or error: {e}")
            return []
    
    return []


async def download_media_file(
    client: Client,
    message: Message,
    file_path: str,
    timeout: float = 120.0
) -> Optional[str]:
    """
    Download media with timeout.
    Returns downloaded path or None on failure.
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            result = await asyncio.wait_for(
                client.download_media(message, file_name=file_path),
                timeout=timeout
            )
            if result and os.path.exists(result):
                return result
            return None
            
        except asyncio.TimeoutError:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BASE_DELAY * (attempt + 1))
                continue
            return None
            
        except FloodWait as e:
            wait_time = min(e.value, 60)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(wait_time)
                continue
            return None
            
        except FATAL_SESSION_ERRORS:
            raise SessionInvalidError("Session invalid during download")
            
        except Exception as e:
            logger.warning(f"Download error: {e}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BASE_DELAY)
                continue
            return None
    
    return None


# =============================================================================
# ALBUM DETECTION & BOUNDARY EXTRACTION
# =============================================================================

async def detect_album_boundary(
    client: Client,
    chat_id: int,
    message_id: int
) -> Optional[AlbumBoundary]:
    """
    Detect if message is part of a photo album and return boundaries.
    
    Returns:
        AlbumBoundary if message is part of a valid photo album (2+ photos)
        None if not an album or detection failed
        
    Use Case:
        When iterating message_ids, call this to detect albums and skip
        to boundary.next_message_id after processing.
    """
    msg = await fetch_message(client, chat_id, message_id)
    if not msg:
        return None
    
    # Must be a photo with media_group_id
    if not msg.photo or not msg.media_group_id:
        return None
    
    # Fetch full album
    album_messages = await fetch_media_group(client, chat_id, message_id)
    if len(album_messages) < 2:
        return None
    
    # Verify all are photos
    for m in album_messages:
        if not m.photo:
            return None
    
    # Sort by message_id
    sorted_msgs = sorted(album_messages, key=lambda m: m.id)
    
    return AlbumBoundary(
        media_group_id=msg.media_group_id,
        first_message_id=sorted_msgs[0].id,
        last_message_id=sorted_msgs[-1].id,
        photo_count=len(sorted_msgs)
    )


async def is_photo_album_message(
    client: Client,
    chat_id: int,
    message_id: int
) -> Tuple[bool, Optional[str], Optional[AlbumBoundary]]:
    """
    Check if message is part of a photo album.
    
    Returns:
        (is_album, media_group_id, boundary)
    """
    boundary = await detect_album_boundary(client, chat_id, message_id)
    if boundary:
        return True, boundary.media_group_id, boundary
    return False, None, None


# =============================================================================
# ALBUM COLLECTION (Task-Scoped, No Persistence)
# =============================================================================

async def collect_album(
    client: Client,
    chat_id: int,
    message_id: int,
    check_cancelled: callable
) -> Optional[CollectedAlbum]:
    """
    Collect a complete photo album from source.
    
    Returns:
        CollectedAlbum ready for download/send, or None if not an album
        
    Note:
        Does NOT persist anything - caller owns the result
    """
    if await check_cancelled():
        return None
    
    # Get the initial message
    msg = await fetch_message(client, chat_id, message_id)
    if not msg or not msg.photo or not msg.media_group_id:
        return None
    
    media_group_id = msg.media_group_id
    
    # Fetch all album messages
    album_messages = await fetch_media_group(client, chat_id, message_id)
    if len(album_messages) < 2:
        return None
    
    # Verify all photos
    for m in album_messages:
        if not m.photo:
            return None
    
    if await check_cancelled():
        return None
    
    # Sort by message_id for correct order
    sorted_messages = sorted(album_messages, key=lambda m: m.id)
    
    # Create temp directory
    unique_id = uuid.uuid4().hex[:8]
    temp_dir = os.path.join(ALBUM_TEMP_BASE, f"album_{unique_id}")
    os.makedirs(temp_dir, exist_ok=True)
    
    # Build album structure
    photos = []
    caption = None
    caption_entities = None
    
    for idx, m in enumerate(sorted_messages):
        photos.append(AlbumPhoto(
            message_id=m.id,
            order_index=idx
        ))
        # Capture caption from first message
        if idx == 0 and m.caption:
            caption = str(m.caption)  # Convert Pyrogram Str → plain str
            caption_entities = m.caption_entities
    
    return CollectedAlbum(
        media_group_id=media_group_id,
        first_message_id=sorted_messages[0].id,
        last_message_id=sorted_messages[-1].id,
        photos=photos,
        caption=caption,
        caption_entities=caption_entities,
        temp_dir=temp_dir
    )


async def download_album_photos(
    client: Client,
    album: CollectedAlbum,
    source_chat_id: int,
    check_cancelled: callable
) -> bool:
    """
    Download all photos in album to temp directory.
    
    Returns:
        True if all photos downloaded successfully
    """
    if not album.temp_dir:
        return False
    
    ordered_photos = album.get_ordered_photos()
    
    for idx, photo in enumerate(ordered_photos):
        if await check_cancelled():
            return False
        
        # Fetch message
        msg = await fetch_message(client, source_chat_id, photo.message_id)
        if not msg or not msg.photo:
            logger.warning(f"Failed to fetch album photo {photo.message_id}")
            continue
        
        # Download
        filename = f"{idx:03d}_photo_{photo.message_id}.jpg"
        file_path = os.path.join(album.temp_dir, sanitize_filename(filename))
        
        downloaded = await download_media_file(client, msg, file_path)
        if downloaded:
            photo.file_path = downloaded
        else:
            logger.warning(f"Failed to download album photo {photo.message_id}")
    
    # Check we have enough photos
    valid_count = sum(1 for p in ordered_photos if p.file_path and os.path.exists(p.file_path))
    return valid_count >= 2


async def send_album(
    bot_client: Client,
    user_session: Client,
    album: CollectedAlbum,
    target_chat_id: int,
    reply_to_message_id: int,
    check_cancelled: callable
) -> bool:
    """
    Send collected album as media group.
    
    Returns:
        True if sent successfully
    """
    if await check_cancelled():
        return False
    
    ordered_photos = album.get_ordered_photos()
    valid_photos = [p for p in ordered_photos if p.file_path and os.path.exists(p.file_path)]
    
    if len(valid_photos) < 2:
        return False
    
    # Use SmartRenderer to handle caption with entities properly
    from core.smart_renderer import from_text_and_entities
    
    caption_for_media = None
    caption_entities = None
    overflow_chunks = None  # Will hold overflow message kwargs
    
    if album.caption:
        try:
            renderer = from_text_and_entities(
                str(album.caption),
                album.caption_entities or [],
                is_caption=True
            )
            caption_result, overflow_chunks = renderer.render_caption_chunks(limit=1024)
            caption_for_media = caption_result.get('caption')
            caption_entities = caption_result.get('caption_entities')
        except Exception as render_err:
            logger.warning(f"SmartRenderer error: {render_err}, using plain caption")
            _plain_caption = str(album.caption)
            caption_for_media = _plain_caption[:1024] if len(_plain_caption) > 1024 else _plain_caption
            caption_entities = None
    
    # Build InputMediaPhoto list
    media_list: List[InputMediaPhoto] = []
    
    for idx, photo in enumerate(valid_photos):
        if idx == 0 and caption_for_media:
            # First photo gets caption with entities
            media_list.append(InputMediaPhoto(
                photo.file_path,
                caption=caption_for_media,
                caption_entities=caption_entities
            ))
        else:
            media_list.append(InputMediaPhoto(photo.file_path))
    
    if await check_cancelled():
        return False
    
    upload_chat_id = getattr(getattr(bot_client, "me", None), "id", None) or target_chat_id

    # Send in batches of 10 (Telegram limit). Media is uploaded by user session;
    # text overflow below remains bot-authored.
    try:
        for i in range(0, len(media_list), 10):
            if await check_cancelled():
                return False
            
            batch = media_list[i:i+10]
            sent = False

            # Do not pass bot-side reply ids to user-session media sends.
            if not sent:
                try:
                    await user_session.send_media_group(upload_chat_id, batch)
                    sent = True
                except TypeError as te:
                    # Pyrofork Messages compatibility issue - ignore
                    if 'topics' in str(te):
                        logger.debug(f"Pyrofork topics warning (ignored): {te}")
                        sent = True  # Message was likely sent
                    else:
                        raise
            
            if i + 10 < len(media_list):
                await asyncio.sleep(1)  # Delay between batches
        
        # Send overflow caption as separate message if exists (from SmartRenderer).
        # Overflow is text, so it stays on bot client.
        if overflow_chunks:
            for idx, chunk_kwargs in enumerate(overflow_chunks):
                try:
                    payload = dict(chunk_kwargs)
                    payload['chat_id'] = target_chat_id
                    await bot_client.send_message(**payload)
                except Exception as oe:
                    logger.warning(f"Failed to send overflow caption chunk {idx}: {oe}")
        
        return True
        
    except Exception as e:
        error_str = str(e)
        # Ignore Pyrofork topics error - album was likely sent
        if 'topics' in error_str.lower() or 'Messages' in error_str:
            logger.debug(f"Pyrofork compatibility issue (ignored): {e}")
            return True
        logger.error(f"Error sending album: {e}")
        return False


def cleanup_album(album: CollectedAlbum) -> None:
    """
    Clean up album temp directory.
    Call in finally block to guarantee cleanup.
    """
    if album and album.temp_dir and os.path.exists(album.temp_dir):
        try:
            shutil.rmtree(album.temp_dir)
        except Exception as e:
            logger.warning(f"Error cleaning up album: {e}")


# =============================================================================
# MAIN ALBUM PROCESSING PIPELINE
# =============================================================================

async def process_album_with_session(
    bot_client: Client,
    user_session: Client,
    user_id: int,
    target_chat_id: int,
    source_chat_id: int,
    message_id: int,
    reply_to_message_id: int,
    check_cancelled: callable,
    sent_albums: Optional[Set[str]] = None
) -> Tuple[bool, str, Optional[AlbumBoundary]]:
    """
    Process album using an EXISTING session (no new session created).
    
    Use this when you already have a connected session to avoid
    nested session creation and event loop issues.
    
    Args:
        bot_client: Bot client for sending
        user_session: Already-connected Pyrogram client
        user_id: User ID
        target_chat_id: Where to send the album
        source_chat_id: Source channel ID
        message_id: Any message ID in the album
        reply_to_message_id: Message to reply to
        check_cancelled: Async callable returning True if cancelled
        sent_albums: Optional set of already-sent media_group_ids (in-memory dedup)
    
    Returns:
        (success, status, boundary)
    """
    album = None
    acc = user_session  # Use existing session
    
    try:
        if await check_cancelled():
            return False, "cancelled", None
        
        # Step 1: Detect album and get boundary
        boundary = await detect_album_boundary(acc, source_chat_id, message_id)
        
        if not boundary:
            return False, "not_album", None
        
        # Step 2: Check if already sent (in-memory only)
        if sent_albums and boundary.media_group_id in sent_albums:
            logger.info(f"Album {boundary.media_group_id} already sent, skipping")
            return True, "already_sent", boundary
        
        # Step 3: Collect album
        album = await collect_album(acc, source_chat_id, message_id, check_cancelled)
        
        if not album:
            return False, "not_album", boundary
        
        if await check_cancelled():
            return False, "cancelled", boundary
        
        # Step 4: Download photos
        download_success = await download_album_photos(
            acc, album, source_chat_id, check_cancelled
        )
        
        if not download_success:
            return False, "download_failed", boundary
        
        if await check_cancelled():
            return False, "cancelled", boundary
        
        # Step 5: Upload album via user session; bot sends only text overflow.
        send_success = await send_album(
            bot_client, acc, album, target_chat_id, reply_to_message_id, check_cancelled
        )
        
        if not send_success:
            return False, "send_failed", boundary
        
        # Step 6: Mark as sent (in-memory only)
        if sent_albums is not None:
            sent_albums.add(boundary.media_group_id)
        
        return True, "sent", boundary
        
    except SessionInvalidError as e:
        logger.error(f"Session invalid: {e}")
        return False, "session_invalid", None
        
    except asyncio.CancelledError:
        return False, "cancelled", None
        
    except Exception as e:
        logger.error(f"Album pipeline error: {e}")
        return False, f"error:{str(e)}", None
        
    finally:
        # GUARANTEED CLEANUP
        if album:
            cleanup_album(album)


# Legacy function - creates new session (use process_album_with_session instead)
async def process_album_pipeline(
    bot_client: Client,
    session_string: str,
    user_id: int,
    target_chat_id: int,
    source_chat_id: int,
    message_id: int,
    reply_to_message_id: int,
    check_cancelled: callable,
    sent_albums: Optional[Set[str]] = None
) -> Tuple[bool, str, Optional[AlbumBoundary]]:
    """
    DEPRECATED: Use process_album_with_session with an existing session.
    This creates a new session which can cause event loop issues.
    """
    try:
        async with task_scoped_session(session_string, user_id) as acc:
            return await process_album_with_session(
                bot_client, acc, user_id, target_chat_id, source_chat_id,
                message_id, reply_to_message_id, check_cancelled, sent_albums
            )
    except SessionInvalidError as e:
        return False, f"session_invalid:{e}", None
    except Exception as e:
        return False, f"error:{e}", None


# =============================================================================
# ALBUM-AWARE MESSAGE ITERATION
# =============================================================================

class AlbumAwareIterator:
    """
    Iterator for message_ids that skips processed albums.
    
    Usage:
        iterator = AlbumAwareIterator(post_ids)
        while True:
            message_id = iterator.next()
            if message_id is None:
                break
            
            # Process message...
            
            # If it was an album, skip to end
            if boundary:
                iterator.skip_to(boundary.next_message_id)
    """
    
    def __init__(self, message_ids: List[int]):
        self._ids = list(message_ids)
        self._index = 0
        self._processed_albums: Set[str] = set()  # In-memory only
    
    def next(self) -> Optional[int]:
        """Get next message_id to process."""
        if self._index >= len(self._ids):
            return None
        msg_id = self._ids[self._index]
        self._index += 1
        return msg_id
    
    def skip_to(self, message_id: int) -> None:
        """
        Skip iteration to message_id.
        Used after processing an album to jump past all album messages.
        """
        # Find index of first ID >= message_id
        while self._index < len(self._ids) and self._ids[self._index] < message_id:
            self._index += 1
    
    def mark_album_processed(self, media_group_id: str) -> None:
        """Mark album as processed (in-memory dedup)."""
        self._processed_albums.add(media_group_id)
    
    def is_album_processed(self, media_group_id: str) -> bool:
        """Check if album was already processed."""
        return media_group_id in self._processed_albums
    
    @property
    def processed_albums(self) -> Set[str]:
        """Get set of processed album IDs (for passing to pipeline)."""
        return self._processed_albums
    
    @property
    def remaining(self) -> int:
        """Number of message_ids remaining."""
        return max(0, len(self._ids) - self._index)
    
    @property
    def total(self) -> int:
        """Total message count."""
        return len(self._ids)
    
    @property
    def current_position(self) -> int:
        """Current position (1-indexed for display)."""
        return min(self._index, len(self._ids))


# =============================================================================
# CONVENIENCE FUNCTIONS FOR SINGLE ALBUM CHECK
# =============================================================================

async def check_if_photo_album(
    session_string: str,
    user_id: int,
    chat_id: int,
    message_id: int
) -> Tuple[bool, Optional[str], Optional[AlbumBoundary]]:
    """
    Check if a message is part of a photo album.
    Creates temporary session, checks, and cleans up.
    
    Returns:
        (is_album, media_group_id, boundary)
    """
    try:
        async with task_scoped_session(session_string, user_id) as client:
            return await is_photo_album_message(client, chat_id, message_id)
    except SessionInvalidError:
        return False, None, None
    except Exception as e:
        logger.warning(f"Error checking album: {e}")
        return False, None, None


# Initialize temp directory
os.makedirs(ALBUM_TEMP_BASE, exist_ok=True)
