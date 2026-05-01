"""
Photo Album (media_group) Processing Pipeline

Race-condition-safe, stop-safe album collector for Telegram photo albums.
This module handles ONLY photo albums - other media types are processed normally.

Features:
- Album duplication prevention via MongoDB + in-memory cache
- Per-user album tracking
- Stop-safe cancellation
- Guaranteed cleanup
"""

import asyncio
import os
import shutil
import uuid
import re
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from pyrogram import Client
from pyrogram.types import Message, InputMediaPhoto
from pyrogram.enums import MessageMediaType, ParseMode
import logging

logger = logging.getLogger(__name__)

# Import StateManager for duplicate tracking (MongoDB + local cache)
try:
    from database.state_manager import state_manager, is_album_sent, mark_album_sent
    STATE_MANAGER_AVAILABLE = True
except ImportError:
    STATE_MANAGER_AVAILABLE = False
    logger.warning("StateManager not available for album tracking")

# Constants
ALBUM_AGGREGATION_DELAY = 0.5  # seconds to wait for late-arriving messages
ALBUM_TEMP_BASE = "downloads/albums"


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to remove problematic characters"""
    if not filename:
        return "photo"
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)
    sanitized = sanitized.strip('. ')
    if len(sanitized) > 200:
        name, ext = os.path.splitext(sanitized)
        sanitized = name[:200-len(ext)] + ext
    return sanitized or "photo"


@dataclass
class AlbumPhoto:
    """Represents a single photo in an album"""
    message_id: int
    file_path: Optional[str] = None
    caption: Optional[str] = None
    caption_entities: Optional[Any] = None
    order_index: int = 0  # Original order in album


@dataclass
class PendingAlbum:
    """Represents a pending album being collected"""
    media_group_id: str
    user_id: int
    chat_id: int
    photos: Dict[int, AlbumPhoto] = field(default_factory=dict)  # message_id -> AlbumPhoto
    first_message_id: int = 0
    caption: Optional[str] = None
    caption_entities: Optional[Any] = None
    temp_dir: Optional[str] = None
    finalized: bool = False
    cancelled: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    
    def add_photo(self, msg: Message, order_index: int) -> None:
        """Add a photo to the album"""
        photo = AlbumPhoto(
            message_id=msg.id,
            order_index=order_index
        )
        
        # Capture caption from FIRST message only (lowest message_id)
        if msg.caption and (self.caption is None or msg.id < self.first_message_id):
            self.caption = msg.caption
            self.caption_entities = msg.caption_entities
            self.first_message_id = msg.id
        
        if self.first_message_id == 0:
            self.first_message_id = msg.id
        elif msg.id < self.first_message_id:
            self.first_message_id = msg.id
        
        self.photos[msg.id] = photo
    
    def get_ordered_photos(self) -> List[AlbumPhoto]:
        """Get photos in original Telegram order (by message_id)"""
        return sorted(self.photos.values(), key=lambda p: p.message_id)


class AlbumCollector:
    """
    Race-condition-safe album collector.
    
    Features:
    - Per-user album buffers to prevent cross-user contamination
    - Async locks for thread safety
    - Aggregation delay to handle late-arriving messages
    - Single finalization per album
    - Stop-safe cancellation
    """
    
    def __init__(self):
        # Structure: {user_id: {media_group_id: PendingAlbum}}
        self._user_albums: Dict[int, Dict[str, PendingAlbum]] = {}
        self._global_lock: Optional[asyncio.Lock] = None
        self._bound_loop_id: Optional[int] = None
        self._finalization_tasks: Dict[str, asyncio.Task] = {}
        
        # Ensure base directory exists
        os.makedirs(ALBUM_TEMP_BASE, exist_ok=True)
    
    def _get_lock(self) -> asyncio.Lock:
        """Get or create lock bound to current event loop."""
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
        except RuntimeError:
            # No running loop - should not happen in async context
            # Return a fresh lock (will be replaced when loop starts)
            return asyncio.Lock()
        
        if self._bound_loop_id is not None and self._bound_loop_id != current_loop_id:
            # Loop changed - reset lock
            self._global_lock = None
        
        self._bound_loop_id = current_loop_id
        
        if self._global_lock is None:
            self._global_lock = asyncio.Lock()
        return self._global_lock
    
    async def _get_user_albums(self, user_id: int) -> Dict[str, PendingAlbum]:
        """Get or create album buffer for a user"""
        async with self._get_lock():
            if user_id not in self._user_albums:
                self._user_albums[user_id] = {}
            return self._user_albums[user_id]
    
    async def _get_or_create_album(
        self, 
        user_id: int, 
        chat_id: int,
        media_group_id: str
    ) -> Tuple[PendingAlbum, bool]:
        """
        Get existing album or create new one.
        Returns (album, is_new)
        """
        user_albums = await self._get_user_albums(user_id)
        
        async with self._get_lock():
            if media_group_id in user_albums:
                return user_albums[media_group_id], False
            
            # Create new album
            unique_id = str(uuid.uuid4())[:8]
            temp_dir = os.path.join(ALBUM_TEMP_BASE, f"album_{user_id}_{unique_id}")
            os.makedirs(temp_dir, exist_ok=True)
            
            album = PendingAlbum(
                media_group_id=media_group_id,
                user_id=user_id,
                chat_id=chat_id,
                temp_dir=temp_dir
            )
            user_albums[media_group_id] = album
            return album, True
    
    async def add_photo_to_album(
        self, 
        user_id: int,
        chat_id: int,
        msg: Message,
        order_index: int = 0
    ) -> PendingAlbum:
        """
        Add a photo message to an album buffer.
        Thread-safe with per-album locking.
        """
        media_group_id = msg.media_group_id
        album, is_new = await self._get_or_create_album(user_id, chat_id, media_group_id)
        
        async with album.lock:
            if album.finalized or album.cancelled:
                # Album already processed or cancelled
                return album
            
            album.add_photo(msg, order_index)
        
        return album
    
    async def collect_album_from_source(
        self,
        acc: Client,
        user_id: int,
        chat_id: int,
        source_chat_id: int,
        first_message_id: int,
        check_cancelled: callable
    ) -> Optional[PendingAlbum]:
        """
        Collect a complete album from a source chat.
        Uses get_media_group to fetch all photos at once.
        
        Args:
            acc: User session client
            user_id: Target user ID
            chat_id: Target chat ID (where to send)
            source_chat_id: Source channel/chat ID
            first_message_id: ID of any message in the album
            check_cancelled: Async callable to check cancellation
        
        Returns:
            PendingAlbum if successful, None otherwise
        """
        try:
            # Check cancellation before starting
            if await check_cancelled():
                return None
            
            # Fetch all messages in the media group
            album_messages = await acc.get_media_group(source_chat_id, first_message_id)
            
            if not album_messages or len(album_messages) < 2:
                # Not a valid album (single photo or empty)
                return None
            
            # Verify all are photos
            for msg in album_messages:
                if not msg.photo:
                    # Not a photo album - return None to use normal processing
                    return None
            
            # Check cancellation after fetch
            if await check_cancelled():
                return None
            
            # Get media_group_id from first message
            media_group_id = album_messages[0].media_group_id
            if not media_group_id:
                return None
            
            # Create album
            album, _ = await self._get_or_create_album(user_id, chat_id, media_group_id)
            
            async with album.lock:
                if album.cancelled:
                    return None
                
                # Add all photos in order (sorted by message_id for correct ordering)
                sorted_messages = sorted(album_messages, key=lambda m: m.id)
                for idx, msg in enumerate(sorted_messages):
                    album.add_photo(msg, idx)
            
            return album
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Error collecting album: {e}")
            return None
    
    async def download_album_photos(
        self,
        acc: Client,
        album: PendingAlbum,
        source_chat_id: int,
        check_cancelled: callable
    ) -> bool:
        """
        Download all photos in an album sequentially.
        Preserves original order strictly.
        
        Returns True if successful, False otherwise.
        """
        try:
            async with album.lock:
                if album.cancelled or album.finalized:
                    return False
                
                ordered_photos = album.get_ordered_photos()
                
                for idx, photo in enumerate(ordered_photos):
                    # Check cancellation before each download
                    if await check_cancelled():
                        album.cancelled = True
                        return False
                    
                    # Fetch the message
                    msg = await acc.get_messages(source_chat_id, photo.message_id)
                    if not msg or not msg.photo:
                        continue
                    
                    # Create ordered filename: 000_photo.jpg, 001_photo.jpg, etc.
                    filename = f"{idx:03d}_photo_{photo.message_id}.jpg"
                    file_path = os.path.join(album.temp_dir, sanitize_filename(filename))
                    
                    # Download photo
                    downloaded = await acc.download_media(msg, file_name=file_path)
                    
                    if downloaded and os.path.exists(downloaded):
                        photo.file_path = downloaded
                    else:
                        logger.warning(f"Failed to download photo {photo.message_id}")
                
                return True
                
        except asyncio.CancelledError:
            album.cancelled = True
            raise
        except Exception as e:
            logger.error(f"Error downloading album photos: {e}")
            return False
    
    async def is_album_already_sent(self, user_id: int, media_group_id: str) -> bool:
        """
        Check if an album was already sent to a user.
        Uses StateManager (local cache + MongoDB with automatic fallback).
        """
        if STATE_MANAGER_AVAILABLE:
            try:
                return await is_album_sent(user_id, media_group_id)
            except Exception as e:
                logger.warning(f"Error checking album via StateManager: {e}")
        return False
    
    async def mark_album_as_sent(self, user_id: int, media_group_id: str, source_chat_id: int = None):
        """
        Mark an album as sent to prevent future duplicates.
        Uses StateManager (local cache + MongoDB with automatic sync).
        """
        if STATE_MANAGER_AVAILABLE:
            try:
                await mark_album_sent(user_id, media_group_id, source_chat_id)
            except Exception as e:
                logger.warning(f"Error marking album via StateManager: {e}")
    
    async def send_album(
        self,
        client: Client,
        album: PendingAlbum,
        reply_to_message_id: int,
        check_cancelled: callable,
        source_chat_id: int = None
    ) -> bool:
        """
        Send album as a media group.
        Caption is attached ONLY to the first photo.
        Checks for duplicates before sending.
        
        Returns True if successful, False otherwise.
        """
        try:
            async with album.lock:
                if album.cancelled:
                    return False
                
                if album.finalized:
                    # Already sent
                    return True
                
                # Check cancellation
                if await check_cancelled():
                    album.cancelled = True
                    return False
                
                # === DUPLICATE PREVENTION CHECK ===
                if await self.is_album_already_sent(album.user_id, album.media_group_id):
                    logger.info(f"Album {album.media_group_id} already sent to user {album.user_id}, skipping")
                    album.finalized = True  # Mark as finalized to prevent reprocessing
                    return True  # Return True as "success" - no error, just skipped
                
                # Get ordered photos with downloaded files
                ordered_photos = album.get_ordered_photos()
                valid_photos = [p for p in ordered_photos if p.file_path and os.path.exists(p.file_path)]
                
                if len(valid_photos) < 2:
                    # Not enough photos for an album
                    return False
                
                # Build InputMediaPhoto list
                media_list: List[InputMediaPhoto] = []
                
                # Get caption entities from first photo that had caption
                caption_entities = None
                if album.caption and hasattr(album, 'caption_entities'):
                    caption_entities = album.caption_entities
                
                # Import entity validation and splitting
                from core.entity_validator import (
                    prepare_entities_for_send, 
                    split_caption_with_entities,
                    utf16_length,
                    CAPTION_LIMIT
                )
                
                # Process caption with entity-aware splitting
                media_caption = None
                media_entities = None
                overflow_text = None
                overflow_entities = None
                
                if album.caption:
                    cap_utf16_len = utf16_length(album.caption)
                    
                    if cap_utf16_len <= CAPTION_LIMIT:
                        # Caption fits - validate entities
                        media_caption = album.caption
                        if caption_entities:
                            media_entities = prepare_entities_for_send(album.caption, caption_entities)
                    else:
                        # Caption too long - split with entity preservation
                        cap_chunk, overflow_chunks = split_caption_with_entities(
                            album.caption, caption_entities or []
                        )
                        media_caption = cap_chunk.text
                        media_entities = cap_chunk.entities if cap_chunk.entities else None
                        
                        # Combine overflow for follow-up message
                        if overflow_chunks:
                            overflow_text = "\n\n".join(chunk.text for chunk in overflow_chunks)
                            overflow_entities = []
                            current_offset = 0
                            for chunk in overflow_chunks:
                                for e in (chunk.entities or []):
                                    try:
                                        from pyrogram.types import MessageEntity
                                        adjusted = MessageEntity(
                                            type=e.type,
                                            offset=e.offset + current_offset,
                                            length=e.length,
                                            url=getattr(e, 'url', None),
                                            user=getattr(e, 'user', None),
                                            language=getattr(e, 'language', None),
                                        )
                                        overflow_entities.append(adjusted)
                                    except Exception:
                                        pass
                                current_offset += utf16_length(chunk.text) + 2
                
                for idx, photo in enumerate(valid_photos):
                    if idx == 0:
                        # First photo gets caption - NEVER use parse_mode with entities
                        media_kwargs = {'media': photo.file_path}
                        if media_caption:
                            media_kwargs['caption'] = media_caption
                            if media_entities:
                                media_kwargs['caption_entities'] = media_entities
                        media_list.append(InputMediaPhoto(**media_kwargs))
                    else:
                        # Other photos have no caption
                        media_list.append(InputMediaPhoto(photo.file_path))
                
                # Check cancellation before send
                if await check_cancelled():
                    album.cancelled = True
                    return False
                
                # Telegram limits albums to 10 items - split if needed with partial delivery tracking
                batches_sent = 0
                batches_failed = 0
                total_batches = (len(media_list) - 1) // 10 + 1
                
                if total_batches > 1:
                    logger.info(f"Large album ({len(media_list)} photos) will be split into {total_batches} batches")
                
                for i in range(0, len(media_list), 10):
                    batch = media_list[i:i+10]
                    
                    if await check_cancelled():
                        album.cancelled = True
                        if batches_sent > 0:
                            logger.warning(f"Album cancelled after {batches_sent}/{total_batches} batches sent")
                        return False
                    
                    try:
                        await client.send_media_group(
                            album.chat_id,
                            batch,
                            reply_to_message_id=reply_to_message_id if i == 0 else None
                        )
                        batches_sent += 1
                    except Exception as e:
                        batches_failed += 1
                        logger.error(f"Album batch {i//10 + 1}/{total_batches} failed: {e}")
                        # If first batch fails, abort entirely
                        if batches_sent == 0:
                            raise
                        # Otherwise continue with remaining batches
                    
                    if i + 10 < len(media_list):
                        await asyncio.sleep(1)  # Small delay between batches
                
                # Send overflow caption if needed
                if overflow_text and batches_sent > 0:
                    try:
                        overflow_header = "[Caption continued]\n\n"
                        full_overflow = overflow_header + overflow_text
                        
                        # Adjust entity offsets for header
                        header_utf16 = utf16_length(overflow_header)
                        adjusted_overflow_entities = None
                        if overflow_entities:
                            adjusted_overflow_entities = []
                            for e in overflow_entities:
                                try:
                                    from pyrogram.types import MessageEntity
                                    adjusted = MessageEntity(
                                        type=e.type,
                                        offset=e.offset + header_utf16,
                                        length=e.length,
                                        url=getattr(e, 'url', None),
                                        user=getattr(e, 'user', None),
                                        language=getattr(e, 'language', None),
                                    )
                                    adjusted_overflow_entities.append(adjusted)
                                except Exception:
                                    pass
                            
                            adjusted_overflow_entities = prepare_entities_for_send(
                                full_overflow, adjusted_overflow_entities
                            )
                        
                        send_kwargs = {'chat_id': album.chat_id, 'text': full_overflow}
                        if adjusted_overflow_entities:
                            send_kwargs['entities'] = adjusted_overflow_entities
                        
                        await client.send_message(**send_kwargs)
                    except Exception as e:
                        logger.warning(f"Failed to send overflow caption: {e}")
                
                # Report partial delivery if applicable
                if batches_failed > 0:
                    logger.warning(f"Album partial delivery: {batches_sent}/{total_batches} batches sent")
                
                album.finalized = True
                
                # === MARK ALBUM AS SENT ===
                await self.mark_album_as_sent(album.user_id, album.media_group_id, source_chat_id)
                
                return batches_sent > 0
                
        except asyncio.CancelledError:
            album.cancelled = True
            raise
        except Exception as e:
            logger.error(f"Error sending album: {e}")
            return False
    
    async def cleanup_album(self, album: PendingAlbum) -> None:
        """
        Cleanup album resources.
        Called in ALL cases: success, error, cancellation.
        """
        try:
            # Remove from user's album buffer
            async with self._get_lock():
                if album.user_id in self._user_albums:
                    self._user_albums[album.user_id].pop(album.media_group_id, None)
            
            # Delete temp directory
            if album.temp_dir and os.path.exists(album.temp_dir):
                shutil.rmtree(album.temp_dir)
                
        except Exception as e:
            logger.warning(f"Error cleaning up album: {e}")
    
    async def cancel_user_albums(self, user_id: int) -> int:
        """
        Cancel all pending albums for a user.
        Called when /stop is triggered.
        
        Returns number of albums cancelled.
        """
        cancelled_count = 0
        
        async with self._get_lock():
            if user_id not in self._user_albums:
                return 0
            
            albums_to_cancel = list(self._user_albums[user_id].values())
        
        for album in albums_to_cancel:
            async with album.lock:
                if not album.finalized and not album.cancelled:
                    album.cancelled = True
                    cancelled_count += 1
            
            # Cleanup
            await self.cleanup_album(album)
        
        # Clear user's album buffer
        async with self._get_lock():
            if user_id in self._user_albums:
                self._user_albums[user_id].clear()
        
        return cancelled_count
    
    async def process_album(
        self,
        client: Client,
        acc: Client,
        user_id: int,
        chat_id: int,
        source_chat_id: int,
        message_id: int,
        reply_to_message_id: int,
        check_cancelled: callable
    ) -> Tuple[bool, str]:
        """
        Complete album processing pipeline with duplicate prevention.
        
        Steps:
        1. Check if album already sent (early exit)
        2. Collect album from source
        3. Download photos sequentially (preserving order)
        4. Build InputMediaPhoto list with caption on first only
        5. Send album (with final duplicate check)
        6. Mark album as sent
        7. Cleanup (guaranteed via try/finally)
        
        Args:
            client: Bot client
            acc: User session client
            user_id: Target user ID
            chat_id: Target chat ID
            source_chat_id: Source channel ID
            message_id: ID of any message in the album
            reply_to_message_id: Message to reply to
            check_cancelled: Async callable to check cancellation
        
        Returns:
            Tuple of (success: bool, status: str)
            - "already_sent" status means album was skipped (not an error)
        """
        album = None
        
        try:
            # Step 1: Collect album (to get media_group_id)
            album = await self.collect_album_from_source(
                acc, user_id, chat_id, source_chat_id, message_id, check_cancelled
            )
            
            if album is None:
                return False, "not_album"  # Not an album or single photo
            
            if album.cancelled:
                return False, "cancelled"
            
            # === EARLY DUPLICATE CHECK ===
            # Check before downloading to save bandwidth
            if await self.is_album_already_sent(user_id, album.media_group_id):
                logger.info(f"Album {album.media_group_id} already sent to user {user_id}, skipping early")
                return True, "already_sent"
            
            # Step 2: Download photos
            success = await self.download_album_photos(
                acc, album, source_chat_id, check_cancelled
            )
            
            if not success or album.cancelled:
                return False, "cancelled" if album.cancelled else "download_failed"
            
            # Step 3 & 4: Send album (includes final duplicate check and marking)
            success = await self.send_album(
                client, album, reply_to_message_id, check_cancelled, source_chat_id
            )
            
            if not success:
                return False, "cancelled" if album.cancelled else "send_failed"
            
            return True, "success"
            
        except asyncio.CancelledError:
            if album:
                album.cancelled = True
            raise
        except Exception as e:
            logger.error(f"Album processing error: {e}")
            return False, f"error: {str(e)}"
        finally:
            # GUARANTEED CLEANUP (Section 5)
            if album:
                await self.cleanup_album(album)


# Global album collector instance
album_collector = AlbumCollector()


async def is_photo_album(acc: Client, chat_id: int, message_id: int) -> Tuple[bool, Optional[str]]:
    """
    Check if a message is part of a photo album.
    
    Returns:
        Tuple of (is_album: bool, media_group_id: str or None)
    """
    try:
        msg = await acc.get_messages(chat_id, message_id)
        
        if not msg or msg.empty:
            return False, None
        
        # Must be a photo with media_group_id
        if not msg.photo:
            return False, None
        
        if not msg.media_group_id:
            return False, None
        
        # Verify it's actually an album (more than 1 photo)
        try:
            album_messages = await acc.get_media_group(chat_id, message_id)
            if len(album_messages) < 2:
                return False, None
            
            # Verify all are photos
            for m in album_messages:
                if not m.photo:
                    return False, None
            
            return True, msg.media_group_id
        except Exception:
            return False, None
        
    except Exception as e:
        logger.warning(f"Error checking album: {e}")
        return False, None
