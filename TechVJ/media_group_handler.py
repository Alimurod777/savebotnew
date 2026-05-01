import asyncio
import os
import shutil
import re
import uuid
from typing import List, Tuple, Optional
from pyrogram import Client
from pyrogram.types import Message, InputMediaPhoto
from pyrogram.enums import MessageMediaType
import logging

logger = logging.getLogger(__name__)

# Base directory for temporary album downloads
TEMP_ALBUM_DIR = "downloads/albums"


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to remove problematic characters"""
    # Remove or replace invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Limit length
    if len(sanitized) > 200:
        name, ext = os.path.splitext(sanitized)
        sanitized = name[:200-len(ext)] + ext
    return sanitized


def get_user_temp_dir(user_id: int) -> str:
    """
    Get or create a unique temporary directory for a user.
    Prevents media collision between users.
    """
    # Create unique directory with user_id and UUID for extra uniqueness
    unique_id = str(uuid.uuid4())[:8]
    user_dir = os.path.join(TEMP_ALBUM_DIR, f"user_{user_id}_{unique_id}")
    os.makedirs(user_dir, exist_ok=True)
    return user_dir


def cleanup_user_temp_dir(temp_dir: str) -> None:
    """Clean up a user's temporary directory"""
    try:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    except Exception as e:
        logger.warning(f"Error cleaning up temp directory {temp_dir}: {e}")


async def is_media_group(acc: Client, chat_id: int, message_id: int) -> Tuple[bool, Optional[str], List[Message]]:
    """
    Check if a message is part of a media group (album).
    
    Returns:
        Tuple of (is_album: bool, media_group_id: str or None, messages: list of album messages)
    """
    try:
        msg = await acc.get_messages(chat_id, message_id)
        
        if not msg or msg.empty:
            return False, None, []
        
        # Check if message has media_group_id (indicates album)
        if not msg.media_group_id:
            return False, None, []
        
        # Fetch all messages in the media group
        media_group_id = msg.media_group_id
        album_messages = await acc.get_media_group(chat_id, message_id)
        
        if not album_messages or len(album_messages) < 2:
            return False, None, []
        
        return True, media_group_id, album_messages
        
    except Exception as e:
        logger.warning(f"Error checking media group: {e}")
        return False, None, []


def is_photo_album(messages: List[Message]) -> bool:
    """
    Check if all messages in the album are photos.
    Only photo albums should use send_media_group.
    """
    if not messages:
        return False
    
    for msg in messages:
        if not msg.photo:
            return False
    
    return True


async def download_album_photos(
    acc: Client, 
    messages: List[Message], 
    user_id: int,
    progress_callback=None
) -> Tuple[str, List[str]]:
    """
    Download all photos from an album to a user-specific temp directory.
    Preserves original order.
    
    Returns:
        Tuple of (temp_dir_path, list of downloaded file paths in order)
    """
    temp_dir = get_user_temp_dir(user_id)
    downloaded_files = []
    
    try:
        # Sort messages by message ID to preserve order
        sorted_messages = sorted(messages, key=lambda m: m.id)
        
        for idx, msg in enumerate(sorted_messages):
            if not msg.photo:
                continue
            
            # Create ordered filename
            filename = f"{idx:03d}_photo_{msg.id}.jpg"
            filepath = os.path.join(temp_dir, filename)
            
            # Download the photo
            downloaded_path = await acc.download_media(
                msg,
                file_name=filepath
            )
            
            if downloaded_path and os.path.exists(downloaded_path):
                downloaded_files.append(downloaded_path)
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.3)
        
        return temp_dir, downloaded_files
        
    except Exception as e:
        # Cleanup on error
        cleanup_user_temp_dir(temp_dir)
        raise e


async def send_photo_album(
    client: Client,
    chat_id: int,
    photo_paths: List[str],
    caption: Optional[str] = None,
    caption_entities: Optional[List] = None,
    reply_to_message_id: Optional[int] = None
) -> Tuple[bool, int, int]:
    """
    Send photos as an album using send_media_group.
    
    FIXED: Uses entity-aware caption splitting to preserve hyperlinks.
    
    Args:
        client: Bot client to send with
        chat_id: Target chat ID
        photo_paths: List of photo file paths in order
        caption: Optional caption (applied to first photo)
        caption_entities: Optional caption entities (for hyperlinks etc.)
        reply_to_message_id: Optional message to reply to
    
    Returns:
        Tuple of (success: bool, batches_sent: int, batches_failed: int)
    """
    if not photo_paths:
        return False, 0, 0
    
    try:
        # Import entity-aware splitting
        from core.entity_validator import (
            split_caption_with_entities, 
            prepare_entities_for_send,
            utf16_length,
            CAPTION_LIMIT
        )
        
        # Process caption with entity-aware splitting
        media_caption = None
        media_entities = None
        overflow_text = None
        overflow_entities = None
        
        if caption:
            cap_utf16_len = utf16_length(caption)
            
            if cap_utf16_len <= CAPTION_LIMIT:
                # Caption fits - validate entities
                media_caption = caption
                if caption_entities:
                    media_entities = prepare_entities_for_send(caption, caption_entities)
            else:
                # Caption too long - split with entity preservation
                cap_chunk, overflow_chunks = split_caption_with_entities(
                    caption, caption_entities or []
                )
                media_caption = cap_chunk.text
                media_entities = cap_chunk.entities if cap_chunk.entities else None
                
                # Combine overflow chunks for follow-up message
                if overflow_chunks:
                    overflow_text = "\n\n".join(chunk.text for chunk in overflow_chunks)
                    # Collect overflow entities with adjusted offsets
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
                        current_offset += utf16_length(chunk.text) + 2  # +2 for \n\n
        
        # Build InputMediaPhoto list
        media_list = []
        
        for idx, photo_path in enumerate(photo_paths):
            if not os.path.exists(photo_path):
                continue
            
            # Only first photo gets the caption - NEVER use parse_mode with entities
            if idx == 0 and media_caption:
                media_kwargs = {'media': photo_path, 'caption': media_caption}
                if media_entities:
                    media_kwargs['caption_entities'] = media_entities
                media_list.append(InputMediaPhoto(**media_kwargs))
            else:
                media_list.append(InputMediaPhoto(photo_path))
        
        if len(media_list) < 2:
            # Can't send album with less than 2 photos
            return False, 0, 0
        
        batches_sent = 0
        batches_failed = 0
        
        # Telegram limits albums to 10 items
        if len(media_list) > 10:
            logger.info(f"Large album ({len(media_list)} photos) will be split into {(len(media_list)-1)//10 + 1} batches")
            
            # Split into multiple albums with partial delivery tracking
            for i in range(0, len(media_list), 10):
                batch = media_list[i:i+10]
                try:
                    await client.send_media_group(
                        chat_id,
                        batch,
                        reply_to_message_id=reply_to_message_id if i == 0 else None
                    )
                    batches_sent += 1
                except Exception as e:
                    batches_failed += 1
                    logger.error(f"Album batch {i//10 + 1} failed: {e}")
                    # Continue with remaining batches instead of failing entirely
                    if batches_sent == 0:
                        # First batch failed - abort
                        raise
                
                if i + 10 < len(media_list):
                    await asyncio.sleep(1)
        else:
            await client.send_media_group(
                chat_id,
                media_list,
                reply_to_message_id=reply_to_message_id
            )
            batches_sent = 1
        
        # Send overflow caption as separate message if needed
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
                    
                    # Validate adjusted entities
                    adjusted_overflow_entities = prepare_entities_for_send(
                        full_overflow, adjusted_overflow_entities
                    )
                
                send_kwargs = {'chat_id': chat_id, 'text': full_overflow}
                if adjusted_overflow_entities:
                    send_kwargs['entities'] = adjusted_overflow_entities
                
                await client.send_message(**send_kwargs)
            except Exception as e:
                logger.warning(f"Failed to send overflow caption: {e}")
        
        return batches_sent > 0, batches_sent, batches_failed
        
    except Exception as e:
        logger.error(f"Error sending photo album: {e}")
        return False, 0, 0


async def handle_media_group(
    client: Client,
    acc: Client,
    message: Message,
    chat_id: int,
    message_id: int,
    check_cancel_callback=None
) -> bool:
    """
    Main handler for media groups.
    Downloads album photos and sends them as a proper album.
    
    Args:
        client: Bot client
        acc: User session client
        message: Original user message
        chat_id: Source chat ID
        message_id: Source message ID
        check_cancel_callback: Optional async callback to check if operation should be cancelled
    
    Returns:
        True if handled as album, False if not an album or error
    """
    temp_dir = None
    
    try:
        # Check if this is a media group
        is_album, media_group_id, album_messages = await is_media_group(acc, chat_id, message_id)
        
        if not is_album:
            return False
        
        # Check if it's a photo album (only photos use send_media_group)
        if not is_photo_album(album_messages):
            # Not a photo album - videos, documents should be sent individually
            return False
        
        # Check for cancellation
        if check_cancel_callback and await check_cancel_callback():
            return True  # Return True to indicate we handled it (by cancelling)
        
        # Send status message
        status_msg = await client.send_message(
            message.chat.id,
            f"Downloading photo album ({len(album_messages)} photos)...",
            reply_to_message_id=message.id
        )
        
        # Download all photos
        temp_dir, photo_paths = await download_album_photos(
            acc,
            album_messages,
            message.chat.id
        )
        
        if not photo_paths:
            await client.edit_message_text(
                message.chat.id,
                status_msg.id,
                "Failed to download album photos."
            )
            return True
        
        # Check for cancellation again
        if check_cancel_callback and await check_cancel_callback():
            await client.edit_message_text(
                message.chat.id,
                status_msg.id,
                "Album download cancelled."
            )
            cleanup_user_temp_dir(temp_dir)
            return True
        
        # Get caption and entities from first message (preserve hyperlinks)
        caption = None
        caption_entities = None
        if album_messages:
            first_msg = sorted(album_messages, key=lambda m: m.id)[0]
            if first_msg.caption:
                caption = first_msg.caption
                caption_entities = list(first_msg.caption_entities) if first_msg.caption_entities else None
        
        # Update status
        await client.edit_message_text(
            message.chat.id,
            status_msg.id,
            f"Uploading photo album ({len(photo_paths)} photos)..."
        )
        
        # Send as album with entity-safe caption handling
        success, batches_sent, batches_failed = await send_photo_album(
            client,
            message.chat.id,
            photo_paths,
            caption=caption,
            caption_entities=caption_entities,
            reply_to_message_id=message.id
        )
        
        # Delete status message
        await client.delete_messages(message.chat.id, [status_msg.id])
        
        if not success:
            await client.send_message(
                message.chat.id,
                "Failed to send album. Photos may have been corrupted.",
                reply_to_message_id=message.id
            )
        elif batches_failed > 0:
            # Partial delivery warning
            await client.send_message(
                message.chat.id,
                f"⚠️ Album partially sent: {batches_sent} batch(es) succeeded, {batches_failed} failed.",
                reply_to_message_id=message.id
            )
        
        return True
        
    except asyncio.CancelledError:
        logger.info(f"Media group handling cancelled for user {message.chat.id}")
        raise
    except Exception as e:
        logger.error(f"Error handling media group: {e}")
        return False
    finally:
        # Always cleanup temp directory
        if temp_dir:
            cleanup_user_temp_dir(temp_dir)


# Initialize the albums directory
os.makedirs(TEMP_ALBUM_DIR, exist_ok=True)
