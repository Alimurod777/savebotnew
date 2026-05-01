"""
Production-Grade Telegram Transfer System

This module provides a complete, integrated solution for:
- Downloading Telegram media with progress
- Uploading media to users with progress
- Automatic cleanup on cancellation/failure
- Adaptive resource management

Author: WOODcraft Production-Grade Refactor
Python: 3.10+
Platform: Windows-optimized

USAGE EXAMPLE:
    from TechVJ.telegram_transfer import TelegramTransfer
    
    # Create transfer instance
    transfer = TelegramTransfer()
    
    # Execute download->upload with progress
    async with create_session(session_string, user_id) as user_client:
        result = await transfer.transfer_media(
            user_client=user_client,
            source_message=message,
            bot_client=bot,
            dest_chat_id=user_id,
            status_message=status_msg,
            check_cancelled=lambda: cancelled
        )
        
        if result.success:
            print(f"Sent: {result.sent_message.id}")
        else:
            print(f"Error: {result.error}")

EVENT LOOP SAFETY:
- All operations run on a SINGLE asyncio event loop
- No threads, executors, or blocking calls
- No cross-loop futures or tasks
- Safe for Windows asyncio implementation
"""

import asyncio
import os
import logging
from typing import Optional, Callable, Union, Awaitable, List
from dataclasses import dataclass, field
from pathlib import Path

from pyrogram import Client
from pyrogram.types import Message

# Import our modules
from TechVJ.async_progress import (
    async_progress_controller,
    TransferType,
    ProgressFormatter,
    AsyncProgressController
)
from TechVJ.async_transfer import (
    async_downloader,
    async_uploader,
    TransferStatus,
    TransferResult,
    cleanup_file,
    get_media_type,
    get_media_filename,
    ensure_temp_dir
)
from TechVJ.session_handler_v2 import (
    create_session,
    SessionContext,
    SessionInvalidError,
    SessionConnectionError,
    SessionTimeoutError
)
from TechVJ.resource_calculator_v2 import (
    calculate_optimal_config,
    OPTIMAL_CONFIG
)

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

DEFAULT_TEMP_DIR = "downloads/temp"


# ============================================================================
# RESULT TYPES
# ============================================================================

@dataclass
class MediaTransferResult:
    """Result of a complete media transfer operation."""
    success: bool = False
    cancelled: bool = False
    error: Optional[str] = None
    
    # File information
    file_path: Optional[str] = None
    file_size: int = 0
    media_type: Optional[str] = None
    
    # Sent message (if successful)
    sent_message: Optional[Message] = None
    
    # Timing
    download_time: float = 0.0
    upload_time: float = 0.0
    total_time: float = 0.0


@dataclass
class BatchTransferResult:
    """Result of a batch transfer operation."""
    total: int = 0
    successful: int = 0
    failed: int = 0
    cancelled: int = 0
    
    results: List[MediaTransferResult] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.successful / self.total * 100


# ============================================================================
# CANCEL CHECKER TYPE
# ============================================================================

CancelChecker = Callable[[], Union[bool, Awaitable[bool]]]


async def check_cancellation(checker: Optional[CancelChecker]) -> bool:
    """Check if operation should be cancelled."""
    if checker is None:
        return False
    result = checker()
    if asyncio.iscoroutine(result):
        return await result
    return bool(result)


# ============================================================================
# MAIN TRANSFER CLASS
# ============================================================================

class TelegramTransfer:
    """
    Complete Telegram media transfer system.
    
    Handles the full download -> upload pipeline with:
    - Real-time progress updates
    - Automatic cleanup
    - Cancellation support
    - Error recovery
    
    All operations run on a single asyncio event loop.
    """
    
    def __init__(self, temp_dir: str = DEFAULT_TEMP_DIR):
        """
        Initialize the transfer system.
        
        Args:
            temp_dir: Directory for temporary files
        """
        self.temp_dir = ensure_temp_dir(temp_dir)
        
    async def transfer_media(
        self,
        user_client: Client,
        source_message: Message,
        bot_client: Client,
        dest_chat_id: int,
        status_message: Message,
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
        check_cancelled: Optional[CancelChecker] = None,
        cleanup_file_after: bool = True
    ) -> MediaTransferResult:
        """
        Transfer media from source to destination with progress.
        
        This is the main entry point for single-file transfers.
        
        Args:
            user_client: Pyrogram client with access to source (user session)
            source_message: Message containing media to download
            bot_client: Pyrogram bot client for uploading
            dest_chat_id: Destination chat ID (usually user's chat with bot)
            status_message: Message to update with progress
            caption: Optional caption (uses original if not provided)
            reply_to_message_id: Optional message to reply to
            check_cancelled: Optional cancellation checker
            cleanup_file_after: Whether to delete temp file after
            
        Returns:
            MediaTransferResult with success/failure info
        """
        import time
        start_time = time.time()
        download_end = 0.0
        file_path: Optional[str] = None
        
        result = MediaTransferResult()
        
        try:
            # Get media info
            result.media_type = get_media_type(source_message)
            
            if not source_message.media:
                result.error = "Message has no downloadable media"
                return result
            
            # ---------- PHASE 1: DOWNLOAD ----------
            
            download_result = await async_downloader.download(
                client=user_client,
                message=source_message,
                status_client=bot_client,
                status_message=status_message,
                check_cancelled=check_cancelled
            )
            
            download_end = time.time()
            result.download_time = download_end - start_time
            
            if download_result.status == TransferStatus.CANCELLED:
                result.cancelled = True
                result.error = "Download cancelled"
                return result
            
            if download_result.status != TransferStatus.SUCCESS:
                result.error = download_result.error or "Download failed"
                return result
            
            file_path = download_result.file_path
            result.file_path = file_path
            result.file_size = download_result.bytes_transferred
            
            # Check cancellation between phases
            if await check_cancellation(check_cancelled):
                result.cancelled = True
                result.error = "Cancelled between download and upload"
                return result
            
            # ---------- PHASE 2: UPLOAD ----------
            
            # Prepare caption
            final_caption = caption
            if final_caption is None and source_message.caption:
                final_caption = source_message.caption
            
            # Determine upload type
            upload_type = result.media_type or "document"
            
            upload_result = await async_uploader.upload(
                client=bot_client,
                chat_id=dest_chat_id,
                file_path=file_path,
                status_message=status_message,
                media_type=upload_type,
                caption=final_caption,
                reply_to_message_id=reply_to_message_id,
                check_cancelled=check_cancelled
            )
            
            upload_end = time.time()
            result.upload_time = upload_end - download_end
            result.total_time = upload_end - start_time
            
            if upload_result.status == TransferStatus.CANCELLED:
                result.cancelled = True
                result.error = "Upload cancelled"
                return result
            
            if upload_result.status != TransferStatus.SUCCESS:
                result.error = upload_result.error or "Upload failed"
                return result
            
            # Success!
            result.success = True
            result.sent_message = upload_result.message
            return result
        
        except asyncio.CancelledError:
            result.cancelled = True
            result.error = "Operation cancelled"
            raise
        
        except Exception as e:
            logger.error(f"Transfer error: {e}")
            result.error = str(e)
            return result
        
        finally:
            # Cleanup temp file
            if cleanup_file_after and file_path:
                cleanup_file(file_path)
    
    async def download_only(
        self,
        user_client: Client,
        source_message: Message,
        bot_client: Client,
        status_message: Message,
        destination: Optional[str] = None,
        check_cancelled: Optional[CancelChecker] = None
    ) -> TransferResult:
        """
        Download media without uploading.
        
        Useful when you need to process the file before uploading.
        
        Args:
            user_client: Client with access to source
            source_message: Message containing media
            bot_client: Client for progress message edits
            status_message: Message to update
            destination: Optional destination path
            check_cancelled: Optional cancellation checker
            
        Returns:
            TransferResult with file path
        """
        return await async_downloader.download(
            client=user_client,
            message=source_message,
            status_client=bot_client,
            status_message=status_message,
            destination=destination,
            check_cancelled=check_cancelled
        )
    
    async def upload_only(
        self,
        bot_client: Client,
        chat_id: int,
        file_path: str,
        status_message: Message,
        media_type: str = "document",
        caption: Optional[str] = None,
        reply_to_message_id: Optional[int] = None,
        check_cancelled: Optional[CancelChecker] = None,
        **kwargs
    ) -> TransferResult:
        """
        Upload a local file.
        
        Args:
            bot_client: Pyrogram client for uploading
            chat_id: Destination chat ID
            file_path: Path to file
            status_message: Message to update
            media_type: Type of media
            caption: Optional caption
            reply_to_message_id: Optional reply
            check_cancelled: Optional cancellation checker
            **kwargs: Additional send arguments
            
        Returns:
            TransferResult with sent message
        """
        return await async_uploader.upload(
            client=bot_client,
            chat_id=chat_id,
            file_path=file_path,
            status_message=status_message,
            media_type=media_type,
            caption=caption,
            reply_to_message_id=reply_to_message_id,
            check_cancelled=check_cancelled,
            **kwargs
        )


# ============================================================================
# GLOBAL INSTANCE
# ============================================================================

# Single instance for easy import
telegram_transfer = TelegramTransfer()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def transfer_media(
    user_client: Client,
    source_message: Message,
    bot_client: Client,
    dest_chat_id: int,
    status_message: Message,
    caption: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
    check_cancelled: Optional[CancelChecker] = None
) -> MediaTransferResult:
    """
    Transfer media from source to destination.
    
    Convenience function using the global instance.
    """
    return await telegram_transfer.transfer_media(
        user_client=user_client,
        source_message=source_message,
        bot_client=bot_client,
        dest_chat_id=dest_chat_id,
        status_message=status_message,
        caption=caption,
        reply_to_message_id=reply_to_message_id,
        check_cancelled=check_cancelled
    )


# ============================================================================
# INTEGRATION EXAMPLE
# ============================================================================

async def example_usage():
    """
    Example showing complete integration.
    
    This code demonstrates how to use the system in a real Telegram bot.
    """
    # This is example code - do not run directly
    
    from pyrogram import Client
    from database.async_db import async_db
    
    async def handle_save_request(bot: Client, message: Message, source_message: Message):
        """Example handler for a /save command."""
        
        user_id = message.chat.id
        
        # Get user's session from database
        user_data = await async_db.find_user(user_id)
        if not user_data or not user_data.get('session'):
            await message.reply("Please login first with /login")
            return
        
        session_string = user_data['session']
        
        # Create status message
        status_msg = await bot.send_message(
            user_id,
            "Starting download..."
        )
        
        # Track cancellation
        cancelled = False
        def check_cancel():
            return cancelled
        
        try:
            # Use session context with automatic cleanup
            async with create_session(session_string, user_id) as user_client:
                
                # Transfer media with progress
                result = await transfer_media(
                    user_client=user_client,
                    source_message=source_message,
                    bot_client=bot,
                    dest_chat_id=user_id,
                    status_message=status_msg,
                    check_cancelled=check_cancel
                )
                
                if result.success:
                    await bot.edit_message_text(
                        user_id,
                        status_msg.id,
                        f"✅ Transfer complete!\n"
                        f"Time: {result.total_time:.1f}s"
                    )
                elif result.cancelled:
                    await bot.edit_message_text(
                        user_id,
                        status_msg.id,
                        "❌ Transfer cancelled"
                    )
                else:
                    await bot.edit_message_text(
                        user_id,
                        status_msg.id,
                        f"❌ Transfer failed: {result.error}"
                    )
        
        except SessionInvalidError:
            await bot.edit_message_text(
                user_id,
                status_msg.id,
                "❌ Session expired. Please /login again."
            )
            # Optionally clear session from database
            await async_db.update_user(user_id, {"logged_in": False, "session": None})
        
        except SessionConnectionError as e:
            await bot.edit_message_text(
                user_id,
                status_msg.id,
                f"❌ Connection error: {e}\nPlease try again."
            )


# ============================================================================
# RE-EXPORTS FOR CONVENIENCE
# ============================================================================

__all__ = [
    # Main class
    'TelegramTransfer',
    'telegram_transfer',
    
    # Result types
    'MediaTransferResult',
    'TransferResult',
    'TransferStatus',
    
    # Session management
    'create_session',
    'SessionContext',
    'SessionInvalidError',
    'SessionConnectionError',
    'SessionTimeoutError',
    
    # Progress
    'async_progress_controller',
    'TransferType',
    
    # Resource config
    'OPTIMAL_CONFIG',
    'calculate_optimal_config',
    
    # Convenience function
    'transfer_media',
]
