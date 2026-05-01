"""
TechVJ/user_queue.py - User Download Queue Manager

Ensures one download at a time per user with queue support.

Features:
- One active download per user
- New links queued automatically
- Queue status visibility
- /stop clears queue
- Auto-start next item when current finishes

Usage:
    from TechVJ.user_queue import user_queue
    
    # Check if can start or should queue
    position = await user_queue.enqueue(user_id, link_data)
    if position == 0:
        # Start immediately
        await process_link(...)
        await user_queue.complete(user_id)
    else:
        # Queued - notify user
        await message.reply(f"Navbatda: {position}-o'rin")
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable, Awaitable, Tuple
from dataclasses import dataclass, field
from collections import deque
import time

logger = logging.getLogger(__name__)

# Maximum queue size per user
MAX_QUEUE_SIZE = 10


@dataclass
class QueueItem:
    """Single item in user's queue."""
    link: str
    parsed_data: Any  # ParsedURL object
    message_id: int  # Original message ID for reference
    chat_id: int
    added_at: float = field(default_factory=time.time)
    
    def __repr__(self):
        return f"QueueItem(link={self.link[:50]}..., msg={self.message_id})"


@dataclass 
class UserQueueState:
    """Queue state for a single user."""
    queue: deque = field(default_factory=deque)
    is_processing: bool = False
    current_item: Optional[QueueItem] = None
    total_processed: int = 0
    total_queued: int = 0
    
    @property
    def queue_size(self) -> int:
        return len(self.queue)
    
    @property
    def is_empty(self) -> bool:
        return len(self.queue) == 0 and not self.is_processing


class UserQueueManager:
    """
    Manages download queues for all users.
    
    Ensures only ONE download runs at a time per user.
    Additional requests are queued and processed in order.
    """
    
    def __init__(self, max_queue_per_user: int = MAX_QUEUE_SIZE):
        self._queues: Dict[int, UserQueueState] = {}
        self._lock: Optional[asyncio.Lock] = None
        self._max_queue = max_queue_per_user
        
        # Callback for processing queued items
        self._process_callback: Optional[Callable] = None
    
    def _get_lock(self) -> asyncio.Lock:
        """Get or create lock in current event loop."""
        try:
            loop = asyncio.get_running_loop()
            loop_id = id(loop)
        except RuntimeError:
            return asyncio.Lock()
        
        if self._lock is None:
            self._lock = asyncio.Lock()
        
        return self._lock
    
    def _get_state(self, user_id: int) -> UserQueueState:
        """Get or create queue state for user."""
        if user_id not in self._queues:
            self._queues[user_id] = UserQueueState()
        return self._queues[user_id]
    
    async def enqueue(
        self,
        user_id: int,
        link: str,
        parsed_data: Any,
        message_id: int,
        chat_id: int
    ) -> int:
        """
        Add link to user's queue.
        
        Returns:
            0 = Can start immediately (no queue)
            1+ = Position in queue
            -1 = Queue full, rejected
        """
        async with self._get_lock():
            state = self._get_state(user_id)
            
            # Check if currently processing
            if not state.is_processing:
                # Can start immediately
                state.is_processing = True
                state.current_item = QueueItem(
                    link=link,
                    parsed_data=parsed_data,
                    message_id=message_id,
                    chat_id=chat_id
                )
                return 0
            
            # Already processing - add to queue
            if state.queue_size >= self._max_queue:
                logger.warning(f"User {user_id} queue full ({self._max_queue})")
                return -1
            
            item = QueueItem(
                link=link,
                parsed_data=parsed_data,
                message_id=message_id,
                chat_id=chat_id
            )
            state.queue.append(item)
            state.total_queued += 1
            
            position = state.queue_size
            logger.info(f"User {user_id} link queued at position {position}")
            
            return position
    
    async def complete(self, user_id: int) -> Optional[QueueItem]:
        """
        Mark current task as complete and get next item.
        
        Returns:
            Next QueueItem to process, or None if queue empty
        """
        async with self._get_lock():
            state = self._get_state(user_id)
            
            state.total_processed += 1
            state.current_item = None
            
            # Check if there's more in queue
            if state.queue:
                next_item = state.queue.popleft()
                state.current_item = next_item
                # is_processing stays True
                logger.info(f"User {user_id} starting next queued item")
                return next_item
            else:
                # Queue empty
                state.is_processing = False
                return None
    
    async def clear_queue(self, user_id: int) -> int:
        """
        Clear user's queue (called on /stop).
        
        Returns:
            Number of items cleared
        """
        async with self._get_lock():
            state = self._get_state(user_id)
            
            cleared = len(state.queue)
            state.queue.clear()
            state.is_processing = False
            state.current_item = None
            
            logger.info(f"User {user_id} queue cleared: {cleared} items")
            return cleared
    
    async def get_status(self, user_id: int) -> Dict[str, Any]:
        """Get queue status for user."""
        async with self._get_lock():
            state = self._get_state(user_id)
            
            return {
                'is_processing': state.is_processing,
                'queue_size': state.queue_size,
                'current_link': state.current_item.link[:50] + '...' if state.current_item else None,
                'total_processed': state.total_processed,
                'total_queued': state.total_queued,
                'queue_items': [
                    {'link': item.link[:40] + '...', 'position': i + 1}
                    for i, item in enumerate(state.queue)
                ]
            }
    
    def is_processing(self, user_id: int) -> bool:
        """Quick sync check if user has active download."""
        if user_id not in self._queues:
            return False
        return self._queues[user_id].is_processing
    
    def get_queue_size(self, user_id: int) -> int:
        """Quick sync check of queue size."""
        if user_id not in self._queues:
            return 0
        return self._queues[user_id].queue_size
    
    async def peek_next(self, user_id: int) -> Optional[QueueItem]:
        """Peek at next item without removing."""
        async with self._get_lock():
            state = self._get_state(user_id)
            if state.queue:
                return state.queue[0]
            return None
    
    async def remove_from_queue(self, user_id: int, message_id: int) -> bool:
        """Remove specific item from queue by message ID."""
        async with self._get_lock():
            state = self._get_state(user_id)
            
            for i, item in enumerate(state.queue):
                if item.message_id == message_id:
                    del state.queue[i]
                    return True
            return False
    
    def get_all_active_users(self) -> List[int]:
        """Get list of users with active downloads or queues."""
        return [
            user_id for user_id, state in self._queues.items()
            if state.is_processing or state.queue_size > 0
        ]


# Global instance
user_queue = UserQueueManager()


# ============================================================================
# HELPER FUNCTIONS FOR INTEGRATION
# ============================================================================

async def check_and_queue(
    user_id: int,
    link: str,
    parsed_data: Any,
    message_id: int,
    chat_id: int
) -> Tuple[bool, int, str]:
    """
    Check if can process or should queue.
    
    Returns:
        (can_start, position, status_message)
        - can_start=True, position=0: Start immediately
        - can_start=False, position>0: Queued at position
        - can_start=False, position=-1: Queue full
    """
    position = await user_queue.enqueue(
        user_id, link, parsed_data, message_id, chat_id
    )
    
    if position == 0:
        return True, 0, ""
    elif position == -1:
        return False, -1, (
            "⚠️ **Navbat to'lgan**\n\n"
            f"Maksimum {MAX_QUEUE_SIZE} ta havola navbatda bo'lishi mumkin.\n"
            "Oldingi yuklanishlar tugashini kuting yoki /stop bilan bekor qiling."
        )
    else:
        queue_status = await user_queue.get_status(user_id)
        return False, position, (
            f"📋 **Navbatga qo'shildi**\n\n"
            f"O'rni: **{position}** / {queue_status['queue_size']}\n"
            f"Hozir yuklanmoqda: {queue_status['current_link'] or 'N/A'}\n\n"
            f"Navbatingiz kelganda avtomatik boshlanadi.\n"
            f"Bekor qilish: /stop"
        )


async def on_download_complete(user_id: int, client, process_func: Callable) -> None:
    """
    Call when download completes to process next in queue.
    
    Args:
        user_id: User ID
        client: Pyrogram client (bot)
        process_func: Async function to process next item
                      Signature: process_func(client, chat_id, parsed_data, message_id)
    """
    next_item = await user_queue.complete(user_id)
    
    if next_item:
        # Notify user
        try:
            await client.send_message(
                next_item.chat_id,
                f"▶️ **Navbatdan keyingi havola boshlanmoqda...**\n"
                f"`{next_item.link[:60]}...`"
            )
        except Exception as e:
            logger.warning(f"Failed to notify user {user_id}: {e}")
        
        # Process next item
        try:
            await process_func(
                client,
                next_item.chat_id,
                next_item.parsed_data,
                next_item.message_id
            )
        except Exception as e:
            logger.error(f"Error processing queued item for {user_id}: {e}")
            # Try next item
            await on_download_complete(user_id, client, process_func)


def format_queue_status(status: Dict[str, Any]) -> str:
    """Format queue status for display."""
    if not status['is_processing'] and status['queue_size'] == 0:
        return "📭 Navbat bo'sh"
    
    lines = ["📋 **Navbat holati**\n"]
    
    if status['is_processing']:
        lines.append(f"▶️ Hozir: {status['current_link'] or 'Yuklanmoqda...'}")
    
    if status['queue_size'] > 0:
        lines.append(f"\n📝 Navbatda: {status['queue_size']} ta\n")
        for item in status['queue_items'][:5]:  # Show max 5
            lines.append(f"  {item['position']}. {item['link']}")
        if status['queue_size'] > 5:
            lines.append(f"  ... va yana {status['queue_size'] - 5} ta")
    
    lines.append(f"\n📊 Jami yuklangan: {status['total_processed']}")
    
    return "\n".join(lines)
