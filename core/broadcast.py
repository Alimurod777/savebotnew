"""Owner broadcasts through the existing bot, isolated from media-save tasks."""
from __future__ import annotations

import asyncio
import copy
import logging
import re
import time
from contextlib import aclosing
from dataclasses import dataclass, field
from typing import List, Optional

from pyrogram.enums import ParseMode
from pyrogram.errors import Forbidden, PeerIdInvalid, InputUserDeactivated, MessageNotModified

from config import OWNER_ID
from core.entity_validator import prepare_entities_for_send, utf16_length
from core.message_utils import MAX_MESSAGE_LENGTH
from core.retry_utils import is_floodwait_error, get_floodwait_seconds, is_transient_error, calculate_backoff
from database.async_db import async_db

logger = logging.getLogger(__name__)
_SUPPORTED = ('text', 'photo', 'video', 'document', 'audio', 'voice', 'animation', 'sticker')
_SOURCE_ERRORS = {'CHAT_FORWARDS_RESTRICTED', 'MESSAGE_ID_INVALID', 'MESSAGE_EMPTY', 'MEDIA_EMPTY', 'MEDIA_INVALID'}
_UNREACHABLE_ERRORS = {'USER_IS_BLOCKED', 'USER_DEACTIVATED', 'INPUT_USER_DEACTIVATED', 'PEER_ID_INVALID', 'CHAT_WRITE_FORBIDDEN', 'CHAT_ID_INVALID'}


class BroadcastBusy(Exception):
    pass


class BroadcastHalted(Exception):
    pass


@dataclass
class BroadcastContent:
    text: Optional[str] = None
    entities: Optional[List] = None
    source_chat_id: Optional[int] = None
    source_message_id: Optional[int] = None

    @classmethod
    def from_command(cls, message):
        raw = str(getattr(message, 'text', '') or '')
        match = re.match(r'^/\S+', raw)
        end = match.end() if match else len(raw)
        if end < len(raw) and raw[end].isspace():
            end += 1  # Remove the command separator, preserving the actual body.
        body = raw[end:]
        if body.strip():
            if utf16_length(body) > MAX_MESSAGE_LENGTH:
                raise ValueError('Broadcast text exceeds Telegram message limit.')
            prefix = utf16_length(raw[:end])
            entities = []
            for entity in getattr(message, 'entities', None) or []:
                start = max(entity.offset, prefix)
                stop = min(entity.offset + entity.length, utf16_length(raw))
                if stop > start:
                    cloned = copy.copy(entity)
                    cloned.offset, cloned.length = start - prefix, stop - start
                    entities.append(cloned)
            return cls(text=body, entities=prepare_entities_for_send(body, entities))
        source = getattr(message, 'reply_to_message', None)
        if not source:
            raise ValueError('Usage: /broadcast Your message\nOr reply to a supported message with /broadcast.')
        chat = getattr(source, 'chat', None)
        if getattr(source, 'has_protected_content', False) or getattr(chat, 'has_protected_content', False):
            raise ValueError('Protected content cannot be broadcast.')
        if not any(getattr(source, name, None) for name in _SUPPORTED):
            raise ValueError('Unsupported message type. Use text, photo, video, document, audio, voice, animation or sticker.')
        if not chat or not getattr(source, 'id', None):
            raise ValueError('The replied-to message is unavailable.')
        return cls(source_chat_id=chat.id, source_message_id=source.id)

    async def send(self, bot, user_id: int):
        if self.source_message_id is not None:
            # Server-side copy preserves media/caption/entities; no download fallback.
            return await bot.copy_message(user_id, self.source_chat_id, self.source_message_id)
        return await bot.send_message(user_id, self.text, entities=self.entities, parse_mode=ParseMode.DISABLED)


@dataclass
class BroadcastJob:
    content: BroadcastContent
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    task: Optional[asyncio.Task] = None
    total: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    excluded: int = 0
    selection_complete: bool = False
    status: str = 'Selecting recipients'
    reason: str = ''
    started_at: float = field(default_factory=time.monotonic)
    finished_at: Optional[float] = None
    progress_id: Optional[int] = None

    @property
    def processed(self):
        return self.successful + self.failed + self.skipped

    def render(self):
        elapsed = (self.finished_at or time.monotonic()) - self.started_at
        qualifier = '' if self.selection_complete else ' (selection incomplete)'
        text = (
            f'Broadcast: {self.status}\n\nTotal eligible recipients: {self.total}{qualifier}'
            f'\nProcessed: {self.processed}\nSuccessful: {self.successful}'
            f'\nFailed: {self.failed}\nSkipped: {self.skipped}'
            f'\nPending: {self.total - self.processed}\nExcluded by policy: {self.excluded}'
            f'\nExecution time: {elapsed:.1f}s'
        )
        if self.reason:
            text += '\n' + self.reason
        if self.finished_at is None:
            text += '\nCancel: /stop broadcast'
        return text


async def recipient_allowed(row, user_id: int) -> bool:
    if any(row.get(name) for name in ('is_bot', 'blocked', 'is_blocked', 'deactivated', 'is_deleted', 'deleted', 'broadcast_opt_out')):
        return False
    if row.get('broadcast_enabled') is False:
        return False
    chat_type = str(getattr(row.get('chat_type'), 'value', row.get('chat_type')) or '').lower()
    if chat_type and chat_type not in ('private', 'user'):
        return False
    # Effective global role/ban policy must override stale individual DB flags.
    from core.role_manager import role_manager
    if await role_manager.is_banned(user_id) or await async_db.is_banned(user_id):
        return False
    from core.mongo_store import get_mongo_store
    store = get_mongo_store()
    return not (store and store.is_connected() and await store.is_blocked(user_id))


class BroadcastManager:
    """One in-memory job; concurrency is deliberately bounded to one delivery.

    All attempts share a 4/s gate and full FloodWait cooldown. No tasks are
    registered with the user's download TaskManager, so targeted cancellation
    cannot stop unrelated downloads. Only one progress child task is created.
    """
    def __init__(self, interval: float = 0.25, max_attempts: int = 3, progress_interval: float = 5):
        self.interval = interval
        self.max_attempts = max_attempts
        self.progress_interval = progress_interval
        self.job: Optional[BroadcastJob] = None
        self._lock = asyncio.Lock()
        self._next_send = 0.0
        self._flood_until = 0.0

    async def start(self, bot, requester_id: int, content: BroadcastContent) -> BroadcastJob:
        if not OWNER_ID or requester_id != OWNER_ID:
            raise PermissionError('Owner only.')
        async with self._lock:
            if self.job and self.job.task and not self.job.task.done():
                raise BroadcastBusy('A broadcast is already running. Cancel it with /stop broadcast.')
            me = getattr(bot, 'me', None) or await bot.get_me()
            if not getattr(me, 'is_bot', False):
                raise PermissionError('Broadcast requires the existing bot account.')
            job = BroadcastJob(content)
            self.job = job
            job.task = asyncio.create_task(self._run(bot, job), name='owner-broadcast')
            return job

    def cancel(self, requester_id: int) -> bool:
        if not OWNER_ID or requester_id != OWNER_ID:
            return False
        job = self.job
        if job is None or job.task is None or job.task.done():
            return False
        job.cancel.set()
        return True

    @staticmethod
    async def _wait(job, seconds):
        if job.cancel.is_set():
            return False
        if seconds <= 0:
            return True
        try:
            await asyncio.wait_for(job.cancel.wait(), timeout=seconds)
            return False
        except asyncio.TimeoutError:
            return True

    def _record_flood(self, error):
        seconds = get_floodwait_seconds(error) + 1
        self._flood_until = max(self._flood_until, time.monotonic() + seconds)
        return seconds

    async def _progress(self, bot, job):
        # Never delay the broadcast with repeated owner progress retries.
        if time.monotonic() < self._flood_until:
            return
        try:
            if job.progress_id is None:
                message = await asyncio.wait_for(bot.send_message(OWNER_ID, job.render(), parse_mode=ParseMode.DISABLED), timeout=15)
                job.progress_id = message.id
            else:
                await asyncio.wait_for(bot.edit_message_text(OWNER_ID, job.progress_id, job.render(), parse_mode=ParseMode.DISABLED), timeout=15)
        except MessageNotModified:
            pass
        except Exception as error:
            if is_floodwait_error(error):
                self._record_flood(error)
            logger.debug('Broadcast progress unavailable: %s', type(error).__name__)

    async def _progress_loop(self, bot, job):
        while True:
            await asyncio.sleep(self.progress_interval)
            await self._progress(bot, job)

    async def _deliver(self, bot, job, user_id):
        for attempt in range(self.max_attempts):
            delay = max(self._next_send, self._flood_until) - time.monotonic()
            if not await self._wait(job, delay):
                return None
            self._next_send = time.monotonic() + self.interval
            try:
                # Existing bot client owns transport and its native short-wait handling.
                result = await asyncio.wait_for(job.content.send(bot, user_id), timeout=90)
                if result is None:
                    return 'failed'
                return 'successful'
            except Exception as error:
                code = str(getattr(error, 'ID', '') or '')
                if code in _SOURCE_ERRORS:
                    raise BroadcastHalted('Source message cannot be copied; broadcast stopped.') from None
                if is_floodwait_error(error):
                    seconds = self._record_flood(error)
                    if seconds > 300:
                        raise BroadcastHalted('Telegram requested a long cooldown; broadcast stopped. Retry after the cooldown.') from None
                elif code in _UNREACHABLE_ERRORS or isinstance(error, (Forbidden, PeerIdInvalid, InputUserDeactivated)):
                    return 'skipped'
                elif is_transient_error(error):
                    if attempt + 1 < self.max_attempts:
                        if not await self._wait(job, calculate_backoff(attempt, base=2, max_backoff=30)):
                            return None
                else:
                    return 'failed'
                if attempt + 1 == self.max_attempts:
                    return 'failed'
        return 'failed'

    async def _run(self, bot, job):
        progress_task = None
        try:
            await self._progress(bot, job)
            progress_task = asyncio.create_task(self._progress_loop(bot, job), name='broadcast-progress')
            recipients, seen = [], set()
            async with aclosing(async_db.iter_registered_users()) as rows:
                async for row in rows:
                    if job.cancel.is_set():
                        break
                    raw_id = row.get('chat_id')
                    if isinstance(raw_id, bool) or not isinstance(raw_id, (int, str)):
                        continue
                    try:
                        uid = int(raw_id)
                    except ValueError:
                        continue
                    if uid <= 0 or uid in seen:
                        continue
                    seen.add(uid)
                    if len(seen) % 200 == 0:
                        await asyncio.sleep(0)
                    if await recipient_allowed(row, uid):
                        recipients.append(uid)
                        job.total += 1
                    else:
                        job.excluded += 1
            if not job.cancel.is_set():
                job.selection_complete = True
                job.status = 'In progress'
            for uid in recipients:
                if job.cancel.is_set():
                    break
                # Recheck policy changes made while this job was running.
                if not await recipient_allowed({}, uid):
                    job.skipped += 1
                    continue
                try:
                    outcome = await self._deliver(bot, job, uid)
                except BroadcastHalted:
                    job.failed += 1
                    raise
                if outcome:
                    setattr(job, outcome, getattr(job, outcome) + 1)
            job.status = 'Cancelled' if job.cancel.is_set() else 'Completed'
        except asyncio.CancelledError:
            job.cancel.set()
            job.status = 'Cancelled'
        except BroadcastHalted as error:
            job.status, job.reason = 'Stopped', str(error)
        except Exception as error:
            job.status = 'Stopped'
            job.reason = 'Recipient database or broadcast operation failed. No automatic restart was scheduled.'
            logger.warning('Broadcast stopped: %s', type(error).__name__)
        finally:
            if progress_task:
                progress_task.cancel()
                await asyncio.gather(progress_task, return_exceptions=True)
            job.finished_at = time.monotonic()
            await self._progress(bot, job)

    async def shutdown(self):
        job = self.job
        if job and job.task and not job.task.done():
            job.cancel.set()
            try:
                await asyncio.wait_for(asyncio.shield(job.task), timeout=5)
            except asyncio.TimeoutError:
                job.task.cancel()
                await asyncio.gather(job.task, return_exceptions=True)


broadcast_manager = BroadcastManager()
