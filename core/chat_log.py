"""Owner-configured native forwards of private bot conversations."""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
import json
import logging
import os
from pathlib import Path
import tempfile
import time
from typing import Optional

from pyrogram.enums import ChatType, ParseMode

from config import LOG_CHANNEL_ID
from core.retry_utils import get_floodwait_seconds, is_floodwait_error

logger = logging.getLogger(__name__)
_inside_send = ContextVar('chat_log_inside_send', default=False)


@dataclass(frozen=True)
class LogPolicy:
    chat_id: int = 0
    enabled: bool = False
    source: str = 'env'


class ChatLogSettings:
    def __init__(self, path: Optional[Path] = None, env_chat_id: int = LOG_CHANNEL_ID):
        self.path = path or Path('data/chat_log.json')
        self.env_chat_id = env_chat_id
        self.lock = asyncio.Lock()
        self.revision = 0
        self._policy = None

    @property
    def policy(self) -> LogPolicy:
        if self._policy is None:
            self._policy = LogPolicy(self.env_chat_id, bool(self.env_chat_id))
            try:
                data = json.loads(self.path.read_text(encoding='utf-8'))
                if data.get('override') is True:
                    chat_id, enabled = data['chat_id'], data['enabled']
                    if type(chat_id) is not int or chat_id > 0 or type(enabled) is not bool:
                        raise ValueError('Invalid chat log policy')
                    if enabled and not chat_id:
                        raise ValueError('Missing destination')
                    self._policy = LogPolicy(chat_id, enabled, 'owner')
            except FileNotFoundError:
                pass
            except (OSError, ValueError, KeyError, TypeError, AttributeError):
                # A damaged override must not silently reactivate an old ENV target.
                self._policy = LogPolicy(source='invalid')
                logger.warning('Cannot read chat log policy; logging disabled')
        return self._policy

    async def save(self, policy: LogPolicy) -> None:
        async with self.lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix='.chat_log-', dir=str(self.path.parent))
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as output:
                    json.dump({'override': policy.source == 'owner',
                               'chat_id': policy.chat_id, 'enabled': policy.enabled}, output)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(name, self.path)
            finally:
                if os.path.exists(name):
                    os.unlink(name)
            self._policy = policy
            self.revision += 1

    async def reset(self) -> None:
        await self.save(LogPolicy(self.env_chat_id, bool(self.env_chat_id)))


async def validate_log_chat(bot, target) -> int:
    chat = await bot.get_chat(target)
    if chat.type not in (ChatType.CHANNEL, ChatType.GROUP, ChatType.SUPERGROUP,
                         getattr(ChatType, "FORUM", ChatType.SUPERGROUP)):
        raise ValueError('Log manzili kanal yoki guruh bo‘lishi kerak.')
    me = getattr(bot, 'me', None) or await bot.get_me()
    member = await bot.get_chat_member(chat.id, me.id)
    status = getattr(member.status, 'value', member.status)
    if status not in ('administrator', 'owner', 'creator'):
        raise ValueError('Botni log kanal/guruhiga admin qiling.')
    privileges = getattr(member, 'privileges', None)
    if chat.type == ChatType.CHANNEL and status == 'administrator':
        if not privileges or not getattr(privileges, 'can_post_messages', False):
            raise ValueError('Botga kanalda xabar yuborish huquqini bering.')
    return int(chat.id)


@dataclass
class LogEvent:
    revision: int
    target: int
    chat_id: int
    message_id: int
    album: bool = False
    user_name: str = ""
    username: str = ""


class ChatLogger:
    # Observe return values because Telegram does not send all bot-authored
    # messages back through update handlers. Nested sends are observed once.
    METHODS = (
        'send_message', 'send_photo', 'send_video', 'send_document', 'send_audio',
        'send_animation', 'send_voice', 'send_video_note', 'send_sticker',
        'send_location', 'send_venue', 'send_contact', 'send_poll', 'send_dice',
        'send_game', 'send_cached_media', 'send_web_page', 'send_media_group', 'copy_message', 'copy_media_group',
        'forward_messages', 'edit_message_text', 'edit_message_caption',
        'edit_message_media', 'edit_message_live_location',
    )

    def __init__(self, settings=None, queue_size=1000, interval=0.4, header_gap=300):
        self.settings = settings or ChatLogSettings()
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.interval = interval
        self.header_gap = header_gap
        self._last_header_key = None
        self._last_activity = 0.0
        self.task = None
        self.bot = None
        self.sent = self.failed = self.dropped = 0
        self.last_error = '-'
        self._seen = OrderedDict()
        self._albums = OrderedDict()
        self._cooldown = 0.0
        self._changed = asyncio.Event()

    def attach(self, bot):
        if getattr(bot, '_chat_log_attached', False):
            return
        bot._chat_log_attached = True
        for name in self.METHODS:
            original = getattr(bot, name, None)
            if original is not None:
                setattr(bot, name, self._wrap(original))

    def _wrap(self, original):
        @wraps(original)
        async def observed(*args, **kwargs):
            if _inside_send.get():
                return await original(*args, **kwargs)
            token = _inside_send.set(True)
            try:
                result = await original(*args, **kwargs)
            finally:
                _inside_send.reset(token)
            # Logging must never change the result of a successful user send.
            try:
                for message in result if isinstance(result, (list, tuple)) else [result]:
                    self.submit(message)
            except Exception:
                logger.warning('Chat log observation failed')
            return result
        return observed

    async def start(self, bot):
        self.bot = bot
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._run(), name='chat-log')

    async def stop(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None
        while not self.queue.empty():
            self.queue.get_nowait()
            self.queue.task_done()
            self.dropped += 1
        self._seen.clear()
        self._albums.clear()
        self._last_header_key = None

    def submit(self, msg):
        if msg is None or self.task is None or self.task.done():
            return
        policy = self.settings.policy
        chat = getattr(msg, 'chat', None)
        if not policy.enabled or not chat or getattr(chat, 'type', None) != ChatType.PRIVATE:
            return
        if chat.id == policy.chat_id or getattr(msg, 'empty', False) or getattr(msg, 'service', None):
            return
        message_id = getattr(msg, 'id', None)
        if not message_id:
            return
        # Edits with new content may be logged, but update + send-return duplicates
        # and nested copy/send calls must not generate two log entries.
        text = getattr(msg, 'text', None)
        caption = getattr(msg, 'caption', None)
        entities = getattr(msg, 'entities', None)
        cap_entities = getattr(msg, 'caption_entities', None)
        signature = (self.settings.revision, chat.id, message_id,
                     str(getattr(msg, 'edit_date', None)), str(text), str(caption),
                     str(entities), str(cap_entities))
        if signature in self._seen:
            return
        album_id = getattr(msg, 'media_group_id', None)
        album_key = (self.settings.revision, chat.id, album_id)
        if album_id and album_key in self._albums:
            return
        peer = chat
        sender = getattr(msg, 'from_user', None)
        if not getattr(peer, 'first_name', None) and sender and getattr(sender, 'id', None) == chat.id:
            peer = sender
        name = ' '.join(str(getattr(peer, key, '') or '') for key in ('first_name', 'last_name')).strip()
        name = ' '.join(name.split())[:160] or 'Noma’lum'
        username = str(getattr(peer, 'username', '') or '').lstrip('@')[:64]
        event = LogEvent(
            revision=self.settings.revision, target=policy.chat_id,
            chat_id=chat.id, message_id=message_id,
            album=bool(album_id), user_name=name, username=username,
        )
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1
            self.last_error = 'QueueFull'
            return
        self._seen[signature] = None
        if len(self._seen) > 10000:
            self._seen.popitem(last=False)
        if album_id:
            self._albums[album_key] = None
            if len(self._albums) > 10000:
                self._albums.popitem(last=False)

    async def configure(self, policy):
        # A configuration change waits for an in-flight RPC. After it returns,
        # no queued old-generation event is sent to either destination.
        await self.settings.save(policy)
        self._changed.set()

    async def _deliver(self, event):
        if event.album:
            await asyncio.sleep(0.8)  # Allow the remaining album updates to arrive.
        for attempt in range(3):
            while self._cooldown > time.monotonic():
                if event.revision != self.settings.revision:
                    self.dropped += 1
                    return
                try:
                    await asyncio.wait_for(self._changed.wait(), timeout=self._cooldown - time.monotonic())
                    self._changed.clear()
                except asyncio.TimeoutError:
                    break
            async with self.settings.lock:
                policy = self.settings.policy
                if event.revision != self.settings.revision or not policy.enabled or event.target != policy.chat_id:
                    self.dropped += 1
                    return
                token = _inside_send.set(True)
                try:
                    header_key = (event.revision, event.chat_id)
                    if self._last_header_key != header_key or time.monotonic() - self._last_activity >= self.header_gap:
                        header = (f"👤 Foydalanuvchi: {event.user_name}\n"
                                  f"ID: {event.chat_id}\n"
                                  f"Username: {'@' + event.username if event.username else 'yo‘q'}")
                        await asyncio.wait_for(self.bot.send_message(
                            event.target, header, parse_mode=ParseMode.DISABLED,
                        ), timeout=90)
                        self._last_header_key = header_key
                        self._last_activity = time.monotonic()
                    ids = event.message_id
                    if event.album:
                        group = await asyncio.wait_for(
                            self.bot.get_media_group(event.chat_id, event.message_id), timeout=30,
                        )
                        ids = sorted(message.id for message in group)
                        if not ids:
                            raise ValueError('Empty media group')
                    # Forward directly from the bot-user chat, preserving Telegram's
                    # original sender attribution and contents. No download/reupload.
                    await asyncio.wait_for(self.bot.forward_messages(
                        event.target, event.chat_id, ids,
                    ), timeout=90)
                    self._last_activity = time.monotonic()
                    self.sent += 1
                    self.last_error = '-'
                    self._cooldown = time.monotonic() + self.interval
                    return
                except Exception as error:
                    self.last_error = type(error).__name__
                    if is_floodwait_error(error):
                        seconds = get_floodwait_seconds(error) + 1
                        self._cooldown = time.monotonic() + seconds
                        if seconds <= 300 and attempt < 2:
                            continue
                    self.failed += 1
                    logger.warning('Chat log delivery failed: %s', type(error).__name__)
                    return
                finally:
                    _inside_send.reset(token)

    async def _run(self):
        while True:
            event = await self.queue.get()
            try:
                await self._deliver(event)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.failed += 1
                self.last_error = type(error).__name__
                logger.warning('Chat log worker error: %s', type(error).__name__)
            finally:
                self.queue.task_done()


chat_logger = ChatLogger()
