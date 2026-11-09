"""Bot-scoped portable owner configuration; no task/session runtime state."""
from __future__ import annotations

import asyncio
import copy
from functools import wraps
import json
import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Only owner configuration files. Session credentials stay in the private DB,
# never logs, Git, public artifacts, or owner status replies.
FILES = {
    'role_policy': Path('data/roles/global.json'),
    'archive_policy': Path('data/bot_archive.json'),
    'channels': Path('data/channel_monitor/channels.json'),
    'session_registry': Path('data/session_manager/sessions.json'),
    'premium_sessions': Path('data/premium/sessions.json'),
    'premium_relay': Path('data/premium/relay.json'),
    'system_session': Path('data/premium/system_session.json'),
    'download_sessions': Path('data/download_sessions.json'),
}



class OwnerSettings:
    def __init__(self, path=None, collection=None):
        self.path = Path(path or 'data/owner_settings.json')
        self.collection = collection
        self.bot_id = None
        self.lock = asyncio.Lock()
        self.task = None
        self.status = 'not_started'
        self.restored = False
        try:
            raw = json.loads(self.path.read_text())
        except FileNotFoundError:
            raw = {'values': {}, 'pending': []}
        if not isinstance(raw.get('values'), dict) or not isinstance(raw.get('pending'), list):
            raise ValueError('Invalid owner settings')
        self.values = raw['values']
        self.pending = set(raw['pending'])

    def _write(self, values, pending):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix='.owner-', dir=self.path.parent)
        try:
            with os.fdopen(fd, 'w') as output:
                json.dump({'values': values, 'pending': sorted(pending)}, output)
                output.flush()
                os.fsync(output.fileno())
            os.replace(name, self.path)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    def stage(self, key, value):
        values = dict(self.values, **{key: copy.deepcopy(value)})
        pending = self.pending | {key}
        self._write(values, pending)
        self.values, self.pending = values, pending

    async def sync(self, restore=False):
        if self.bot_id is None:
            return
        async with self.lock:
            try:
                if self.collection is None:
                    from database.async_db import get_db
                    db = get_db()
                    if db is None:
                        self.status = 'local_only'
                        return
                    coll = db.get_collection('bot_settings')
                else:
                    coll = self.collection
                query = {'_id': f'owner_settings:{self.bot_id}'}
                if restore or not self.restored:
                    remote = await asyncio.wait_for(coll.find_one(query), 5)
                    values = dict(self.values)
                    if remote:
                        for key, value in remote.get('values', {}).items():
                            if key not in self.pending:
                                values[key] = value
                    remote_keys = set(remote.get('values', {})) if remote else set()
                    self.pending.update(set(values) - remote_keys)
                    self._write(values, self.pending)
                    self.values = values
                    self.restored = True
                # Capture revisions: stage() can run while Mongo awaits.
                for key in list(self.pending):
                    value = copy.deepcopy(self.values[key])
                    await asyncio.wait_for(coll.update_one(query, {'$set': {
                        f'values.{key}': value}}, upsert=True), 5)
                    if self.values[key] == value:
                        pending = self.pending - {key}
                        self._write(self.values, pending)
                        self.pending = pending
                self.status = 'synced' if not self.pending else 'pending'
            except Exception as error:
                self.status = 'pending' if self.pending else 'restore_failed'
                logger.warning('Owner settings sync: %s', type(error).__name__)

    async def start(self, bot_id):
        self.bot_id = int(bot_id)
        # Legacy local files seed missing remote keys; existing Mongo values win.
        for key, path in FILES.items():
            if key not in self.values and path.exists():
                self.values[key] = json.loads(path.read_text())
        await self.sync(restore=True)
        self.apply()
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._retry())

    async def _retry(self):
        while True:
            await asyncio.sleep(60)
            retry_restore = self.status in ('restore_failed', 'local_only')
            await self.sync(restore=retry_restore)
            if retry_restore:
                self.apply()

    async def stop(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None

    def apply(self):
        for key, path in FILES.items():
            if key not in self.values:
                continue
            value = self.values[key]
            if value is None:
                path.unlink(missing_ok=True)
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix='.restore-', dir=path.parent)
            try:
                with os.fdopen(fd, 'w') as output:
                    json.dump(value, output)
                os.replace(name, path)
            finally:
                if os.path.exists(name):
                    os.unlink(name)
        from core.rate_limiter import rate_limiter
        from core.priority_queue import priority_queue
        from core.role_manager import UserRole, role_manager
        from core.bot_archive import bot_archive_settings
        for role, limit in self.values.get('rate_limits', {}).items():
            rate_limiter.set_limit(UserRole(role), limit)
        for role, limit in self.values.get('parallel_limits', {}).items():
            priority_queue.set_limit(UserRole(role), limit)
        if self.values.get('role_policy') is not None:
            role_manager._write_policy(self.values['role_policy'])
            role_manager._cache.clear()
        if self.values.get('archive_policy') is not None:
            bot_archive_settings._enabled = self.values['archive_policy']['enabled']
        from core.channel_monitor import channel_monitor
        channel_monitor.load()

    async def checkpoint(self, before=None):
        if self.bot_id is None:
            return
        for key, path in FILES.items():
            value = json.loads(path.read_text()) if path.exists() else None
            if before is not None and before.get(key) == value:
                continue
            if key not in self.values or self.values[key] != value:
                self.stage(key, value)
        await self.sync()

    async def change(self, key, value):
        self.stage(key, value)
        await self.sync()

    def notice(self):
        if self.status == 'synced':
            return '\n💾 MongoDBda saqlandi; boshqa serverda ham tiklanadi.'
        return '\n💾 Lokal saqlandi. MongoDB sync kutilmoqda; boshqa serverga hali ko‘chmagan.'


owner_settings = OwnerSettings()


def persist_owner_change(handler):
    @wraps(handler)
    async def wrapped(client, message, *args, **kwargs):
        from config import OWNER_ID
        owner = getattr(getattr(message, 'from_user', None), 'id', None) == OWNER_ID
        before = {}
        if owner and owner_settings.bot_id is not None:
            for key, path in FILES.items():
                before[key] = json.loads(path.read_text()) if path.exists() else None
        result = await handler(client, message, *args, **kwargs)
        if owner:
            try:
                await owner_settings.checkpoint(before)
                if owner_settings.bot_id is not None and owner_settings.status != 'synced':
                    target = getattr(message, 'message', None) or message
                    await target.reply(owner_settings.notice())
            except Exception as error:
                logger.warning('Owner settings checkpoint: %s', type(error).__name__)
                target = getattr(message, 'message', None) or message
                await target.reply('❌ Sozlamani serverlar orasida saqlash bajarilmadi. Qayta urinib ko‘ring.')
        return result
    return wrapped
