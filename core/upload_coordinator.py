"""Serialize uploads by Telegram account, across workers and split clients.

State is process-local. Account identity (not requester/session string) is the
key, so two authorized sessions of the same account share the cooldown.
"""
from __future__ import annotations

import asyncio
import math
from time import monotonic
from dataclasses import dataclass, field
from typing import Dict
from weakref import WeakKeyDictionary

from wzgram.errors import FloodWait

from core.retry_utils import MAX_FLOODWAIT_SECONDS, get_floodwait_seconds, is_floodwait_error


@dataclass
class AccountUploadState:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    flood_until: float = 0.0


class UploadCoordinator:
    def __init__(self):
        self._loops = WeakKeyDictionary()

    def state(self, account_id: int) -> AccountUploadState:
        states: Dict[int, AccountUploadState] = self._loops.setdefault(asyncio.get_running_loop(), {})
        return states.setdefault(account_id, AccountUploadState())

    async def account_id(self, client) -> int:
        account_id = getattr(client, '_techvj_session_user_id', None)
        if account_id is None:
            me = getattr(client, 'me', None)
            if getattr(me, 'id', None) is None:
                me = await asyncio.wait_for(client.get_me(), timeout=10)
            account_id = getattr(me, 'id', None)
        if not isinstance(account_id, int) or account_id <= 0:
            raise RuntimeError('Cannot identify upload account safely')
        client._techvj_session_user_id = account_id
        return account_id

    def mark_flood(self, account_id: int, seconds: float):
        state = self.state(account_id)
        state.flood_until = max(state.flood_until, monotonic() + seconds + 1)

    async def send(self, client, send_fn, *, check_cancelled=None, on_wait=None,
                   wait_limit=MAX_FLOODWAIT_SECONDS):
        account_id = await self.account_id(client)
        state = self.state(account_id)

        async def check():
            if getattr(asyncio.current_task(), 'cancelling', lambda: 0)():
                raise asyncio.CancelledError()
            if check_cancelled and await check_cancelled():
                raise asyncio.CancelledError()

        async def notify(reason, seconds):
            if on_wait:
                try:
                    await asyncio.wait_for(on_wait(reason, seconds), timeout=10)
                except Exception:
                    pass  # A status RPC must never break delivery.

        # Poll lock acquisition so /stop is honored without starting an upload.
        announced = False
        while True:
            await check()
            if state.lock.locked() and not announced:
                remaining = max(0, state.flood_until - monotonic())
                reason = 'stopped' if remaining > wait_limit else ('flood' if remaining else 'queue')
                await notify(reason, math.ceil(remaining))
                announced = True
            await check()
            try:
                await asyncio.wait_for(state.lock.acquire(), timeout=1)
                break
            except asyncio.TimeoutError:
                continue
        try:
            deadline = monotonic() + wait_limit
            notified_until = 0.0
            while True:
                await check()
                remaining = state.flood_until - monotonic()
                if remaining <= 0:
                    break
                if state.flood_until > notified_until:
                    await notify('stopped' if state.flood_until > deadline else 'flood', math.ceil(remaining))
                    notified_until = state.flood_until
                if state.flood_until > deadline:
                    # Preserve the full cooldown even though this request stops.
                    raise FloodWait(math.ceil(remaining))
                await asyncio.sleep(min(5, remaining))
            await check()
            previous_notifier = getattr(client, '_techvj_upload_wait_notifier', None)
            client._techvj_upload_wait_notifier = on_wait
            try:
                return await send_fn(client)
            except Exception as error:
                if is_floodwait_error(error):
                    seconds = get_floodwait_seconds(error)
                    self.mark_flood(account_id, seconds)
                    await notify('stopped' if seconds + 1 > wait_limit else 'flood', seconds + 1)
                raise
            finally:
                client._techvj_upload_wait_notifier = previous_notifier
        finally:
            state.lock.release()


def upload_wait_notifier(bot, user_id):
    last_notice = [None, 0.0]
    async def notify(reason, seconds):
        # Parallel file-part workers may see the same flood. One explanation
        # per short interval is enough; terminal notices are never suppressed.
        if reason != 'stopped' and last_notice[0] == reason and monotonic() - last_notice[1] < 15:
            return
        last_notice[:] = [reason, monotonic()]
        from core.safe_send import safe_send_message
        from TechVJ.lang import get_string
        key = {
            'queue': 'account_upload_queue',
            'stopped': 'account_upload_stopped',
        }.get(reason, 'account_upload_flood')
        text = get_string(user_id, key, seconds=seconds)
        await safe_send_message(bot, user_id, text)
    return notify


upload_coordinator = UploadCoordinator()
