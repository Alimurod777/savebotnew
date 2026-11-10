from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from TechVJ import safe_reconnect as reconnect


@pytest.mark.parametrize('setting', [False, None, True])
def test_reconnect_restores_update_policy_before_start(monkeypatch, setting):
    async def run():
        client = SimpleNamespace(is_connected=True, no_updates=setting)

        async def stop():
            assert client.no_updates is True
            client.is_connected = False

        async def start():
            assert client.no_updates is setting
            client.is_connected = True

        client.stop = AsyncMock(side_effect=stop)
        client.start = AsyncMock(side_effect=start)
        client.get_me = AsyncMock()
        monkeypatch.setattr(reconnect.asyncio, 'sleep', AsyncMock())
        assert await reconnect.safe_reconnect(client)
        assert client.no_updates is setting
        client.start.assert_awaited_once()
        client.get_me.assert_awaited_once()

    asyncio.run(run())


@pytest.mark.parametrize('error', [RuntimeError, asyncio.TimeoutError, asyncio.CancelledError])
def test_disconnect_restores_updates_on_failure_or_cancellation(error):
    async def run():
        client = SimpleNamespace(is_connected=True, no_updates=False,
                                 stop=AsyncMock(side_effect=error()))
        if error is asyncio.CancelledError:
            with pytest.raises(asyncio.CancelledError):
                await reconnect.safe_disconnect(client)
        else:
            assert not await reconnect.safe_disconnect(client)
        assert client.no_updates is False

    asyncio.run(run())


def test_failed_disconnect_never_starts_client(monkeypatch):
    async def run():
        client = SimpleNamespace(is_connected=True, no_updates=False,
                                 stop=AsyncMock(side_effect=RuntimeError()),
                                 start=AsyncMock(), get_me=AsyncMock())
        monkeypatch.setattr(reconnect.asyncio, 'sleep', AsyncMock())
        assert not await reconnect.safe_reconnect(client, max_attempts=2)
        assert client.no_updates is False
        assert client.stop.await_count == 2
        client.start.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize('succeeds', [True, False])
def test_concurrent_reconnects_share_one_attempt(monkeypatch, succeeds):
    async def run():
        entered = asyncio.Event()
        release = asyncio.Event()
        yield_turn = asyncio.sleep
        client = SimpleNamespace(is_connected=True, no_updates=False)

        async def stop():
            entered.set()
            await release.wait()
            client.is_connected = False

        async def start():
            assert client.no_updates is False
            if not succeeds:
                raise RuntimeError('synthetic failure')
            client.is_connected = True

        client.stop = AsyncMock(side_effect=stop)
        client.start = AsyncMock(side_effect=start)
        client.get_me = AsyncMock()
        monkeypatch.setattr(reconnect.asyncio, 'sleep', AsyncMock())
        first = asyncio.create_task(reconnect.safe_reconnect(client, max_attempts=1))
        await entered.wait()
        second = asyncio.create_task(reconnect.safe_reconnect(client, max_attempts=1))
        # Give the second caller a turn to wait for the same reconnect lock.
        await yield_turn(0)
        release.set()
        assert await asyncio.gather(first, second) == [succeeds, succeeds]
        client.stop.assert_awaited_once()
        client.start.assert_awaited_once()
        assert client.no_updates is False

        # A later disconnect must still be able to start a new recovery.
        client.is_connected = False
        assert await reconnect.safe_reconnect(client, max_attempts=1) is succeeds
        assert client.start.await_count == 2

    asyncio.run(run())
