from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from wzgram.errors import FloodWait

from core import upload_coordinator as module
from core.upload_coordinator import UploadCoordinator, upload_wait_notifier
from core.user_upload_worker import UserUploadWorker, UploadTask
from TechVJ import save


def client(uid=123):
    return SimpleNamespace(me=SimpleNamespace(id=uid))


@pytest.fixture
def clock(monkeypatch):
    value = [100.0]
    real_sleep = asyncio.sleep
    async def sleep(seconds):
        value[0] += seconds
        await real_sleep(0)
    monkeypatch.setattr(module, 'monotonic', lambda: value[0])
    monkeypatch.setattr(module.asyncio, 'sleep', sleep)
    return value


def test_split_and_worker_share_account_queue_but_other_accounts_run():
    async def run():
        started, release, queued = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def split_send(*args, **kwargs):
            started.set()
            await release.wait()
            return 'split'
        acc = client()
        acc.send_document = split_send
        first = asyncio.create_task(save._send_split_part_with_peer_retry(
            acc=acc, target_chat_id=777, bot_client=client(777),
            msg_type='Document', chunk_path='part.zip', send_kwargs={},
        ))
        await started.wait()
        worker = UserUploadWorker('different-session-same-account', 456)
        worker._client = client(123)
        async def notice(*args):
            queued.set()
        send = AsyncMock(return_value='worker')
        task = UploadTask(send, future=asyncio.get_running_loop().create_future(), on_wait=notice)
        second = asyncio.create_task(worker._send_coordinated(task))
        await asyncio.wait_for(queued.wait(), 1)
        send.assert_not_awaited()
        other = AsyncMock(return_value='other account')
        assert await module.upload_coordinator.send(client(789), other) == 'other account'
        release.set()
        assert await first == 'split'
        assert await second == 'worker'
        send.assert_awaited_once_with(worker._client)
    asyncio.run(run())


def test_flood_blocks_another_client_until_full_wait_with_buffer(clock):
    async def run():
        guard = UploadCoordinator()
        first, second = client(), client()
        error = FloodWait(8)
        with pytest.raises(FloodWait) as caught:
            await guard.send(first, AsyncMock(side_effect=error))
        assert caught.value is error
        calls = []
        async def send(_):
            calls.append(clock[0])
            return 'ok'
        notice = AsyncMock()
        assert await guard.send(second, send, on_wait=notice) == 'ok'
        assert calls == [109.0]
        notice.assert_awaited_once_with('flood', 9)
    asyncio.run(run())


def test_split_long_flood_also_blocks_worker_for_same_account(clock):
    async def run():
        acc = client()
        acc.send_document = AsyncMock(side_effect=FloodWait(1912))
        with pytest.raises(FloodWait):
            await save._send_split_part_with_peer_retry(
                acc=acc, target_chat_id=777, bot_client=client(777),
                msg_type='Document', chunk_path='part.zip', send_kwargs={},
            )
        assert acc.sleep_threshold == 0 and acc._techvj_flood_safe_upload
        worker = UserUploadWorker('other-session', 456)
        worker._client = client()
        send = AsyncMock()
        task = UploadTask(send, future=asyncio.get_running_loop().create_future(), on_wait=AsyncMock())
        with pytest.raises(FloodWait):
            await worker._send_coordinated(task)
        send.assert_not_awaited()
        assert task.on_wait.await_args.args == ('stopped', 1913)
    asyncio.run(run())


def test_long_flood_survives_failed_request_and_does_not_block_other_account(clock):
    async def run():
        guard = UploadCoordinator()
        error = FloodWait(1912)
        notice = AsyncMock()
        with pytest.raises(FloodWait):
            await guard.send(client(), AsyncMock(side_effect=error), on_wait=notice)
        deadline = guard.state(123).flood_until
        send = AsyncMock()
        with pytest.raises(FloodWait):
            await guard.send(client(), send, on_wait=notice)
        send.assert_not_awaited()
        assert guard.state(123).flood_until == deadline == 2013.0
        assert notice.await_args.args == ('stopped', 1913)
        other = AsyncMock(return_value='ok')
        assert await guard.send(client(456), other) == 'ok'
        clock[0] = deadline
        await guard.send(client(), send)
        send.assert_awaited_once()
    asyncio.run(run())


def test_flood_deadline_only_extends(clock):
    async def run():
        guard = UploadCoordinator()
        guard.mark_flood(123, 100)
        clock[0] += 10
        guard.mark_flood(123, 1)
        assert guard.state(123).flood_until == 201
        guard.mark_flood(123, 200)
        assert guard.state(123).flood_until == 311
    asyncio.run(run())


def test_stop_during_shared_cooldown_releases_lock_without_send(clock):
    async def run():
        guard = UploadCoordinator()
        guard.mark_flood(123, 20)
        send = AsyncMock()
        stop = AsyncMock(side_effect=lambda: clock[0] > 100)
        with pytest.raises(asyncio.CancelledError):
            await guard.send(client(), send, check_cancelled=stop)
        send.assert_not_awaited()
        assert not guard.state(123).lock.locked()
        assert guard.state(123).flood_until == 121
    asyncio.run(run())


def test_cancel_queued_request_does_not_unlock_active_upload():
    async def run():
        guard = UploadCoordinator()
        started, release, queued = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def send(_):
            started.set()
            await release.wait()
        async def notice(*args):
            queued.set()
        first = asyncio.create_task(guard.send(client(), send))
        await started.wait()
        never = AsyncMock()
        second = asyncio.create_task(guard.send(client(), never, on_wait=notice))
        await queued.wait()
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        assert guard.state(123).lock.locked()
        never.assert_not_awaited()
        release.set()
        await first
        assert not guard.state(123).lock.locked()
    asyncio.run(run())


def test_bad_account_identity_fails_before_upload():
    async def run():
        guard = UploadCoordinator()
        send = AsyncMock()
        acc = SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=None)))
        with pytest.raises(RuntimeError, match='identify'):
            await guard.send(acc, send)
        send.assert_not_awaited()
    asyncio.run(run())


def test_account_lookup_cached_and_same_account_uses_one_state():
    async def run():
        guard = UploadCoordinator()
        acc = SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=123)))
        assert await guard.account_id(acc) == await guard.account_id(acc) == 123
        acc.get_me.assert_awaited_once()
        assert guard.state(await guard.account_id(acc)) is guard.state(await guard.account_id(client()))
    asyncio.run(run())


@pytest.mark.parametrize('reason,seconds,expected', [
    ('queue', 0, 'Upload navbatda'),
    ('flood', 31, 'avtomatik davom etadi'),
    ('stopped', 1913, 'upload to‘xtatildi'),
])
def test_user_notice_uses_bot_only(reason, seconds, expected):
    async def run():
        bot = SimpleNamespace(send_message=AsyncMock(return_value='sent'))
        await upload_wait_notifier(bot, 456)(reason, seconds)
        bot.send_message.assert_awaited_once()
        args = bot.send_message.await_args.kwargs
        assert args['chat_id'] == 456
        assert expected in args['text']
        if reason == 'stopped':
            assert 'avtomatik davom etadi' not in args['text']
    asyncio.run(run())


def test_status_failure_does_not_abort_upload(clock):
    async def run():
        guard = UploadCoordinator()
        guard.mark_flood(123, 2)
        send = AsyncMock(return_value='sent')
        assert await guard.send(client(), send, on_wait=AsyncMock(side_effect=RuntimeError())) == 'sent'
    asyncio.run(run())


def test_cancelled_request_does_not_stop_worker_or_send_cancelled_file(monkeypatch):
    async def run():
        worker = UserUploadWorker('synthetic', 123)
        worker._client = client()
        worker._running = True
        # The throttle has slots; patch its class, preserving the real loop.
        monkeypatch.setattr(type(worker._throttle), 'wait_before_send', AsyncMock())
        monkeypatch.setattr(type(worker._throttle), 'check_batch_pause', AsyncMock())
        state = module.upload_coordinator.state(123)
        await state.lock.acquire()
        queued = asyncio.Event()
        async def notice(*args):
            queued.set()
        never = AsyncMock()
        first = UploadTask(never, on_wait=notice)
        worker._task = asyncio.create_task(worker._loop())
        request = asyncio.create_task(worker.enqueue(first))
        await queued.wait()
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        state.lock.release()
        send = AsyncMock(return_value='next request')
        assert await asyncio.wait_for(worker.enqueue(UploadTask(send)), 2) == 'next request'
        never.assert_not_awaited()
        send.assert_awaited_once()
        await worker.stop()
    asyncio.run(run())


@pytest.mark.parametrize('premium', [False, True])
@pytest.mark.parametrize('seconds', [8, 1912])
def test_private_range_resumes_after_full_flood_cooldown(monkeypatch, tmp_path, clock, premium, seconds):
    from TechVJ import lang
    # This regression checks Uzbek cooldown/resume wording explicitly.
    monkeypatch.setattr(lang, "get_user_lang", lambda user_id: "uz")
    class PremiumFlood(Exception):
        ID = 'FLOOD_PREMIUM_WAIT_X'
        x = seconds

    class Context:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def get_temp_dir(self):
            return str(tmp_path)
        async def check_cancelled(self):
            return False

    async def run():
        request = SimpleNamespace(chat=SimpleNamespace(id=123), id=1)
        bot = SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(id=10)),
            edit_message_text=AsyncMock(), unpin_chat_message=AsyncMock(),
        )
        monkeypatch.setattr(save, 'StopSafePipeline', lambda *args: Context())
        monkeypatch.setattr(save, '_open_download_session', lambda *args, **kwargs: Context())
        monkeypatch.setattr(save, '_THROTTLE_AVAILABLE', False)
        monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={'logged_in': True, 'session': 'synthetic'}))
        monkeypatch.setattr(save, '_resolve_peer_safe', AsyncMock())
        monkeypatch.setattr(save, '_send_source_context_message', AsyncMock(return_value=request))
        monkeypatch.setattr(save, 'detect_album_boundary', AsyncMock(return_value=None))
        monkeypatch.setattr(save, 'ping_activity', AsyncMock())
        monkeypatch.setattr(save, '_notify_realtime_post_failure', AsyncMock())
        process = AsyncMock(side_effect=[PremiumFlood() if premium else FloodWait(seconds), True])
        monkeypatch.setattr(save, 'process_single_post', process)
        parsed, _ = save.parse_telegram_url('https://t.me/c/2027730307/62')
        await save.process_private_posts.__wrapped__(bot, request, parsed, context=SimpleNamespace(user_id=123))
        text = bot.edit_message_text.await_args.args[2]
        assert 'Tugadi' in text
        assert process.await_count == 2
        assert process.await_args_list[0].args[4] == process.await_args_list[1].args[4] == 62
        assert clock[0] >= 100 + seconds + 1
        notices = [call.kwargs['text'] if 'text' in call.kwargs else call.args[1]
                   for call in bot.send_message.await_args_list]
        assert any('62-postdan' in notice and str(seconds + 1) in notice for notice in notices)
    asyncio.run(run())
