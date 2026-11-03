from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from wzgram.errors import FloodWait

from TechVJ import save
from core import premium_logic
from core import upload_coordinator as coordinator_module


@pytest.fixture(autouse=True)
def cooldown_clock(monkeypatch):
    # Existing retry tests mock sleep; advance the account clock by the same
    # amount so the new shared cooldown expires only after the full wait.
    def now():
        sleep = save.asyncio.sleep
        return sum(call.args[0] for call in getattr(sleep, 'await_args_list', []))
    monkeypatch.setattr(coordinator_module, 'monotonic', now)


class PremiumFlood(Exception):
    ID = 'FLOOD_PREMIUM_WAIT_X'
    x = 8


async def send_part(acc, **kwargs):
    acc.me = SimpleNamespace(id=123)
    return await save._send_split_part_with_peer_retry(
        acc=acc, target_chat_id=777, bot_client=SimpleNamespace(),
        msg_type=kwargs.pop('msg_type', 'Document'),
        chunk_path=kwargs.pop('chunk_path', 'part.zip.001'),
        send_kwargs={'caption': 'Part 2/3', 'progress': 'callback'}, **kwargs,
    )


@pytest.mark.parametrize('error', [FloodWait(8), PremiumFlood()])
@pytest.mark.parametrize('video', [False, True])
def test_current_part_retried_after_full_flood_wait(monkeypatch, error, video):
    async def run():
        sleep = AsyncMock()
        monkeypatch.setattr(save.asyncio, 'sleep', sleep)
        send = AsyncMock(side_effect=[error, 'sent'])
        acc = SimpleNamespace(send_document=send, send_video=send)
        status = AsyncMock()
        monkeypatch.setattr(save, '_safe_status_edit_message', status)
        assert await send_part(
            acc, msg_type='Video' if video else 'Document',
            chunk_path='part.mp4' if video else 'part.zip.001',
            status_msg=SimpleNamespace(id=9), status_chat_id=123,
        ) == 'sent'
        assert send.await_args_list[0] == send.await_args_list[1]
        assert sum(c.args[0] for c in sleep.await_args_list) == 9
        status.assert_awaited_once()
        assert status.await_args.args[1:3] == (123, 9)
    asyncio.run(run())


@pytest.mark.parametrize('seconds,attempts,waited', [(1, 4, 6), (100, 3, 202), (301, 1, 0)])
def test_flood_retries_and_total_wait_are_bounded(monkeypatch, seconds, attempts, waited):
    async def run():
        sleep = AsyncMock()
        monkeypatch.setattr(save.asyncio, 'sleep', sleep)
        err = FloodWait(seconds)
        send = AsyncMock(side_effect=err)
        with pytest.raises(FloodWait) as caught:
            await send_part(SimpleNamespace(send_document=send))
        assert caught.value is err
        assert send.await_count == attempts
        assert sum(c.args[0] for c in sleep.await_args_list) == waited
    asyncio.run(run())


def test_flood_after_peer_refresh_retries_refreshed_target(monkeypatch):
    async def run():
        monkeypatch.setattr(save.asyncio, 'sleep', AsyncMock())
        refresh = AsyncMock(return_value=(777, 'the_bot'))
        monkeypatch.setattr(save, '_ensure_user_session_bot_peer', refresh)
        send = AsyncMock(side_effect=[RuntimeError('PEER_ID_INVALID'), FloodWait(1), 'sent'])
        assert await send_part(SimpleNamespace(send_document=send)) == 'sent'
        assert [c.args[0] for c in send.await_args_list] == [777, 'the_bot', 'the_bot']
        refresh.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('task_cancel', [False, True])
def test_cancel_during_flood_wait_never_sends_again(monkeypatch, task_cancel):
    async def run():
        send = AsyncMock(side_effect=FloodWait(20))
        sleep = AsyncMock(side_effect=asyncio.CancelledError() if task_cancel else None)
        stop = AsyncMock(side_effect=lambda: sleep.await_count > 0)
        monkeypatch.setattr(save.asyncio, 'sleep', sleep)
        with pytest.raises(asyncio.CancelledError):
            await send_part(SimpleNamespace(send_document=send), check_cancelled=stop)
        send.assert_awaited_once()
    asyncio.run(run())


def test_unrelated_send_error_is_not_retried(monkeypatch):
    async def run():
        sleep = AsyncMock()
        monkeypatch.setattr(save.asyncio, 'sleep', sleep)
        send = AsyncMock(side_effect=RuntimeError('disk failure'))
        with pytest.raises(RuntimeError, match='disk failure'):
            await send_part(SimpleNamespace(send_document=send))
        send.assert_awaited_once()
        sleep.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('outcome', ['success', 'exhausted', 'cancelled'])
@pytest.mark.parametrize('premium_fallback', [False, True])
def test_split_pipeline_preserves_completed_parts_and_cleans_files(monkeypatch, tmp_path, outcome, premium_fallback):
    async def run():
        source = tmp_path / 'archive.zip'
        source.write_bytes(b'archive')
        chunks = [tmp_path / f'part.{i}' for i in range(3)]
        for path in chunks:
            path.write_bytes(b'part')
        acc = SimpleNamespace(send_document=AsyncMock(), me=SimpleNamespace(id=123))
        sent_paths = []
        failed_once = False

        async def send(target, path, **kwargs):
            nonlocal failed_once
            sent_paths.append(path)
            if path == str(chunks[1]):
                if outcome == 'cancelled':
                    raise asyncio.CancelledError()
                if outcome == 'exhausted' or not failed_once:
                    failed_once = True
                    raise FloodWait(1)
            return SimpleNamespace(id=80, chat=SimpleNamespace(id=777))

        acc.send_document.side_effect = send
        bot = SimpleNamespace(delete_messages=AsyncMock())
        msg = SimpleNamespace(id=62, document=SimpleNamespace(file_size=7, file_name='archive.zip'), caption='archive', caption_entities=[])
        request = SimpleNamespace(chat=SimpleNamespace(id=123), id=12)
        pipeline = SimpleNamespace(check_cancelled=AsyncMock(return_value=False))
        monkeypatch.setattr(save.asyncio, 'sleep', AsyncMock())
        monkeypatch.setattr(save, 'MAX_TELEGRAM_FILE_SIZE', 1)
        monkeypatch.setattr(save, '_ensure_media_route_clients', AsyncMock(return_value=(bot, acc, 777, 123)))
        monkeypatch.setattr(save, 'get_engine', AsyncMock(return_value=SimpleNamespace(download=AsyncMock(return_value=str(source)))))
        monkeypatch.setattr(save, '_prepare_local_upload', AsyncMock(return_value=('Document', {}, None)))
        monkeypatch.setattr(premium_logic, 'check_user_premium', AsyncMock(return_value=premium_fallback))
        monkeypatch.setattr(premium_logic, 'get_user_upload_setting', lambda _: 'auto')
        monkeypatch.setattr(premium_logic, 'should_split', lambda *args: not premium_fallback)
        if premium_fallback:
            from core.user_upload_worker import worker_registry
            monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={'session': 'fake-session'}))
            worker = SimpleNamespace(resolve_bot_reply_message_id=AsyncMock(return_value=None))
            monkeypatch.setattr(worker_registry, 'get_or_create', AsyncMock(return_value=worker))
            monkeypatch.setattr(save, '_cached_client_me', lambda _: SimpleNamespace(username='the_bot'))
            monkeypatch.setattr(save, '_enqueue_media_delivery', AsyncMock(side_effect=RuntimeError('premium upload failed')))
        monkeypatch.setattr(save, 'split_file', lambda _: [str(p) for p in chunks])
        monkeypatch.setattr(save, '_get_user_session_account_id', AsyncMock(return_value=123))
        monkeypatch.setattr(save, '_ensure_user_session_bot_peer', AsyncMock(return_value=(777, 'the_bot')))
        copy = AsyncMock(return_value=True)
        monkeypatch.setattr(save, '_copy_user_session_upload_to_user', copy)
        call = save.download_and_send_media.__wrapped__(
            client=bot, acc=acc, message=request, msg=msg, msg_type='Document',
            pipeline=pipeline, temp_dir=str(tmp_path),
        )
        if outcome == 'success':
            assert await call is True
            assert sent_paths == [str(chunks[i]) for i in [0, 1, 1, 2]]
            assert copy.await_count == 3
        else:
            with pytest.raises(asyncio.CancelledError if outcome == 'cancelled' else FloodWait):
                await call
            assert sent_paths.count(str(chunks[0])) == 1
            assert str(chunks[2]) not in sent_paths
            assert copy.await_count == 1
        assert not source.exists()
        assert all(not p.exists() for p in chunks)
    asyncio.run(run())
