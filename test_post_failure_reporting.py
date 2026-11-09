from __future__ import annotations

import asyncio
import inspect
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from wzgram import Client
from wzgram.enums import ParseMode
from wzgram.errors import FloodWait

from TechVJ import save
from core.failure_classifier import FailureCategory, classify_failure
from core.user_upload_worker import make_send_fn


@pytest.mark.parametrize('cancel', [False, True])
def test_album_flood_resumes_only_current_batch(monkeypatch, tmp_path, cancel):
    from TechVJ.album_collector_v2 import AlbumPhoto, CollectedAlbum, send_album

    async def run():
        album = CollectedAlbum('synthetic', 1, 12)
        for post_id in range(1, 13):
            path = tmp_path / f'{post_id}.jpg'
            path.write_bytes(b'synthetic-photo')
            album.photos.append(AlbumPhoto(post_id, str(path)))
        bot = SimpleNamespace(me=SimpleNamespace(id=777, username='synthetic_bot'))
        upload = SimpleNamespace(send_media_group=AsyncMock(side_effect=[
            None, FloodWait(290), None]))
        wait = AsyncMock(side_effect=asyncio.CancelledError() if cancel else None)
        monkeypatch.setattr(save.asyncio, 'sleep', AsyncMock())
        call = send_album(bot, upload, album, 123, 1, AsyncMock(return_value=False),
                          on_flood_wait=wait)
        if cancel:
            with pytest.raises(asyncio.CancelledError):
                await call
        else:
            assert await call
            calls = upload.send_media_group.await_args_list
            assert len(calls[0].args[1]) == 10
            assert len(calls[1].args[1]) == 2
            assert calls[1] == calls[2]
        assert upload.send_media_group.await_count == (2 if cancel else 3)
        wait.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('throttled', [False, True])
@pytest.mark.parametrize('stop_kind', [
    'flood', 'flood_short', 'flood_last', 'flood_repeated', 'premium_flood',
    'cancel_flag', 'cancel_task', 'errors', 'complete',
])
def test_private_range_final_status_preserves_stop_reason(monkeypatch, throttled, stop_kind):
    async def run():
        notice = AsyncMock()
        pipeline = SimpleNamespace(
            get_temp_dir=AsyncMock(return_value='/tmp/mock-range'),
            check_cancelled=AsyncMock(side_effect=lambda:
                stop_kind == 'cancel_flag' and notice.await_count > 0))
        acc = SimpleNamespace(
            get_chat=AsyncMock(return_value=None), get_messages=AsyncMock(return_value=None))

        @asynccontextmanager
        async def open_pipeline(*args, **kwargs):
            yield pipeline

        @asynccontextmanager
        async def open_session(*args, **kwargs):
            yield acc

        client = SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(id=40)),
            edit_message_text=AsyncMock(), pin_chat_message=AsyncMock(),
            unpin_chat_message=AsyncMock())
        batch = SimpleNamespace(
            check_pause=lambda _: 0, record_flood=lambda _: None,
            record_error=lambda: None, record_success=lambda: None,
            should_stop=lambda: stop_kind == 'errors', finish=lambda: None)
        tracker = SimpleNamespace(update=AsyncMock(), finish=AsyncMock())
        monkeypatch.setattr(save, 'StopSafePipeline', open_pipeline)
        monkeypatch.setattr(save, '_open_download_session', open_session)
        monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={
            'logged_in': True, 'session': 'synthetic'}))
        monkeypatch.setattr(save, '_resolve_peer_safe', AsyncMock())
        monkeypatch.setattr(save, '_send_source_context_message', AsyncMock(return_value=SimpleNamespace(id=41)))
        monkeypatch.setattr(save, 'ping_activity', AsyncMock())
        monkeypatch.setattr(save, 'detect_album_boundary', AsyncMock(return_value=None))
        monkeypatch.setattr(save, '_notify_realtime_post_failure', AsyncMock())
        monkeypatch.setattr(save, '_THROTTLE_AVAILABLE', throttled)
        monkeypatch.setattr(save, 'StatusTracker', lambda *args: tracker)
        monkeypatch.setattr(save, 'BatchController', lambda *args: batch)
        sleep = AsyncMock()
        if stop_kind == 'cancel_task':
            async def cancel_during_wait(seconds):
                if notice.await_count:
                    raise asyncio.CancelledError()
            sleep.side_effect = cancel_during_wait
        monkeypatch.setattr(save.asyncio, 'sleep', sleep)
        monkeypatch.setattr(save, 'core_safe_send', notice)
        attempted = []
        flood_id = 163 if stop_kind == 'flood_last' else 162
        flood_count = 0 if stop_kind in ('errors', 'complete') else (
            2 if stop_kind == 'flood_repeated' else 1)
        initial_flood_count = flood_count

        async def process_post(*args, **kwargs):
            nonlocal flood_count
            post_id = args[4]
            attempted.append(post_id)
            if post_id == flood_id and flood_count:
                flood_count -= 1
                if stop_kind == 'premium_flood':
                    raise RuntimeError('FLOOD_PREMIUM_WAIT_290')
                raise FloodWait(3 if stop_kind == 'flood_short' else 290)
            return stop_kind != 'errors'

        process = AsyncMock(side_effect=process_post)
        monkeypatch.setattr(save, 'process_single_post', process)
        parsed = SimpleNamespace(post_ids=[161, 162, 163], channel_id=-1001234567890)
        await inspect.unwrap(save.process_private_posts)(
            client, SimpleNamespace(id=1), parsed, context=SimpleNamespace(user_id=123))
        final = client.edit_message_text.await_args.args[2]
        if stop_kind.startswith('cancel_'):
            assert 'Bekor qilindi' in final
            assert 'Tugadi' not in final
            assert attempted == [161, 162]
        elif initial_flood_count:
            assert 'Tugadi' in final and 'Yuklandi: 3/3' in final
            assert attempted == (
                [161, 162] + [flood_id] * initial_flood_count + [163]
                if stop_kind == 'flood_last' else
                [161] + [162] * (initial_flood_count + 1) + [163])
            assert notice.await_count == initial_flood_count
            wait_text = notice.await_args.args[2]
            assert f'Yetib kelgan post ID: {flood_id}' in wait_text
            assert f'aynan {flood_id}-postdan avtomatik' in wait_text
            assert 'Havolani qayta yuborishingiz shart emas' in wait_text
            assert '/stop' in wait_text
            assert notice.await_args.args[0] is client
            wait = 4 if stop_kind == 'flood_short' else 291
            assert f'Kutish: {wait} soniya' in wait_text
            assert sum(c.args[0] for c in sleep.await_args_list) == (
                wait * initial_flood_count + 3 * 1.5)
            assert all(c.args[0] <= 5 for c in sleep.await_args_list)
        elif stop_kind == 'errors' and throttled:
            assert "Ko'p xato" in final and 'Tugadi' not in final
        elif stop_kind == 'errors':
            assert "Hech qanday xabar jo'natilmadi" in final
        else:
            assert 'Tugadi' in final
        client.unpin_chat_message.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('error', [
    TypeError("Got unexpected keyword argument(s): {'parse_mode'}"),
    "TypeError: Got unexpected keyword argument(s): {'parse_mode'}",
    AttributeError('parser missing'),
])
def test_programming_error_is_not_classified_as_user_input(error):
    assert classify_failure('send_media', str(error), error) == FailureCategory.SYSTEM_FAILURE


@pytest.mark.parametrize('stage,reason,error,expected', [
    ('parse', 'Invalid URL', ValueError('Invalid URL'), FailureCategory.USER_INPUT_ERROR),
    ('send_media', 'post deleted', None, FailureCategory.EXPECTED_TELEGRAM_STATE),
    ('send_media', 'FLOOD_WAIT_10', None, FailureCategory.EXPECTED_TELEGRAM_STATE),
])
def test_normal_operational_failures_keep_existing_classification(stage, reason, error, expected):
    assert classify_failure(stage, reason, error) == expected


def test_video_note_omits_unsupported_parse_mode_preserving_reply():
    async def run():
        params = inspect.signature(Client.send_video_note).parameters
        assert 'parse_mode' not in params
        async def strict_send(chat_id, path, **kwargs):
            assert set(kwargs).issubset(params)
            return SimpleNamespace(id=5)
        uploader = SimpleNamespace(send_video_note=AsyncMock(side_effect=strict_send))
        kwargs = {'caption': 'caption', 'caption_entities': [],
                  'parse_mode': ParseMode.DISABLED, 'disable_notification': True}
        sent = await make_send_fn(777, 'VideoNote', '/tmp/note.mp4', kwargs)(uploader)
        assert sent.id == 5
        uploader.send_video_note.assert_awaited_once_with(
            777, '/tmp/note.mp4', disable_notification=True)
        assert 'parse_mode' in kwargs  # Caller data is not mutated.
    asyncio.run(run())


@pytest.mark.parametrize('verified', [True, False])
def test_owner_gets_exact_error_even_if_context_verification_fails(monkeypatch, verified):
    async def run():
        monkeypatch.setattr(save, 'OWNER_ID', 999)
        monkeypatch.setattr(save, 'ping_activity', AsyncMock())
        monkeypatch.setattr(save, '_owner_diag_verified_context', AsyncMock(return_value=(
            verified, {'skip_reason': '' if verified else 'channel_unavailable',
                       'me': None, 'chat': None, 'member_status': '-'})))
        persist = AsyncMock(return_value=12)
        monkeypatch.setattr(save, '_log_failed_download_local', persist)
        client = SimpleNamespace(send_message=AsyncMock())
        source = SimpleNamespace(id=3, empty=False, media=None)
        acc = SimpleNamespace(get_messages=AsyncMock())
        error = TypeError("Got unexpected keyword argument(s): {'parse_mode'}")
        await save._notify_realtime_post_failure(
            client, acc, user_id=123, chat_id=-100456, post_id=3,
            stage='send_media', reason=str(error), error=error, source_msg=source)
        owner_calls = [call for call in client.send_message.await_args_list if call.args[0] == 999]
        assert owner_calls
        report = '\n'.join(call.args[1] for call in owner_calls)
        assert 'Post ID: 3' in report and 'TypeError' in report and 'parse_mode' in report
        assert f"Context verified: {'yes' if verified else 'no'}" in report
        user_calls = [call for call in client.send_message.await_args_list if call.args[0] == 123]
        assert 'botning ichki xatosi' in user_calls[0].args[1]
        persist.assert_awaited_once()
        acc.get_messages.assert_not_awaited()
    asyncio.run(run())
