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


@pytest.mark.parametrize('throttled', [False, True])
@pytest.mark.parametrize('stop_kind', ['flood', 'errors', 'complete'])
def test_private_range_final_status_preserves_stop_reason(monkeypatch, throttled, stop_kind):
    async def run():
        pipeline = SimpleNamespace(
            get_temp_dir=AsyncMock(return_value='/tmp/mock-range'),
            check_cancelled=AsyncMock(return_value=False))
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
        monkeypatch.setattr(save.asyncio, 'sleep', AsyncMock())
        process = AsyncMock(return_value=stop_kind == 'complete')
        if stop_kind == 'flood':
            process.side_effect = FloodWait(290)
        monkeypatch.setattr(save, 'process_single_post', process)
        parsed = SimpleNamespace(post_ids=[161, 162, 163], channel_id=-1001234567890)
        await inspect.unwrap(save.process_private_posts)(
            client, SimpleNamespace(id=1), parsed, context=SimpleNamespace(user_id=123))
        final = client.edit_message_text.await_args.args[2]
        if stop_kind == 'flood':
            assert 'FloodWait: 290 soniya' in final
            assert '161-postdan' in final
            assert 'Avtomatik davom etmaydi' in final
            assert 'Joriy va qolgan ID lar: 3' in final
            assert 'Tugadi' not in final
            assert process.await_count == 1
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
