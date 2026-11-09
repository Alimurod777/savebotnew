from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from wzgram.types import ReplyParameters

from TechVJ import save
from core import premium_logic
from core.reply_compat import get_reply_message_id
from core.user_upload_worker import UserUploadWorker, worker_registry


@pytest.mark.parametrize('route', ['legacy', 'premium', 'pool_fallback'])
@pytest.mark.parametrize('resolved', [None, 88])
def test_native_reply_is_resolved_before_direct_media_upload(monkeypatch, tmp_path, route, resolved):
    async def run():
        premium = route == 'premium'
        path = tmp_path / 'photo.jpg'
        path.write_bytes(b'synthetic-photo')
        bot = SimpleNamespace(me=SimpleNamespace(id=777, username='synthetic_bot'))
        acc = SimpleNamespace()
        request = SimpleNamespace(id=1, chat=SimpleNamespace(id=123))
        anchor = SimpleNamespace(id=55, text='Source', chat=request.chat)
        msg = SimpleNamespace(id=255, photo=SimpleNamespace(file_size=10),
                              document=None, caption=None, caption_entities=[], reply_markup=None)
        worker = SimpleNamespace(resolve_bot_reply_message_id=AsyncMock(return_value=resolved),
                                 session_user_id=123)
        uploader = SimpleNamespace(send_photo=AsyncMock(return_value=SimpleNamespace(id=99)))
        monkeypatch.setattr(save, '_ensure_media_route_clients', AsyncMock(return_value=(bot, acc, 777, 123)))
        monkeypatch.setattr(save, 'get_engine', AsyncMock(return_value=SimpleNamespace(
            download=AsyncMock(return_value=str(path)))))
        monkeypatch.setattr(save, '_prepare_local_upload', AsyncMock(return_value=('Photo', {}, None)))
        monkeypatch.setattr(save, '_download_override_for', lambda _: True)
        monkeypatch.setattr(save, '_cached_client_me', lambda _: bot.me)
        monkeypatch.setattr(premium_logic, 'check_user_premium', AsyncMock(return_value=premium))
        monkeypatch.setattr(premium_logic, 'get_user_upload_setting', lambda _: 'auto')
        monkeypatch.setattr(premium_logic, 'should_split', lambda *args: False)
        monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={'session': 'synthetic'}))
        monkeypatch.setattr(worker_registry, 'get_or_create', AsyncMock(return_value=worker))
        if route == 'pool_fallback':
            from core.premium_relay import relay_uploader
            from core.session_manager import session_manager
            monkeypatch.setattr(save, '_download_override_for', lambda _: False)
            monkeypatch.setattr(save, '_GOVERNANCE_AVAILABLE', False)
            monkeypatch.setattr(session_manager, '_initialized', False)
            monkeypatch.setattr(relay_uploader, 'pool', SimpleNamespace(
                count=1, get_available=AsyncMock(return_value=(0, 'pool-synthetic'))))
            monkeypatch.setattr(worker_registry, 'get_or_create', AsyncMock(side_effect=[
                SimpleNamespace(session_user_id=999), worker]))
        if premium:
            monkeypatch.setattr(save, 'MAX_TELEGRAM_FILE_SIZE', 1)

        async def deliver(**kwargs):
            if route == 'pool_fallback' and kwargs['is_pool_session']:
                raise RuntimeError('synthetic pool upload failure')
            assert not kwargs['is_pool_session']
            return await kwargs['send_fn'](uploader)

        monkeypatch.setattr(save, '_enqueue_media_delivery', deliver)
        result = await save.download_and_send_media.__wrapped__(
            bot, acc, request, msg, 'Photo', str(tmp_path),
            SimpleNamespace(check_cancelled=AsyncMock(return_value=False)),
            session_string='synthetic', reply_target_message=anchor)
        assert result is True
        worker.resolve_bot_reply_message_id.assert_awaited_once_with(anchor)
        sent_kwargs = uploader.send_photo.await_args.kwargs
        assert get_reply_message_id(sent_kwargs) == resolved
        assert 'reply_to_message_id' not in sent_kwargs
        if resolved is None:
            assert 'reply_parameters' not in sent_kwargs
    asyncio.run(run())


@pytest.mark.parametrize('candidate_kind', ['old', 'missing_date', 'wrong_sender', 'valid'])
def test_reply_resolver_rejects_old_same_text_at_bot_side_id(candidate_kind):
    async def run():
        now = datetime(2026, 11, 7, 10, 0, tzinfo=timezone.utc)
        anchor = SimpleNamespace(id=55, text='📡 Manba: Course', date=now)
        candidate = SimpleNamespace(id=55, text=anchor.text, date=now,
                                    from_user=SimpleNamespace(id=777), empty=False)
        if candidate_kind == 'old':
            candidate.date = now - timedelta(minutes=5)
        elif candidate_kind == 'missing_date':
            candidate.date = None
        elif candidate_kind == 'wrong_sender':
            candidate.from_user.id = 123
        correct = SimpleNamespace(id=88, text=anchor.text, date=now,
                                  from_user=SimpleNamespace(id=777), empty=False)

        async def history(*args, **kwargs):
            yield candidate
            yield correct

        worker = UserUploadWorker('synthetic', 123, bot_id=777)
        worker._client = SimpleNamespace(get_messages=AsyncMock(return_value=candidate),
                                         get_chat_history=history)
        result = await worker.resolve_bot_reply_message_id(anchor)
        assert result == (55 if candidate_kind == 'valid' else 88)
        worker._client.get_messages.reset_mock()
        assert await worker.resolve_bot_reply_message_id(anchor) == result
        worker._client.get_messages.assert_not_awaited()
        # A later request to the same channel must not reuse this mapping.
        new_anchor = SimpleNamespace(id=56, text=anchor.text, date=now + timedelta(minutes=2))
        assert await worker.resolve_bot_reply_message_id(new_anchor) is None
        await worker._disconnect()
        assert not worker._reply_anchor_cache
    asyncio.run(run())


def test_reply_resolver_omits_reply_when_only_old_matches_exist():
    async def run():
        now = datetime(2026, 11, 7, tzinfo=timezone.utc)
        anchor = SimpleNamespace(id=55, text='Same source', date=now)
        old = SimpleNamespace(id=55, text=anchor.text, date=now - timedelta(minutes=10),
                              from_user=SimpleNamespace(id=777))

        async def history(*args, **kwargs):
            yield old

        worker = UserUploadWorker('synthetic', 123, bot_id=777)
        worker._client = SimpleNamespace(get_messages=AsyncMock(return_value=old),
                                         get_chat_history=history)
        assert await worker.resolve_bot_reply_message_id(anchor) is None
    asyncio.run(run())


def test_reply_resolver_requires_anchor_date():
    async def run():
        worker = UserUploadWorker('synthetic', 123, bot_id=777)
        worker._client = SimpleNamespace(get_messages=AsyncMock())
        assert await worker.resolve_bot_reply_message_id(
            SimpleNamespace(id=55, text='Repeated source header')) is None
        worker._client.get_messages.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('kwargs,expected', [
    ({'reply_parameters': ReplyParameters(message_id=55)}, 55),
    ({'reply_parameters': {'message_id': 55}}, 55),
    ({'reply_to_message_id': 55}, 55),
    ({'reply_parameters': ReplyParameters(message_id=88), 'reply_to_message_id': 55}, 88),
    ({'reply_parameters': {'message_id': 'bad'}}, None),
    ({}, None),
])
def test_native_and_legacy_reply_id_extraction(kwargs, expected):
    assert get_reply_message_id(kwargs) == expected
