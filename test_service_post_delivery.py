from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest
from wzgram.enums import MessageMediaType, MessageServiceType
from wzgram.types import Message, Photo

from TechVJ import save
from core.downloader.resume import ResumableDownloader
from core.user_upload_worker import make_send_fn


@pytest.mark.parametrize('photo_delivered', [True, False])
def test_private_range_chat_photo_counts_only_after_upload(monkeypatch, photo_delivered):
    async def run():
        photo = Photo(file_id='synthetic', file_unique_id='unique', width=640,
                      height=640, file_size=100, date=datetime(2026, 1, 1))
        posts = {
            105: Message(id=105, video_note=NS(file_id='note')),
            106: Message(id=106, text='https://t.me/example'),
            107: Message(id=107, service=MessageServiceType.NEW_CHAT_PHOTO,
                         new_chat_photo=photo),
        }
        pipeline = NS(get_temp_dir=AsyncMock(return_value='/tmp/mock-service'),
                      check_cancelled=AsyncMock(return_value=False))
        acc = NS(get_chat=AsyncMock(return_value=None),
                 get_messages=AsyncMock(side_effect=lambda chat, post_id: posts[post_id]))

        @asynccontextmanager
        async def open_pipeline(*args, **kwargs):
            yield pipeline

        @asynccontextmanager
        async def open_session(*args, **kwargs):
            yield acc

        bot = NS(send_message=AsyncMock(return_value=NS(id=40)),
                 edit_message_text=AsyncMock(), pin_chat_message=AsyncMock(),
                 unpin_chat_message=AsyncMock(), send_photo=AsyncMock())
        uploader = NS(send_photo=AsyncMock(return_value=NS(id=45)))

        async def media(client, download_acc, request, msg, kind, *args, **kwargs):
            if kind == 'VideoNote':
                return True
            assert client is bot and download_acc is acc and kind == 'Photo'
            assert msg is not posts[107] and msg.id == 107
            assert msg.photo is photo and msg.media == MessageMediaType.PHOTO
            assert msg.service is None
            # Use the actual downloader's extraction and worker send dispatch.
            downloader = object.__new__(ResumableDownloader)
            assert downloader._get_file_info(msg) == ('photo_107.jpg', 100, 'synthetic')
            if not photo_delivered:
                return False
            sent = await make_send_fn(777, kind, '/tmp/mock-photo.jpg', {})(uploader)
            return bool(sent)

        monkeypatch.setattr(save, 'StopSafePipeline', open_pipeline)
        monkeypatch.setattr(save, '_open_download_session', open_session)
        monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={
            'logged_in': True, 'session': 'synthetic'}))
        monkeypatch.setattr(save, '_resolve_peer_safe', AsyncMock())
        monkeypatch.setattr(save, '_send_source_context_message', AsyncMock(return_value=NS(
            id=41, chat=NS(id=123))))
        monkeypatch.setattr(save, 'ping_activity', AsyncMock())
        monkeypatch.setattr(save, 'detect_album_boundary', AsyncMock(return_value=None))
        monkeypatch.setattr(save, '_notify_realtime_post_failure', AsyncMock())
        monkeypatch.setattr(save, '_THROTTLE_AVAILABLE', False)
        monkeypatch.setattr(save.asyncio, 'sleep', AsyncMock())
        monkeypatch.setattr(save, 'download_and_send_media', media)
        await save.process_private_posts.__wrapped__(
            bot, NS(chat=NS(id=123), id=1),
            NS(channel_id=-100999, post_ids=[105, 106, 107]),
            context=NS(user_id=123))
        final = bot.edit_message_text.await_args.args[2]
        assert f'Yuklandi: {3 if photo_delivered else 2}/3' in final
        assert ('Jo\'natilmagan ID' in final) is (not photo_delivered)
        if not photo_delivered:
            assert '107' in final
        assert posts[107].photo is None
        assert posts[107].service == MessageServiceType.NEW_CHAT_PHOTO
        bot.send_photo.assert_not_awaited()
        assert uploader.send_photo.await_count == int(photo_delivered)
        assert any(c.kwargs.get('text') == 'https://t.me/example'
                   for c in bot.send_message.await_args_list)
    asyncio.run(run())


@pytest.mark.parametrize('chunks,result', [([], None), ([('', [])], None),
                                         ([('text', [])], None),
                                         ([('text', [])], NS(id=8))])
def test_text_success_requires_delivered_chunk(monkeypatch, chunks, result):
    async def run():
        import core.text_renderer as renderer
        monkeypatch.setattr(renderer, 'extract_to_renderer', lambda *args: NS(
            render_chunks=lambda limit: chunks))
        bot = NS(send_message=AsyncMock(return_value=result))
        sent = await save.send_text_message(bot, NS(chat=NS(id=123), id=1),
                                            Message(id=107, text='text'))
        assert sent is bool(result and chunks)
    asyncio.run(run())


def test_empty_service_message_never_counts_as_sent(monkeypatch):
    async def run():
        bot = NS(send_message=AsyncMock())
        acc = NS()
        msg = Message(id=107, service=MessageServiceType.DELETE_CHAT_PHOTO)
        request = NS(chat=NS(id=123), id=1)
        assert await save.send_text_message(bot, request, msg) is False
        monkeypatch.setattr(save, '_ensure_media_route_clients', AsyncMock(
            return_value=(bot, acc, 777, 123)))
        engine = AsyncMock()
        monkeypatch.setattr(save, 'get_engine', engine)
        assert await save.download_and_send_media.__wrapped__(
            bot, acc, request, msg, 'Text', None,
            NS(check_cancelled=AsyncMock(return_value=False))) is False
        bot.send_message.assert_not_awaited()
        engine.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('with_text,with_deleted', [(False, False), (True, False), (False, True)])
def test_pin_service_events_are_skipped_without_delivery_failure(monkeypatch, with_text, with_deleted):
    async def run():
        posts = {
            15: Message(id=15, service=MessageServiceType.PINNED_MESSAGE),
            17: Message(id=17, service=MessageServiceType.PINNED_MESSAGE),
        }
        if with_text:
            posts[16] = Message(id=16, text='Actual post')
        if with_deleted:
            posts[16] = Message(id=16, empty=True)
        pipeline = NS(get_temp_dir=AsyncMock(return_value='/tmp/mock-service'),
                      check_cancelled=AsyncMock(return_value=False))
        acc = NS(get_chat=AsyncMock(return_value=None),
                 get_messages=AsyncMock(side_effect=lambda chat, post_id: posts[post_id]))

        @asynccontextmanager
        async def open_pipeline(*args, **kwargs):
            yield pipeline

        @asynccontextmanager
        async def open_session(*args, **kwargs):
            yield acc

        bot = NS(send_message=AsyncMock(return_value=NS(id=40)),
                 edit_message_text=AsyncMock(), pin_chat_message=AsyncMock(),
                 unpin_chat_message=AsyncMock())
        owner = AsyncMock()
        monkeypatch.setattr(save, '_notify_owner_channel_post_failure', owner)
        monkeypatch.setattr(save, 'StopSafePipeline', open_pipeline)
        monkeypatch.setattr(save, '_open_download_session', open_session)
        monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={
            'logged_in': True, 'session': 'synthetic'}))
        monkeypatch.setattr(save, '_resolve_peer_safe', AsyncMock())
        monkeypatch.setattr(save, '_send_source_context_message', AsyncMock(return_value=NS(
            id=41, chat=NS(id=123))))
        monkeypatch.setattr(save, 'ping_activity', AsyncMock())
        monkeypatch.setattr(save, 'detect_album_boundary', AsyncMock(return_value=None))
        monkeypatch.setattr(save, '_THROTTLE_AVAILABLE', False)
        monkeypatch.setattr(save.asyncio, 'sleep', AsyncMock())
        await save.process_private_posts.__wrapped__(
            bot, NS(chat=NS(id=123), id=1),
            NS(channel_id=-100999, post_ids=sorted(posts)), context=NS(user_id=123))
        final = bot.edit_message_text.await_args.args[2]
        assert 'Kontentsiz xizmat xabarlari: 2' in final
        assert 'Jo\'natilmagan ID' not in final and 'Xato: 2' not in final
        if with_text:
            assert 'Yuklandi: 1/3' in final
        else:
            assert 'Hech qanday xabar' in final
            assert 'Barcha xabarlar o\'chirilgan' not in final
            assert 'Xato: 0' in final
        if with_deleted:
            assert 'O\'chirilgan: 1' in final
        owner.assert_not_awaited()
        # No blank send attempts or per-post user error notifications.
        sent_texts = [c.kwargs.get('text', c.args[1] if len(c.args) > 1 else None)
                      for c in bot.send_message.await_args_list]
        assert len(sent_texts) == 1 + int(with_text)
        assert all(sent_texts)
    asyncio.run(run())


def test_actual_text_send_failure_still_reports_owner(monkeypatch):
    async def run():
        source = Message(id=16, text='Actual post')
        bot = NS(send_message=AsyncMock(return_value=None))
        acc = NS(get_messages=AsyncMock(return_value=source))
        owner = AsyncMock()
        monkeypatch.setattr(save, '_resolve_peer_safe', AsyncMock())
        monkeypatch.setattr(save, 'ping_activity', AsyncMock())
        monkeypatch.setattr(save, '_notify_owner_channel_post_failure', owner)
        result = await save.process_single_post.__wrapped__(
            bot, acc, NS(chat=NS(id=123), id=1), -100999, 16, None,
            NS(check_cancelled=AsyncMock(return_value=False)))
        assert result is False
        owner.assert_awaited_once()
        assert owner.await_args.kwargs['stage'] == 'send_text'
    asyncio.run(run())


def test_owner_diagnostics_distinguish_service_from_plain_text():
    lines = save._owner_diag_media_lines(Message(id=15, service=MessageServiceType.PINNED_MESSAGE))
    assert 'Post type: Service' in lines
    assert 'Service type: pinned_message' in lines
    assert 'Post type: Text' in save._owner_diag_media_lines(Message(id=16, text='Actual post'))


@pytest.mark.parametrize('with_text', [False, True])
def test_public_user_session_counts_service_skips_separately(monkeypatch, with_text):
    async def run():
        posts = {15: Message(id=15, service=MessageServiceType.PINNED_MESSAGE)}
        if with_text:
            posts[16] = Message(id=16, text='Actual post')
        pipeline = NS(check_cancelled=AsyncMock(return_value=False))
        acc = NS(get_chat=AsyncMock(return_value=None),
                 get_messages=AsyncMock(side_effect=lambda chat, post_id: posts[post_id]))

        @asynccontextmanager
        async def open_pipeline(*args, **kwargs):
            yield pipeline

        @asynccontextmanager
        async def open_session(*args, **kwargs):
            yield acc

        bot = NS(get_chat=AsyncMock(return_value=None),
                 send_message=AsyncMock(return_value=NS(id=40)), edit_message_text=AsyncMock())
        batch = NS(check_pause=lambda idx: 0, should_stop=lambda: False,
                   record_error=Mock(), record_success=lambda: None, finish=lambda: None)
        monkeypatch.setattr(save, 'StopSafePipeline', open_pipeline)
        monkeypatch.setattr(save, '_open_download_session', open_session)
        monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={
            'logged_in': True, 'session': 'synthetic'}))
        monkeypatch.setattr(save, '_resolve_peer_safe', AsyncMock())
        monkeypatch.setattr(save, '_send_source_context_message', AsyncMock(return_value=NS(
            id=41, chat=NS(id=123))))
        monkeypatch.setattr(save, 'ping_activity', AsyncMock())
        monkeypatch.setattr(save, '_THROTTLE_AVAILABLE', True)
        monkeypatch.setattr(save, 'BatchController', lambda uid: batch)
        monkeypatch.setattr(save.asyncio, 'sleep', AsyncMock())
        ctx = NS(user_id=123)
        ctx.with_client_type = lambda kind: ctx
        await save.process_public_posts.__wrapped__(
            bot, NS(chat=NS(id=123), id=1),
            NS(channel_id='synthetic_channel', post_ids=sorted(posts)), context=ctx)
        final = bot.edit_message_text.await_args.args[2]
        assert 'Service messages skipped: 1' in final
        assert ('Processed: 1/2' if with_text else 'No content to send') in final
        batch.record_error.assert_not_called()
    asyncio.run(run())
