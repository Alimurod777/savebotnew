from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from wzgram.errors import ChannelInvalid, ChannelPrivate, FloodWait, PeerIdInvalid

from TechVJ import save
from core.topic_extractor import TopicExtractionError, TopicExtractor, TopicExtractorConfig


@pytest.mark.parametrize('error_type', [ChannelInvalid, ChannelPrivate, PeerIdInvalid])
@pytest.mark.parametrize('override_account', [None, 456])
def test_topic_source_access_failure_stops_before_extraction(monkeypatch, error_type, override_account):
    async def run():
        pipeline = NS(get_temp_dir=AsyncMock(return_value='/tmp/topic-test'))
        acc = NS(resolve_peer=AsyncMock(side_effect=error_type()), get_chat=AsyncMock())

        @asynccontextmanager
        async def open_pipeline(*args, **kwargs):
            yield pipeline

        @asynccontextmanager
        async def open_session(*args, **kwargs):
            assert args == ('own-session', 123)
            yield acc

        bot = NS(send_message=AsyncMock(return_value=NS(id=10)), edit_message_text=AsyncMock())
        monkeypatch.setattr(save, 'StopSafePipeline', open_pipeline)
        monkeypatch.setattr(save, '_open_download_session', open_session)
        monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={
            'logged_in': True, 'session': 'own-session'}))
        monkeypatch.setattr(save, '_download_account_id_for', lambda uid: override_account)
        source_header = AsyncMock()
        monkeypatch.setattr(save, '_send_source_context_message', source_header)
        await save.process_topic_posts.__wrapped__(
            bot, NS(id=1), NS(topic_id=21, channel_id=-100999, post_ids=[21, 80],
                             topic_range_anchor=(21, 80)),
            context=NS(user_id=123, task_id='synthetic', source_chat_id=-100999))
        text = bot.edit_message_text.await_args.args[2]
        assert "Manba guruhiga kirish imkoni yo'q" in text
        assert 'Guruh ID: -100999' in text and '/userdata' in text
        assert f'Yuklab olish akkaunti: {override_account or 123}' in text
        assert error_type.__name__ in text
        assert 'chat-history scan disabled' not in text
        acc.get_chat.assert_not_awaited()
        source_header.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('error_type', [ChannelInvalid, ChannelPrivate, PeerIdInvalid])
@pytest.mark.parametrize('method', ['search', 'replies'])
def test_extractor_preserves_source_access_error_without_other_api_attempts(monkeypatch, error_type, method):
    async def run():
        extractor = TopicExtractor(NS(), TopicExtractorConfig(chat_id=-100999, topic_id=21, use_cache=False))
        if method == 'replies':
            extractor._raw_search_supported = False
        search = AsyncMock(side_effect=error_type())
        replies = AsyncMock(side_effect=error_type())
        monkeypatch.setattr(extractor, '_fetch_search_page', search)
        monkeypatch.setattr(extractor, '_fetch_replies_page', replies)
        with pytest.raises(TopicExtractionError) as caught:
            await extractor.extract_between(21, 80)
        assert 'Cannot access source chat -100999' in str(caught.value)
        assert error_type.__name__ in str(caught.value)
        assert 'chat-history scan disabled' not in str(caught.value)
        assert search.await_count == (method == 'search')
        assert replies.await_count == (method == 'replies')
    asyncio.run(run())


@pytest.mark.parametrize('method', ['search', 'replies'])
@pytest.mark.parametrize('premium', [False, True])
def test_topic_flood_retries_same_api_and_offset(monkeypatch, method, premium):
    async def run():
        extractor = TopicExtractor(NS(), TopicExtractorConfig(chat_id=-100999, topic_id=21, use_cache=False))
        if method == 'replies':
            extractor._raw_search_supported = False
        error = RuntimeError('FLOOD_PREMIUM_WAIT_7') if premium else FloodWait(7)
        page = [NS(id=80)]
        fetch = AsyncMock(side_effect=[error, page])
        other = AsyncMock(side_effect=AssertionError('Flood must not rotate API'))
        monkeypatch.setattr(extractor, '_fetch_search_page', fetch if method == 'search' else other)
        monkeypatch.setattr(extractor, '_fetch_replies_page', fetch if method == 'replies' else other)
        sleep = AsyncMock()
        monkeypatch.setattr('core.topic_extractor.asyncio.sleep', sleep)
        assert await extractor._fetch_page(offset_id=81, search_offset=100) is page
        assert fetch.await_count == 2
        assert fetch.await_args_list[0] == fetch.await_args_list[1]
        sleep.assert_awaited_once_with(12)
        other.assert_not_awaited()
        assert getattr(extractor, f'_raw_{method}_supported') is not False
    asyncio.run(run())


def test_unsupported_search_still_uses_replies(monkeypatch):
    async def run():
        extractor = TopicExtractor(NS(), TopicExtractorConfig(chat_id=-100999, topic_id=21, use_cache=False))
        monkeypatch.setattr(extractor, '_fetch_search_page', AsyncMock(side_effect=TypeError('unsupported API')))
        page = [NS(id=80)]
        replies = AsyncMock(return_value=page)
        monkeypatch.setattr(extractor, '_fetch_replies_page', replies)
        assert await extractor._fetch_page(offset_id=81) is page
        assert extractor._raw_search_supported is False
        replies.assert_awaited_once_with(81)
    asyncio.run(run())
