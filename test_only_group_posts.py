from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from wzgram.enums import ChatType
from wzgram.errors import FloodWait
from wzgram.types import MessageOriginChannel

from TechVJ import lang, save
from TechVJ.user_queue import UserQueueManager
from core.group_post_filter import GroupPostFilter

GROUP = -100123456
CHANNEL = -100654321
OTHER = -100999999


def message(post_id=1, sender=GROUP, automatic=False, origin=None, **kwargs):
    return NS(id=post_id, chat=NS(id=GROUP), sender_chat=NS(id=sender) if sender else None,
              automatic_forward=automatic, forward_origin=NS(chat=NS(id=origin)) if origin else None,
              empty=False, media_group_id=None, text='Original 😀', **kwargs)


@pytest.mark.parametrize('sender,automatic,origin,expected', [
    (GROUP, False, None, True),
    (CHANNEL, True, CHANNEL, True),
    (None, True, CHANNEL, True),
    (CHANNEL, True, None, True),
    (None, False, None, False),
    (None, False, CHANNEL, False),  # A member manually forwarding the main channel.
    (CHANNEL, False, CHANNEL, False),
    (OTHER, False, None, False),
    (OTHER, True, OTHER, False),
    (GROUP, True, OTHER, False),
    (CHANNEL, True, OTHER, False),
])
def test_group_identity_and_automatic_origin_are_required(sender, automatic, origin, expected):
    assert GroupPostFilter(GROUP, CHANNEL).accepts(message(sender=sender, automatic=automatic, origin=origin)) is expected


def test_native_wzgram_origin_and_fail_closed_on_missing_link():
    msg = message(sender=None, automatic=True)
    msg.forward_origin = MessageOriginChannel(date=None, chat=NS(id=CHANNEL), message_id=55)
    assert GroupPostFilter(GROUP, CHANNEL).accepts(msg)
    assert not GroupPostFilter(GROUP).accepts(msg)
    assert GroupPostFilter(GROUP).accepts(message())
    msg.chat.id = OTHER
    assert not GroupPostFilter(GROUP, CHANNEL).accepts(msg)
    assert not GroupPostFilter(GROUP, CHANNEL).accepts(None)


@pytest.fixture
def mock_pipeline(monkeypatch, tmp_path):
    monkeypatch.setattr(lang, 'LANG_DIR', str(tmp_path))
    lang.set_user_lang(123, 'ru')
    pipeline = NS(check_cancelled=AsyncMock(return_value=False), get_temp_dir=AsyncMock(return_value='/tmp/mock-only'))
    group = NS(id=GROUP, type=ChatType.SUPERGROUP, title='Original Group', linked_chat_id=CHANNEL)
    acc = NS(get_chat=AsyncMock(return_value=group), get_messages=AsyncMock(), send_message=AsyncMock())

    @asynccontextmanager
    async def open_pipeline(*args, **kwargs):
        yield pipeline

    @asynccontextmanager
    async def open_session(*args, **kwargs):
        yield acc

    async def send(chat_id, text, **kwargs):
        return NS(id=40, chat=NS(id=chat_id), text=text)

    bot = NS(send_message=AsyncMock(side_effect=send), edit_message_text=AsyncMock())
    monkeypatch.setattr(save, 'StopSafePipeline', open_pipeline)
    monkeypatch.setattr(save, '_open_download_session', open_session)
    monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={'logged_in': True, 'session': 'synthetic'}))
    monkeypatch.setattr(save, 'ping_activity', AsyncMock())
    monkeypatch.setattr(save.asyncio, 'sleep', AsyncMock())
    monkeypatch.setattr(save, 'process_single_post', AsyncMock(return_value=True))
    monkeypatch.setattr(save, 'process_album_messages', AsyncMock(return_value=True))
    return NS(bot=bot, acc=acc, group=group, pipeline=pipeline, request=NS(id=1, chat=NS(id=123), from_user=NS(id=123)))


async def run_filter(env, parsed):
    await save.process_only_group_posts(env.bot, env.request, parsed)


def test_only_group_members_are_filtered_before_existing_senders(mock_pipeline):
    env = mock_pipeline
    messages = [message(1, None), message(2, CHANNEL, True, CHANNEL),
                message(3, GROUP), message(4, None, False, CHANNEL), message(5, OTHER)]
    env.acc.get_messages.return_value = list(reversed(messages))
    parsed = save.ParsedURL(GROUP, [1, 2, 3, 4, 5, 3], only_group_posts=True)
    asyncio.run(run_filter(env, parsed))
    delivered = [call.kwargs['source_message'] for call in save.process_single_post.await_args_list]
    assert delivered == [messages[1], messages[2]]
    assert all(msg.text == 'Original 😀' for msg in messages)
    final = env.bot.edit_message_text.await_args.args[2]
    assert 'Выбрано сообщений: 2' in final and 'Отфильтровано: 3' in final
    assert 'Ошибок отправки: 0' in final
    assert save.process_album_messages.await_count == 0
    env.acc.send_message.assert_not_awaited()


@pytest.mark.parametrize('chat_type', [ChatType.CHANNEL, ChatType.PRIVATE, ChatType.BOT])
def test_non_groups_are_rejected_before_fetching_messages(mock_pipeline, chat_type):
    env = mock_pipeline
    env.group.type = chat_type
    asyncio.run(run_filter(env, save.ParsedURL('channel_name', [1], only_group_posts=True)))
    env.acc.get_messages.assert_not_awaited()
    save.process_single_post.assert_not_awaited()
    assert env.bot.edit_message_text.await_args.args[2] == lang.STRINGS['ru']['only_group_required']


def test_album_is_filtered_without_fetching_unrequested_neighbours(mock_pipeline, monkeypatch):
    env = mock_pipeline
    posts = [message(i) for i in range(1, 5)]
    for post in posts:
        post.media_group_id = 'album'
    posts[1].sender_chat = None
    env.acc.get_messages.return_value = posts
    monkeypatch.setattr(save, 'get_message_type', lambda _: 'Photo')
    asyncio.run(run_filter(env, save.ParsedURL(GROUP, [1, 2, 3], only_group_posts=True)))
    album = save.process_album_messages.await_args.args[3]
    assert [post.id for post in album] == [1, 3]
    assert posts[3] not in album  # Unrequested neighbour is never expanded into the request.
    save.process_single_post.assert_not_awaited()
    assert env.acc.get_messages.await_args.args == (GROUP, [1, 2, 3])


def test_flood_retries_only_current_selected_item(mock_pipeline):
    env = mock_pipeline
    env.acc.get_messages.return_value = [message(1), message(2)]
    save.process_single_post.side_effect = [True, FloodWait(3), True]
    asyncio.run(run_filter(env, save.ParsedURL(GROUP, [1, 2], only_group_posts=True)))
    assert [call.kwargs['source_message'].id for call in save.process_single_post.await_args_list] == [1, 2, 2]
    assert sum(c.args[0] for c in save.asyncio.sleep.await_args_list) == 4
    assert 'Отправлено постов/альбомов: 2' in env.bot.edit_message_text.await_args.args[2]


def test_cancel_during_flood_does_not_send_next_item(mock_pipeline):
    env = mock_pipeline
    env.acc.get_messages.return_value = [message(1), message(2)]
    save.process_single_post.side_effect = FloodWait(10)
    env.pipeline.check_cancelled.side_effect = lambda: save.process_single_post.await_count > 0
    asyncio.run(run_filter(env, save.ParsedURL(GROUP, [1, 2], only_group_posts=True)))
    assert save.process_single_post.await_count == 1
    assert 'Операция отменена' in env.bot.edit_message_text.await_args.args[2]


def test_topic_range_uses_existing_extractor_before_sender_filter(mock_pipeline, monkeypatch):
    env = mock_pipeline
    messages = [message(50), message(40, None), message(30, CHANNEL, True, CHANNEL)]
    extractor = NS(extract_between=AsyncMock(return_value=messages), _is_topic_message=lambda _: True)
    monkeypatch.setattr(save, 'TopicExtractor', lambda *args: extractor)
    monkeypatch.setattr(save, '_TOPIC_EXTRACTOR_AVAILABLE', True)
    parsed = save.ParsedURL(GROUP, [50, 30], url_type='topic', topic_id=7, topic_range_anchor=(50, 30), only_group_posts=True)
    asyncio.run(run_filter(env, parsed))
    extractor.extract_between.assert_awaited_once_with(50, 30)
    assert [call.kwargs['source_message'].id for call in save.process_single_post.await_args_list] == [50, 30]
    env.acc.get_messages.assert_not_awaited()


def test_topic_single_message_cannot_escape_to_other_topic(mock_pipeline, monkeypatch):
    env = mock_pipeline
    post = message(5)
    post.message_thread_id = 999
    env.acc.get_messages.return_value = [post]
    asyncio.run(run_filter(env, save.ParsedURL(GROUP, [5], url_type='topic', topic_id=7, only_group_posts=True)))
    save.process_single_post.assert_not_awaited()
    assert 'Отфильтровано: 1' in env.bot.edit_message_text.await_args.args[2]


def test_command_is_explicit_and_normal_requests_do_not_inherit_it(monkeypatch, tmp_path):
    async def run():
        monkeypatch.setattr(lang, 'LANG_DIR', str(tmp_path))
        monkeypatch.setattr(save, '_GOVERNANCE_AVAILABLE', False)
        monkeypatch.setattr(save.async_db, 'is_banned', AsyncMock(return_value=False))
        monkeypatch.setattr(save, '_validate_restricted_channel_request', AsyncMock(return_value=(True, '')))
        checker = AsyncMock(return_value=(False, 1, 'queued'))
        monkeypatch.setattr(save, 'check_and_queue', checker)
        user = NS(id=123)
        request = NS(text='/only https://t.me/c/123456/10-20', id=7, from_user=user, chat=NS(id=123), reply=AsyncMock())
        await save.only_command(NS(), request)
        assert checker.await_args.kwargs['parsed_data'].only_group_posts
        normal = NS(text='https://t.me/c/123456/10-20', id=8, from_user=user, chat=request.chat, reply=AsyncMock())
        await save.save(NS(), normal)
        assert not checker.await_args.kwargs['parsed_data'].only_group_posts
        checker.reset_mock()
        request.text = '/only'
        await save.only_command(NS(), request)
        checker.assert_not_awaited()
        assert '/only <group_message_link>' in request.reply.await_args.args[0]
    asyncio.run(run())


def test_only_marker_survives_queue_and_priority_dispatch(monkeypatch):
    async def run():
        queue = UserQueueManager()
        normal = save.ParsedURL(GROUP, [1])
        only = save.ParsedURL(GROUP, [2], only_group_posts=True)
        assert await queue.enqueue(123, 'normal', normal, 1, 123) == 0
        assert await queue.enqueue(123, 'only', only, 2, 123) == 1
        item = await queue.complete(123)
        assert item.parsed_data.only_group_posts
        only_sender = AsyncMock()
        normal_sender = AsyncMock()
        monkeypatch.setattr(save, '_GOVERNANCE_AVAILABLE', False)
        monkeypatch.setattr(save, 'process_only_group_posts', only_sender)
        ctx = save._task_context_from_queue(item, item.parsed_data)
        await save._dispatch_with_priority(NS(), NS(), item.parsed_data, normal_sender, context=ctx)
        only_sender.assert_awaited_once()
        normal_sender.assert_not_awaited()
    asyncio.run(run())


def test_album_remains_grouped_across_fetch_batches(mock_pipeline, monkeypatch):
    env = mock_pipeline
    posts = [message(i, None) for i in range(1, 100)] + [message(100), message(101)]
    posts[-2].media_group_id = posts[-1].media_group_id = 'album'
    env.acc.get_messages.side_effect = [posts[:100], posts[100:]]
    monkeypatch.setattr(save, 'get_message_type', lambda _: 'Photo')
    asyncio.run(run_filter(env, save.ParsedURL(GROUP, list(range(1, 102)), only_group_posts=True)))
    assert [m.id for m in save.process_album_messages.await_args.args[3]] == [100, 101]
    assert env.acc.get_messages.await_count == 2


def test_contentless_service_is_not_counted_as_delivered(mock_pipeline):
    env = mock_pipeline
    env.acc.get_messages.return_value = [message(1)]
    save.process_single_post.return_value = None
    asyncio.run(run_filter(env, save.ParsedURL(GROUP, [1], only_group_posts=True)))
    final = env.bot.edit_message_text.await_args.args[2]
    assert 'Отправлено постов/альбомов: 0' in final
    assert 'Не найдено/служебных: 1' in final
    assert 'Ошибок отправки: 0' in final


def test_prefetched_message_reuses_regular_text_sender_without_refetch(monkeypatch):
    from wzgram.types import Message

    async def run():
        text = Message(id=1, chat=NS(id=GROUP), text='Original body 😀')
        acc = NS(get_messages=AsyncMock())
        bot = NS()
        pipeline = NS(check_cancelled=AsyncMock(return_value=False))
        sender = AsyncMock(return_value=True)
        monkeypatch.setattr(save, 'send_text_message', sender)
        result = await save.process_single_post.__wrapped__(
            bot, acc, NS(chat=NS(id=123)), GROUP, 1, '/tmp/mock-only', pipeline,
            target_user_id=123, source_message=text)
        assert result is True
        assert sender.await_args.args[2] is text
        acc.get_messages.assert_not_awaited()
    asyncio.run(run())


def test_delivery_failure_does_not_claim_complete_success(mock_pipeline):
    env = mock_pipeline
    env.acc.get_messages.return_value = [message(1)]
    save.process_single_post.return_value = False
    asyncio.run(run_filter(env, save.ParsedURL(GROUP, [1], only_group_posts=True)))
    final = env.bot.edit_message_text.await_args.args[2]
    assert final.startswith('⚠️ /only')
    assert 'Ошибок отправки: 1' in final


def test_only_respects_maintenance_before_queueing(monkeypatch, tmp_path):
    async def run():
        monkeypatch.setattr(lang, 'LANG_DIR', str(tmp_path))
        monkeypatch.setattr(save, '_GOVERNANCE_AVAILABLE', True)
        monkeypatch.setattr(save, '_is_maintenance', lambda: True)
        monkeypatch.setattr(save, '_validate_restricted_channel_request', AsyncMock(return_value=(True, '')))
        queue = AsyncMock()
        monkeypatch.setattr(save, 'check_and_queue', queue)
        request = NS(id=1, from_user=NS(id=123), chat=NS(id=123), reply=AsyncMock())
        await save._save_request(NS(), request, 'https://t.me/c/123456/1', only_group_posts=True)
        assert request.reply.await_args.args[0] == lang.STRINGS['en']['maintenance_notice']
        queue.assert_not_awaited()
    asyncio.run(run())


def test_missing_session_never_falls_back_to_unfiltered_bot_copy(mock_pipeline, monkeypatch):
    env = mock_pipeline
    monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value=None))
    asyncio.run(run_filter(env, save.ParsedURL(GROUP, [1], only_group_posts=True)))
    env.acc.get_chat.assert_not_awaited()
    env.acc.get_messages.assert_not_awaited()
    assert env.bot.send_message.await_args.args[1] == lang.STRINGS['ru']['need_login']
