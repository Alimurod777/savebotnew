from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from wzgram.enums import ChatType, ChatMemberStatus, ParseMode
from wzgram.errors import FloodWait

from core import chat_log
from core.chat_log import ChatLogger, ChatLogSettings, LogPolicy, validate_log_chat
from TechVJ import owner_commands, log_channel


@pytest.fixture(autouse=True)
def no_live_settings_db(monkeypatch):
    monkeypatch.setattr(chat_log, '_settings_collection', lambda: None)


def message(mid=1, text='Hello 😀', uid=123, **kwargs):
    values = dict(id=mid, text=text, caption=None, entities=[], caption_entities=[],
                  chat=SimpleNamespace(id=uid, type=ChatType.PRIVATE,
                                       first_name='Test', last_name='User', username='tester'),
                  from_user=SimpleNamespace(id=uid), outgoing=False,
                  edit_date=None, media_group_id=None, empty=False, service=None)
    values.update(kwargs)
    return SimpleNamespace(**values)


def bot():
    return SimpleNamespace(
        me=SimpleNamespace(id=777, is_bot=True),
        send_message=AsyncMock(return_value=message(uid=-10099)),
        forward_messages=AsyncMock(return_value=message(text=None, uid=-10099)),
        copy_message=AsyncMock(return_value=message(text=None)),
        get_messages=AsyncMock(return_value=message()),
        get_media_group=AsyncMock(return_value=[message(2, None), message(1, None)]),
        get_chat=AsyncMock(return_value=SimpleNamespace(id=-10099, type=ChatType.CHANNEL)),
        get_chat_member=AsyncMock(return_value=SimpleNamespace(
            status=ChatMemberStatus.ADMINISTRATOR,
            privileges=SimpleNamespace(can_post_messages=True))),
    )


def test_env_owner_precedence_remove_and_reset_survive_restart(tmp_path):
    async def run():
        path = tmp_path / 'log.json'
        settings = ChatLogSettings(path, -1001)
        assert settings.policy == LogPolicy(-1001, True)
        await settings.save(LogPolicy(-1002, True, 'owner'))
        assert ChatLogSettings(path, -1003).policy == LogPolicy(-1002, True, 'owner')
        await settings.save(LogPolicy(-1002, False, 'owner'))
        assert ChatLogSettings(path, -1003).policy == LogPolicy(-1002, False, 'owner')
        await settings.save(LogPolicy(source='owner'))
        assert ChatLogSettings(path, -1003).policy == LogPolicy(source='owner')
        await settings.reset()
        assert settings.policy == LogPolicy(-1001, True)
        assert ChatLogSettings(path, -1003).policy == LogPolicy(-1003, True)
    asyncio.run(run())


@pytest.mark.parametrize('data', ['{bad', '{"override":true}', '{"override":true,"chat_id":12,"enabled":true}'])
def test_damaged_override_never_reactivates_env(tmp_path, data):
    path = tmp_path / 'log.json'
    path.write_text(data)
    assert ChatLogSettings(path, -1001).policy == LogPolicy(source='invalid')


def test_failed_save_retains_live_policy_and_cleans_tmp(tmp_path, monkeypatch):
    async def run():
        settings = ChatLogSettings(tmp_path / 'log.json', -1001)
        before = settings.policy
        def fail(*args):
            raise OSError('disk failure')
        monkeypatch.setattr(chat_log.os, 'replace', fail)
        with pytest.raises(OSError):
            await settings.save(LogPolicy(-1002, True, 'owner'))
        assert settings.policy == before
        assert settings.revision == 0
        assert list(tmp_path.iterdir()) == []
    asyncio.run(run())


@pytest.mark.parametrize('kind,status,post,valid', [
    (ChatType.PRIVATE, ChatMemberStatus.ADMINISTRATOR, True, False),
    (ChatType.CHANNEL, ChatMemberStatus.MEMBER, True, False),
    (ChatType.CHANNEL, ChatMemberStatus.ADMINISTRATOR, False, False),
    (ChatType.CHANNEL, ChatMemberStatus.ADMINISTRATOR, True, True),
    (ChatType.SUPERGROUP, ChatMemberStatus.ADMINISTRATOR, False, True),
    (ChatType.GROUP, ChatMemberStatus.OWNER, False, True),
    (getattr(ChatType, "FORUM", ChatType.SUPERGROUP), ChatMemberStatus.ADMINISTRATOR, False, True),
])
def test_destination_validation(kind, status, post, valid):
    async def run():
        client = bot()
        client.get_chat.return_value.type = kind
        client.get_chat_member.return_value.status = status
        client.get_chat_member.return_value.privileges.can_post_messages = post
        if valid:
            assert await validate_log_chat(client, '@logs') == -10099
        else:
            with pytest.raises(ValueError):
                await validate_log_chat(client, '@logs')
    asyncio.run(run())


@pytest.fixture
def setup(tmp_path, monkeypatch):
    manager = ChatLogger(ChatLogSettings(tmp_path / 'log.json', -10099), interval=0)
    monkeypatch.setattr(chat_log, 'chat_logger', manager)
    monkeypatch.setattr(log_channel, 'chat_logger', manager)
    monkeypatch.setattr(owner_commands, 'OWNER_ID', 999)
    return manager, bot()


def test_text_is_forwarded_directly_from_chat_with_user_header(setup):
    async def run():
        manager, client = setup
        await manager.start(client)
        entities = [SimpleNamespace(type='bold', offset=0, length=5)]
        msg = message(entities=entities)
        await log_channel.log_private_message(client, msg)
        await manager.queue.join()
        client.send_message.assert_awaited_once()
        header = client.send_message.await_args
        assert 'Test User' in header.args[1] and '123' in header.args[1] and '@tester' in header.args[1]
        assert header.kwargs['parse_mode'] == ParseMode.DISABLED
        client.forward_messages.assert_awaited_once_with(-10099, 123, 1)
        assert manager.sent == 1
        await manager.stop()
    asyncio.run(run())


def test_media_uses_ordinary_native_forward_without_caption_changes(setup):
    async def run():
        manager, client = setup
        await manager.start(client)
        manager.submit(message(text=None, caption='Media 😀', caption_entities=['entity']))
        await manager.queue.join()
        client.forward_messages.assert_awaited_once_with(-10099, 123, 1)
        assert manager.sent == 1
        await manager.stop()
    asyncio.run(run())


def test_album_copied_once_in_order_with_one_user_header(setup, monkeypatch):
    async def run():
        manager, client = setup
        monkeypatch.setattr(chat_log.asyncio, 'sleep', AsyncMock())
        await manager.start(client)
        manager.submit(message(2, None, media_group_id='album'))
        manager.submit(message(1, None, media_group_id='album'))
        await manager.queue.join()
        client.forward_messages.assert_awaited_once_with(-10099, 123, [1, 2])
        client.send_message.assert_awaited_once()
        await manager.stop()
    asyncio.run(run())


def test_send_hooks_capture_bot_results_once_and_dont_touch_return_values(setup):
    async def run():
        manager, client = setup
        delivered = message(outgoing=True, from_user=SimpleNamespace(id=777))
        original = client.send_message
        original_forward = client.forward_messages
        original.side_effect = lambda target, *args, **kwargs: delivered if target == 123 else message(uid=target)
        async def nested_copy(*args, **kwargs):
            return await client.send_message(123, 'Hello 😀')
        client.copy_message = nested_copy
        manager.attach(client)
        manager.attach(client)  # Idempotent, no duplicate wrapping.
        await manager.start(client)
        result = await client.copy_message(123, 321, 8)
        assert result is delivered
        manager.submit(delivered)  # Telegram's matching update must be deduplicated.
        await manager.queue.join()
        assert manager.sent == 1
        assert original.await_count == 2  # user send + log header; content uses native forward
        header = original.await_args_list[1].args[1]
        assert 'ID: 123' in header
        original_forward.assert_awaited_once_with(-10099, 123, 1)
        assert manager.queue.empty()
        await manager.stop()
    asyncio.run(run())


def test_logging_failure_never_breaks_successful_user_send(setup):
    async def run():
        manager, client = setup
        delivered = message()
        async def send(target, *args, **kwargs):
            if target == 123:
                return delivered
            raise RuntimeError('log denied')
        client.send_message.side_effect = send
        manager.attach(client)
        await manager.start(client)
        assert await client.send_message(123, 'hello') is delivered
        await manager.queue.join()
        assert manager.failed == 1 and manager.last_error == 'RuntimeError'
        await manager.stop()
    asyncio.run(run())


def test_log_destination_and_nonprivate_updates_are_never_relogged(setup):
    async def run():
        manager, client = setup
        await manager.start(client)
        for kind in (ChatType.CHANNEL, ChatType.GROUP, ChatType.SUPERGROUP):
            manager.submit(message(chat=SimpleNamespace(id=-10099, type=kind)))
        assert manager.queue.empty()
        await manager.stop()
    asyncio.run(run())


def test_changed_policy_discards_queued_events_instead_of_redirecting_them(setup):
    async def run():
        manager, client = setup
        await manager.start(client)
        manager.submit(message())
        event = manager.queue.get_nowait()
        manager.queue.task_done()
        await manager.configure(LogPolicy(-10077, True, 'owner'))
        await manager._deliver(event)
        client.send_message.assert_not_awaited()
        assert manager.dropped == 1
        await manager.stop()
    asyncio.run(run())


def test_off_wakes_flood_wait_and_does_not_copy_after_acknowledgement(setup):
    async def run():
        manager, client = setup
        seen = asyncio.Event()
        async def flood(*args, **kwargs):
            seen.set()
            raise FloodWait(50)
        client.forward_messages.side_effect = flood
        await manager.start(client)
        manager.submit(message(text=None))
        await asyncio.wait_for(seen.wait(), 1)
        await manager.configure(LogPolicy(-10099, False, 'owner'))
        await asyncio.wait_for(manager.queue.join(), 1)
        client.forward_messages.assert_awaited_once()
        assert manager.dropped == 1
        await manager.stop()
    asyncio.run(run())


def test_flood_retries_content_without_duplicate_header(setup, monkeypatch):
    async def run():
        manager, client = setup
        client.forward_messages.side_effect = [FloodWait(0), message(text=None)]
        # Advance the monotonic clock after the first failed RPC, avoiding real waits.
        tick = [0]
        def now():
            tick[0] += 2
            return tick[0]
        monkeypatch.setattr(chat_log.time, 'monotonic', now)
        await manager.start(client)
        manager.submit(message(text=None))
        await manager.queue.join()
        assert client.forward_messages.await_count == 2
        client.send_message.assert_awaited_once()
        assert manager.sent == 1
        await manager.stop()
    asyncio.run(run())


def test_queue_is_bounded_and_reports_overflow(tmp_path):
    async def run():
        manager = ChatLogger(ChatLogSettings(tmp_path / 'log.json', -10099), queue_size=1)
        client = bot()
        await manager.start(client)
        manager.submit(message(1))
        manager.submit(message(2))
        assert manager.queue.qsize() == 1
        assert manager.dropped == 1 and manager.last_error == 'QueueFull'
        await manager.stop()
    asyncio.run(run())


def test_edits_are_copied_but_identical_updates_are_deduplicated(setup):
    async def run():
        manager, client = setup
        await manager.start(client)
        original = message()
        manager.submit(original)
        manager.submit(original)
        await log_channel.log_private_edit(client, message(text='Edited 😀'))
        await manager.queue.join()
        assert manager.sent == 2
        assert client.forward_messages.await_count == 2
        client.send_message.assert_awaited_once()  # same user's block has one header
        await manager.stop()
    asyncio.run(run())


def test_owner_controls_and_runtime_overrides_work_without_env(setup):
    async def run():
        manager, client = setup
        manager.settings.env_chat_id = 0
        async def command(text, uid=999):
            await owner_commands.cmd_logchannel(client, message(text=text, uid=uid))
        await command('/logchannel set @logs', uid=123)
        client.get_chat.assert_not_awaited()
        await command('/logchannel set @logs')
        assert manager.settings.policy == LogPolicy(-10099, True, 'owner')
        await command('/logchannel off')
        assert not manager.settings.policy.enabled
        await command('/logchannel on')
        assert manager.settings.policy.enabled
        await command('/logchannel test')
        assert any(c.args[0] == -10099 for c in client.send_message.await_args_list)
        await command('/logchannel status')
        assert 'owner' in client.send_message.await_args.args[1]
        await command('/logchannel remove')
        assert manager.settings.policy == LogPolicy(source='owner')
        await command('/logchannel reset')
        assert manager.settings.policy == LogPolicy()
    asyncio.run(run())


def test_owner_command_does_not_claim_success_when_persistence_fails(setup, monkeypatch):
    async def run():
        manager, client = setup
        def fail(*args):
            raise OSError('disk full')
        monkeypatch.setattr(chat_log.os, 'replace', fail)
        await owner_commands.cmd_logchannel(client, message(text='/logchannel off', uid=999))
        assert manager.settings.policy == LogPolicy(-10099, True)
        assert 'bajarilmadi' in client.send_message.await_args.args[1]
    asyncio.run(run())


def test_user_header_only_on_switch_or_after_long_idle_gap(setup, monkeypatch):
    async def run():
        manager, client = setup
        await manager.start(client)
        for mid in (1, 2, 3):
            manager.submit(message(mid))
        await manager.queue.join()
        assert client.send_message.await_count == 1
        assert client.forward_messages.await_count == 3
        manager.submit(message(4, uid=456))
        await manager.queue.join()
        assert client.send_message.await_count == 2
        assert 'ID: 456' in client.send_message.await_args.args[1]
        manager.submit(message(5, uid=456))
        await manager.queue.join()
        assert client.send_message.await_count == 2
        manager._last_activity -= 301
        manager.submit(message(6, uid=456))
        await manager.queue.join()
        assert client.send_message.await_count == 3
        assert 'ID: 456' in client.send_message.await_args.args[1]
        await manager.stop()
    asyncio.run(run())


@pytest.mark.parametrize('text', [
    '📥 Downloading\n\n████░ 96.3%\n⏱ ETA: 25s',
    '📤 **Uploading**\n\n`███░` **33.3%**',
    '**Downloading**\n699.1 MB',
    'Qism 2/3 - 713.0/1333.3 MB (53.5%)',
    'Qism 2/3 - 1333.3/1333.3 MB (100.0%)',
    '⏳ 164/601 | ID: `165`\n✅ 121  📁 3  ❌ 0',
    '⏳ 601 ta post yuklanmoqda...',
])
def test_bot_progress_send_and_edit_are_not_logged(setup, text):
    async def run():
        manager, client = setup
        progress = message(text=text, from_user=SimpleNamespace(id=777), outgoing=True)
        client.send_message.return_value = progress
        client.edit_message_text = AsyncMock(return_value=progress)
        original_forward = client.forward_messages
        manager.attach(client)
        await manager.start(client)
        assert await client.send_message(123, text) is progress
        assert await client.edit_message_text(123, 1, text) is progress
        await log_channel.log_private_edit(client, progress)
        await manager.queue.join()
        original_forward.assert_not_awaited()
        assert manager.sent == manager.failed == manager.dropped == 0
        await manager.stop()
    asyncio.run(run())


def test_user_progress_text_and_media_caption_still_forward(setup):
    async def run():
        manager, client = setup
        await manager.start(client)
        manager.submit(message(text='📥 Downloading\n96.3%'))
        manager.submit(message(2, text=None, caption='📤 Uploading',
                               document=SimpleNamespace(file_id='file'), outgoing=True))
        await manager.queue.join()
        assert client.forward_messages.await_count == 2
        await manager.stop()
    asyncio.run(run())


def test_queued_status_edited_to_progress_is_not_forwarded(setup):
    async def run():
        manager, client = setup
        client.get_messages.return_value = message(text='Qism 2/3 - 713.0/1333.3 MB (53.5%)')
        await manager.start(client)
        manager.submit(message(text='Preparing...', outgoing=True))
        await manager.queue.join()
        client.forward_messages.assert_not_awaited()
        client.send_message.assert_not_awaited()
        await manager.stop()
    asyncio.run(run())


def test_final_result_on_same_progress_message_is_logged(setup):
    async def run():
        manager, client = setup
        await manager.start(client)
        manager.submit(message(text='📥 Downloading', outgoing=True))
        final = message(text="⚠️ Hech qanday xabar jo'natilmadi\nSo'ralgan: 1\n❌ Xato: 0",
                        outgoing=True)
        client.get_messages.return_value = final
        manager.submit(final)
        await manager.queue.join()
        client.forward_messages.assert_awaited_once_with(-10099, 123, 1)
        await manager.stop()
    asyncio.run(run())
