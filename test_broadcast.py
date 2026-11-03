from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from wzgram.enums import MessageEntityType
from wzgram.errors import FloodWait, UserIsBlocked
from wzgram.types import MessageEntity

from core import broadcast
from core.broadcast import BroadcastContent, BroadcastManager, BroadcastBusy
from core.entity_validator import utf16_length
from database.async_db import async_db
from TechVJ import owner_commands


@pytest.fixture
def setup(monkeypatch):
    from core.role_manager import role_manager
    from core import mongo_store
    monkeypatch.setattr(broadcast, 'OWNER_ID', 999)
    monkeypatch.setattr(owner_commands, 'OWNER_ID', 999)
    monkeypatch.setattr(role_manager, 'is_banned', AsyncMock(return_value=False))
    monkeypatch.setattr(async_db, 'is_banned', AsyncMock(return_value=False))
    monkeypatch.setattr(mongo_store, 'get_mongo_store', lambda: None)
    monkeypatch.setattr(broadcast, 'calculate_backoff', lambda *args, **kwargs: 0)
    state = SimpleNamespace(rows=[], scanned=0, closed=False)
    async def rows(*args, **kwargs):
        state.scanned += 1
        try:
            for row in state.rows:
                yield row
        finally:
            state.closed = True
    monkeypatch.setattr(async_db, 'iter_registered_users', rows)
    manager = BroadcastManager(interval=0, progress_interval=0.01)
    monkeypatch.setattr(broadcast, 'broadcast_manager', manager)
    bot = SimpleNamespace(me=SimpleNamespace(is_bot=True),
                          send_message=AsyncMock(return_value=SimpleNamespace(id=700)),
                          copy_message=AsyncMock(return_value=SimpleNamespace(id=800)),
                          edit_message_text=AsyncMock(return_value=SimpleNamespace(id=700)))
    return state, manager, bot


def command(text='/broadcast Hello everyone!', uid=999, source=None, entities=None):
    return SimpleNamespace(text=text, from_user=SimpleNamespace(id=uid), chat=SimpleNamespace(id=uid),
                           reply_to_message=source, entities=entities)


def assert_totals(job):
    assert job.processed == job.successful + job.failed + job.skipped
    assert 0 <= job.processed <= job.total
    text = job.render()
    assert f'Pending: {job.total - job.processed}' in text
    assert f'Successful: {job.successful}' in text


def test_nonowner_rejected_before_source_parsing_or_enumeration(setup):
    state, manager, bot = setup
    asyncio.run(owner_commands.cmd_broadcast(bot, SimpleNamespace(from_user=SimpleNamespace(id=1))))
    assert state.scanned == 0
    assert manager.job is None
    bot.send_message.assert_not_awaited()


def test_manager_rejects_nonowner_and_user_session(setup):
    async def run():
        state, manager, bot = setup
        with pytest.raises(PermissionError):
            await manager.start(bot, 1, BroadcastContent(text='hello'))
        bot.me.is_bot = False
        with pytest.raises(PermissionError):
            await manager.start(bot, 999, BroadcastContent(text='hello'))
        assert state.scanned == 0
    asyncio.run(run())


def test_text_broadcast_uses_bot_identity_and_consistent_completion(setup):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}, {'chat_id': 2, 'logged_in': False}]
        await owner_commands.cmd_broadcast(bot, command())
        job = manager.job
        await job.task
        deliveries = [call for call in bot.send_message.await_args_list if call.args[0] != 999]
        assert [(call.args[0], call.args[1]) for call in deliveries] == [(1, 'Hello everyone!'), (2, 'Hello everyone!')]
        assert all(call.kwargs['parse_mode'] == broadcast.ParseMode.DISABLED for call in deliveries)
        assert job.status == 'Completed'
        assert (job.total, job.successful, job.failed, job.skipped) == (2, 2, 0, 0)
        assert job.finished_at >= job.started_at
        assert state.closed
        assert_totals(job)
        final = bot.edit_message_text.await_args
        assert final.args[0] == 999 and 'Completed' in final.args[2]
    asyncio.run(run())


@pytest.mark.parametrize('kind', ['text', 'photo', 'video', 'document', 'audio', 'voice', 'animation', 'sticker'])
def test_reply_broadcast_copies_supported_content_without_overrides(setup, kind):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}]
        source = SimpleNamespace(id=5, chat=SimpleNamespace(id=999), caption='original 😀 caption',
                                 **{kind: 'original text' if kind == 'text' else SimpleNamespace(file_id='cached')})
        await owner_commands.cmd_broadcast(bot, command('/broadcast', source=source))
        await manager.job.task
        bot.copy_message.assert_awaited_once_with(1, 999, 5)
        assert source.caption == 'original 😀 caption'
        assert manager.job.successful == 1
    asyncio.run(run())


def test_text_formatting_offsets_and_whitespace_are_preserved():
    raw = '/broadcast 😀 Hello  '
    prefix = utf16_length('/broadcast ')
    entity = MessageEntity(type=MessageEntityType.BOLD, offset=prefix + 3, length=5)
    content = BroadcastContent.from_command(command(raw, entities=[entity]))
    assert content.text == '😀 Hello  '
    assert content.entities[0].offset == 3
    assert content.entities[0].length == 5
    assert entity.offset == prefix + 3
    spanning = MessageEntity(type=MessageEntityType.BOLD, offset=0, length=utf16_length(raw))
    assert BroadcastContent.from_command(command(raw, entities=[spanning])).entities[0].offset == 0


@pytest.mark.parametrize('source', [None, SimpleNamespace(poll=True, chat=SimpleNamespace(id=999)),
    SimpleNamespace(video=True, id=5, has_protected_content=True, chat=SimpleNamespace(id=999)),
    SimpleNamespace(photo=True, id=5, chat=SimpleNamespace(id=999, has_protected_content=True))])
def test_unsupported_or_protected_reply_is_explicit_and_never_starts(setup, source):
    state, manager, bot = setup
    asyncio.run(owner_commands.cmd_broadcast(bot, command('/broadcast', source=source)))
    assert manager.job is None
    assert state.scanned == 0
    assert bot.send_message.await_args.args[0] == 999
    assert bot.send_message.await_args.args[1]


def test_recipients_deduplicated_and_policy_states_excluded(setup, monkeypatch):
    async def run():
        state, manager, bot = setup
        state.rows = [
            {'chat_id': 1}, {'chat_id': '1'}, {'chat_id': -100}, {'chat_id': 0}, {'chat_id': True},
            {'chat_id': 'invalid'}, {'chat_id': 1.5}, {'chat_id': 2, 'blocked': True},
            {'chat_id': 3, 'broadcast_opt_out': True}, {'chat_id': 4, 'deactivated': True},
            {'chat_id': 5, 'broadcast_enabled': False}, {'chat_id': 6, 'is_bot': True},
            {'chat_id': 7, 'chat_type': 'channel'}, {'chat_id': 8}]
        monkeypatch.setattr(async_db, 'is_banned', AsyncMock(side_effect=lambda uid: uid == 8))
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert (job.total, job.successful, job.excluded) == (1, 1, 7)
        assert_totals(job)
    asyncio.run(run())


def test_effective_role_ban_and_existing_block_store_respected(setup, monkeypatch):
    async def run():
        state, manager, bot = setup
        from core.role_manager import role_manager
        from core import mongo_store
        monkeypatch.setattr(role_manager, 'is_banned', AsyncMock(side_effect=lambda uid: uid == 1))
        store = SimpleNamespace(is_connected=lambda: True, is_blocked=AsyncMock(side_effect=lambda uid: uid == 2))
        monkeypatch.setattr(mongo_store, 'get_mongo_store', lambda: store)
        state.rows = [{'chat_id': 1}, {'chat_id': 2}, {'chat_id': 3, 'banned': True}]
        # Raw stale ban flags cannot override the effective global policy.
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert (job.total, job.excluded, job.successful) == (1, 2, 1)
    asyncio.run(run())


def test_blocked_unreachable_recipient_is_not_retried(setup):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}, {'chat_id': 2}]
        async def send(uid, *args, **kwargs):
            if uid == 1:
                raise UserIsBlocked()
            return SimpleNamespace(id=700)
        bot.send_message.side_effect = send
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert (job.successful, job.skipped, job.failed) == (1, 1, 0)
        assert sum(call.args[0] == 1 for call in bot.send_message.await_args_list) == 1
        assert_totals(job)
    asyncio.run(run())


def test_floodwait_honors_full_shared_cooldown_then_retries(setup):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}]
        attempts, waits = [], []
        async def send(uid, *args, **kwargs):
            if uid != 999:
                attempts.append(uid)
                if len(attempts) == 1:
                    raise FloodWait(2)
            return SimpleNamespace(id=700)
        async def wait(job, seconds):
            waits.append(seconds)
            return True
        bot.send_message.side_effect = send
        manager._wait = wait
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert attempts == [1, 1]
        assert max(waits) > 2
        assert job.successful == 1 and job.failed == 0
    asyncio.run(run())


def test_transient_errors_obey_retry_limit_and_continue(setup):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}, {'chat_id': 2}]
        async def send(uid, *args, **kwargs):
            if uid == 1:
                raise ConnectionError('temporary')
            return SimpleNamespace(id=700)
        bot.send_message.side_effect = send
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert sum(call.args[0] == 1 for call in bot.send_message.await_args_list) == manager.max_attempts
        assert (job.failed, job.successful, job.status) == (1, 1, 'Completed')
        assert_totals(job)
    asyncio.run(run())


def test_long_flood_stops_without_bypassing_cooldown(setup):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}, {'chat_id': 2}]
        async def send(uid, *args, **kwargs):
            if uid != 999:
                raise FloodWait(999)
            return SimpleNamespace(id=700)
        bot.send_message.side_effect = send
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert job.status == 'Stopped'
        assert job.failed == 1 and job.total == 2
        assert not any(call.args[0] == 2 for call in bot.send_message.await_args_list)
        assert_totals(job)
    asyncio.run(run())


def test_concurrent_broadcast_prevented_and_cancel_keeps_inflight_stats(setup):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}, {'chat_id': 2}]
        entered, release = asyncio.Event(), asyncio.Event()
        async def send(uid, *args, **kwargs):
            if uid == 1:
                entered.set()
                await release.wait()
            return SimpleNamespace(id=700)
        bot.send_message.side_effect = send
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await entered.wait()
        with pytest.raises(BroadcastBusy):
            await manager.start(bot, 999, BroadcastContent(text='duplicate'))
        assert not manager.cancel(1)
        assert manager.cancel(999)
        release.set()
        await job.task
        assert (job.total, job.successful, job.status) == (2, 1, 'Cancelled')
        assert not any(call.args[0] == 2 for call in bot.send_message.await_args_list)
        assert not any(t.get_name() == 'broadcast-progress' for t in asyncio.all_tasks())
        assert_totals(job)
    asyncio.run(run())


def test_cancel_interrupts_backoff_and_shutdown_cleans_tasks(setup):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}]
        entered = asyncio.Event()
        async def send(uid, *args, **kwargs):
            if uid == 1:
                entered.set()
                manager._next_send = broadcast.time.monotonic() + 100
                raise ConnectionError('temporary')
            return SimpleNamespace(id=700)
        bot.send_message.side_effect = send
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await entered.wait()
        await manager.shutdown()
        assert job.task.done()
        assert job.status == 'Cancelled'
        assert_totals(job)
    asyncio.run(run())


def test_progress_uses_owner_only_and_final_statistics_match(setup):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}]
        async def send(uid, *args, **kwargs):
            if uid == 1:
                await asyncio.sleep(0.035)
            return SimpleNamespace(id=700)
        bot.send_message.side_effect = send
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert manager.progress_interval == 0.01  # Production default is five seconds.
        assert BroadcastManager().progress_interval == 5
        edits = bot.edit_message_text.await_args_list
        assert len(edits) >= 2
        assert all(call.args[0] == 999 for call in edits)
        assert 'Processed: 0' in edits[0].args[2]
        assert 'Processed: 1' in edits[-1].args[2]
        assert 'Completed' in edits[-1].args[2]
        assert_totals(job)
    asyncio.run(run())


def test_source_copy_restriction_stops_without_download_fallback(setup):
    async def run():
        state, manager, bot = setup
        state.rows = [{'chat_id': 1}, {'chat_id': 2}]
        error = RuntimeError('protected')
        error.ID = 'CHAT_FORWARDS_RESTRICTED'
        bot.copy_message.side_effect = error
        job = await manager.start(bot, 999, BroadcastContent(source_chat_id=999, source_message_id=8))
        await job.task
        assert job.status == 'Stopped' and job.failed == 1
        bot.copy_message.assert_awaited_once()
        assert_totals(job)
    asyncio.run(run())


def test_enumeration_failure_is_not_reported_as_completed(setup, monkeypatch):
    async def run():
        state, manager, bot = setup
        async def rows():
            yield {'chat_id': 1}
            raise RuntimeError('DB down')
        monkeypatch.setattr(async_db, 'iter_registered_users', rows)
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert job.status == 'Stopped' and job.successful == 0
        assert not job.selection_complete
        assert_totals(job)
    asyncio.run(run())


def test_stop_broadcast_does_not_touch_download_tasks(setup, monkeypatch):
    async def run():
        from TechVJ import save
        state, manager, bot = setup
        monkeypatch.setattr(save, 'OWNER_ID', 999)
        monkeypatch.setattr(save.task_manager, 'get_active_task_count', AsyncMock(side_effect=AssertionError('download touched')))
        monkeypatch.setattr(save.task_manager, 'cancel_all_tasks', AsyncMock(side_effect=AssertionError('download touched')))
        msg = command('/stop broadcast')
        await save.stop_command(bot, msg)
        assert bot.send_message.await_args.args[1] == 'No active broadcast.'
        save.task_manager.cancel_all_tasks.assert_not_awaited()
    asyncio.run(run())


def test_ownerhelp_preserves_existing_entries_controls_and_security(setup):
    async def run():
        msg = command('/ownerhelp')
        msg.reply = AsyncMock()
        await owner_commands.cmd_ownerhelp(None, msg)
        text = msg.reply.await_args.args[0]
        assert '/broadcast — Send a message to all registered bot users. Owner only.' in text
        assert '/stop broadcast' in text and '/autoarchive' in text and '/setrole' in text
        assert msg.reply.await_args.kwargs['reply_markup']
        assert utf16_length(text) <= 4096
        other = command('/ownerhelp', uid=1)
        other.reply = AsyncMock()
        await owner_commands.cmd_ownerhelp(None, other)
        other.reply.assert_not_awaited()
    asyncio.run(run())


def test_delivery_spacing_is_bounded_and_not_parallel(setup):
    async def run():
        state, manager, bot = setup
        manager.interval = 0.015
        state.rows = [{'chat_id': uid} for uid in (1, 2, 3)]
        times, active, maximum = [], 0, 0
        async def send(uid, *args, **kwargs):
            nonlocal active, maximum
            if uid != 999:
                active += 1
                maximum = max(maximum, active)
                times.append(broadcast.time.monotonic())
                await asyncio.sleep(0)
                active -= 1
            return SimpleNamespace(id=700)
        bot.send_message.side_effect = send
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert maximum == 1
        assert all(later - earlier >= 0.012 for earlier, later in zip(times, times[1:]))
    asyncio.run(run())


def test_cancel_during_selection_closes_cursor_and_marks_partial_total(setup, monkeypatch):
    async def run():
        state, manager, bot = setup
        async def rows():
            try:
                yield {'chat_id': 1}
                manager.cancel(999)
                yield {'chat_id': 2}
            finally:
                state.closed = True
        monkeypatch.setattr(async_db, 'iter_registered_users', rows)
        job = await manager.start(bot, 999, BroadcastContent(text='hello'))
        await job.task
        assert state.closed
        assert not job.selection_complete
        assert job.status == 'Cancelled' and job.processed == 0
        assert 'selection incomplete' in job.render()
    asyncio.run(run())


def test_sqlite_registered_iterator_batches_and_omits_session_strings(monkeypatch, tmp_path):
    async def run():
        from database import local_storage as local
        import aiosqlite
        path = tmp_path / 'registered.db'
        async with aiosqlite.connect(path) as db:
            await db.execute('CREATE TABLE sessions(chat_id INTEGER PRIMARY KEY, session_string TEXT, logged_in INTEGER, data_json TEXT)')
            await db.executemany('INSERT INTO sessions VALUES(?,?,?,?)', [
                (1, 'private-auth', 0, '{"broadcast_opt_out":true,"chat_id":9000,"session":"do-not-read"}'),
                (2, 'private-auth', 1, '{}'), (3, 'private-auth', 0, 'bad-json'),
                (-100, 'private-auth', 0, '{}')])
            await db.commit()
        monkeypatch.setattr(local, '_DB_PATH', str(path))
        monkeypatch.setattr(local, '_initialized', True)
        rows = [row async for row in local.LocalStorage.iter_registered_users(batch_size=1)]
        assert rows == [{'chat_id': 1, 'broadcast_opt_out': True}, {'chat_id': 2}, {'chat_id': 3}]
        assert all('session' not in row and 'session_string' not in row for row in rows)
    asyncio.run(run())


def test_database_local_first_and_mongo_fallback_close_cursors(monkeypatch):
    async def run():
        from database import async_db as db_module
        class Cursor:
            def __init__(self):
                self.close = AsyncMock()
                self.size = None
            def batch_size(self, size):
                self.size = size
                return self
            def __aiter__(self):
                return self.rows()
            async def rows(self):
                yield {'chat_id': 2}
        cursor = Cursor()
        projection = {}
        def find(query, fields):
            projection.update(fields)
            return cursor
        monkeypatch.setattr(db_module, 'get_sessions_collection', lambda: SimpleNamespace(find=find))
        async def local_rows(size):
            yield {'chat_id': 1}
        local = SimpleNamespace(iter_registered_users=local_rows)
        monkeypatch.setattr(db_module.AsyncDatabase, '_get_local', lambda: local)
        assert [row async for row in db_module.AsyncDatabase.iter_registered_users(1)] == [{'chat_id': 1}, {'chat_id': 2}]
        cursor.close.assert_awaited_once()
        cursor.close.reset_mock()
        async def broken(size):
            raise RuntimeError('SQLite unavailable')
            yield
        local.iter_registered_users = broken
        assert [row async for row in db_module.AsyncDatabase.iter_registered_users(2)] == [{'chat_id': 2}]
        assert cursor.size == 2
        cursor.close.assert_awaited_once()
        assert 'session' not in projection and 'session_string' not in projection
    asyncio.run(run())
