from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core import bot_archive
from core.bot_archive import BotArchiveSettings
from core.user_upload_worker import UserUploadWorker, UserWorkerRegistry
from TechVJ import owner_commands, session_handler


@pytest.fixture
def policy(monkeypatch, tmp_path):
    settings = BotArchiveSettings(tmp_path / 'policy.json')
    monkeypatch.setattr(bot_archive, 'bot_archive_settings', settings)
    return settings


def test_policy_defaults_on_and_survives_restart(policy):
    assert policy.enabled is True
    asyncio.run(policy.set_enabled(False))
    assert BotArchiveSettings(policy.path).enabled is False
    asyncio.run(policy.set_enabled(True))
    assert BotArchiveSettings(policy.path).enabled is True


def test_failed_write_keeps_previous_policy(policy, monkeypatch):
    asyncio.run(policy.set_enabled(False))
    def fail(*args):
        raise OSError('disk unavailable')
    monkeypatch.setattr(bot_archive.os, 'replace', fail)
    with pytest.raises(OSError):
        asyncio.run(policy.set_enabled(True))
    assert policy.enabled is False
    assert BotArchiveSettings(policy.path).enabled is False
    assert list(policy.path.parent.glob('.bot_archive-*')) == []


@pytest.mark.parametrize('enabled', [True, False])
def test_apply_routes_only_to_archive_operation(policy, enabled):
    async def run():
        await policy.set_enabled(enabled)
        acc = SimpleNamespace(archive_chats=AsyncMock(return_value=True),
                              unarchive_chats=AsyncMock(return_value=True), send_message=AsyncMock())
        assert await bot_archive.apply_bot_archive(acc, 777)
        selected = acc.archive_chats if enabled else acc.unarchive_chats
        other = acc.unarchive_chats if enabled else acc.archive_chats
        selected.assert_awaited_once_with([777])
        other.assert_not_awaited()
        acc.send_message.assert_not_awaited()
    asyncio.run(run())


def test_rpc_error_is_nonfatal(policy):
    acc = SimpleNamespace(archive_chats=AsyncMock(side_effect=RuntimeError('flood')))
    assert asyncio.run(bot_archive.apply_bot_archive(acc, 777)) is False


@pytest.mark.parametrize('enabled', [True, False])
def test_worker_existing_dialog_applies_policy_without_start(policy, enabled):
    async def run():
        await policy.set_enabled(enabled)
        worker = UserUploadWorker('synthetic', 123, bot_id=777, bot_username='testbot')
        acc = SimpleNamespace(get_chat=AsyncMock(return_value=SimpleNamespace(id=777)),
                              archive_chats=AsyncMock(return_value=True),
                              unarchive_chats=AsyncMock(return_value=True), send_message=AsyncMock())
        worker._client = acc
        await worker._prepare_bot_chat('testbot')
        assert worker._bot_prepared is True
        (acc.archive_chats if enabled else acc.unarchive_chats).assert_awaited_once_with([777])
        acc.send_message.assert_not_awaited()
    asyncio.run(run())


def test_sync_reuses_live_client_and_continues_after_bad_session(policy, monkeypatch):
    async def run():
        await policy.set_enabled(False)
        from core.user_upload_worker import worker_registry
        live = SimpleNamespace(get_chat=AsyncMock(return_value=SimpleNamespace(id=777)),
                               unarchive_chats=AsyncMock(return_value=True))
        offline = SimpleNamespace(get_chat=AsyncMock(return_value=SimpleNamespace(id=777)),
                                  unarchive_chats=AsyncMock(return_value=True))
        monkeypatch.setattr(bot_archive, '_collect_archive_sessions', AsyncMock(return_value=(
            {'live': 1, 'invalid': 2, 'offline': 3}, 1)))
        monkeypatch.setattr(worker_registry, 'archive_clients_snapshot', AsyncMock(return_value=[('live', 1, live)]))
        opened = []
        closed = []
        @asynccontextmanager
        async def connect(session, uid, **kwargs):
            opened.append(session)
            if session == 'invalid':
                raise RuntimeError('expired')
            try:
                yield offline
            finally:
                closed.append(session)
        monkeypatch.setattr(session_handler, 'create_user_session', connect)
        result = await bot_archive.sync_bot_archive('@testbot')
        assert result == {'total': 3, 'updated': 2, 'failed': 1, 'source_errors': 1}
        assert opened == ['invalid', 'offline']
        assert closed == ['offline']
        live.unarchive_chats.assert_awaited_once_with([777])
        offline.unarchive_chats.assert_awaited_once_with([777])
    asyncio.run(run())


def test_session_collection_deduplicates_all_sources(policy, monkeypatch, tmp_path):
    async def run():
        from database.async_db import async_db
        from core.session_manager import session_manager
        from core import premium_session_store, premium_logic
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(async_db, 'get_all_logged_in_users', AsyncMock(return_value=[
            {'chat_id': 1, 'session': 'shared'}, {'chat_id': 2, 'session': None}]))
        monkeypatch.setattr(session_manager.registry, 'get_all', lambda: [
            SimpleNamespace(session_string='shared', owner_user_id=1),
            SimpleNamespace(session_string='managed', owner_user_id=None)])
        monkeypatch.setattr(premium_session_store, 'get_all_sessions', AsyncMock(return_value=[
            SimpleNamespace(session_string='premium', user_id=3)]))
        monkeypatch.setattr(premium_logic, 'get_system_session', lambda: SimpleNamespace(
            session_string='system', user_id=4))
        folder = tmp_path / 'sessions'
        folder.mkdir()
        (folder / 'user_5.session').write_text('fallback')
        (folder / 'user_6.session').write_bytes(b'SQLite format 3\x00')
        sessions, errors = await bot_archive._collect_archive_sessions()
        assert sessions == {'shared': 1, 'managed': 0, 'premium': 3, 'system': 4, 'fallback': 5}
        assert errors == 0
    asyncio.run(run())


def make_message(owner=True, text='/autoarchive off'):
    progress = SimpleNamespace(edit=AsyncMock())
    return SimpleNamespace(text=text, chat=SimpleNamespace(id=owner_commands.OWNER_ID),
                           from_user=SimpleNamespace(id=owner_commands.OWNER_ID if owner else -1),
                           reply=AsyncMock(return_value=progress)), progress


def test_owner_command_off_persists_and_reports_results(policy, monkeypatch):
    sync = AsyncMock(return_value={'total': 2, 'updated': 1, 'failed': 1, 'source_errors': 0})
    monkeypatch.setattr(bot_archive, 'sync_bot_archive', sync)
    msg, progress = make_message()
    asyncio.run(owner_commands.cmd_autoarchive(SimpleNamespace(me=SimpleNamespace(username='testbot')), msg))
    assert policy.enabled is False
    sync.assert_awaited_once_with('testbot')
    text = progress.edit.await_args.kwargs['text']
    assert 'OFF' in text and 'Yangilandi: 1' in text and 'Muvaffaqiyatsiz: 1' in text


def test_nonowner_command_and_callback_cannot_change_policy(policy, monkeypatch):
    async def run():
        sync = AsyncMock()
        monkeypatch.setattr(bot_archive, 'sync_bot_archive', sync)
        msg, _ = make_message(owner=False)
        await owner_commands.cmd_autoarchive(None, msg)
        msg.reply.assert_not_awaited()
        query = SimpleNamespace(from_user=msg.from_user, data='bot_archive:off', answer=AsyncMock())
        await owner_commands.cb_bot_archive(None, query)
        assert policy.enabled is True
        sync.assert_not_awaited()
        query.answer.assert_awaited_once()
    asyncio.run(run())


def test_ownerhelp_has_current_state_and_controls(policy):
    asyncio.run(policy.set_enabled(False))
    msg, _ = make_message(text='/ownerhelp')
    asyncio.run(owner_commands.cmd_ownerhelp(None, msg))
    args = msg.reply.await_args
    assert '/autoarchive on|off|status' in args.args[0]
    assert 'OFF' in args.args[0]
    buttons = args.kwargs['reply_markup'].inline_keyboard[0]
    assert [button.callback_data for button in buttons] == ['bot_archive:on', 'bot_archive:off']


def test_status_does_not_sync_and_owner_callback_does(policy, monkeypatch):
    async def run():
        sync = AsyncMock(return_value={'total': 0, 'updated': 0, 'failed': 0, 'source_errors': 0})
        monkeypatch.setattr(bot_archive, 'sync_bot_archive', sync)
        msg, _ = make_message(text='/autoarchive status')
        await owner_commands.cmd_autoarchive(None, msg)
        sync.assert_not_awaited()
        query = SimpleNamespace(from_user=msg.from_user, message=msg,
                                data='bot_archive:off', answer=AsyncMock())
        await owner_commands.cb_bot_archive(SimpleNamespace(me=SimpleNamespace(username='testbot')), query)
        assert policy.enabled is False
        sync.assert_awaited_once()
    asyncio.run(run())


def test_new_login_unblock_honors_off(policy, monkeypatch):
    async def run():
        from core.bot_unblock import try_unblock_bot
        await policy.set_enabled(False)
        acc = SimpleNamespace(get_chat=AsyncMock(return_value=SimpleNamespace(id=777)),
                              unblock_user=AsyncMock(), send_message=AsyncMock(),
                              unarchive_chats=AsyncMock(return_value=True), archive_chats=AsyncMock())
        @asynccontextmanager
        async def connect(*args, **kwargs):
            yield acc
        monkeypatch.setattr(session_handler, 'create_user_session', connect)
        assert await try_unblock_bot('synthetic', 123, 'testbot')
        acc.unarchive_chats.assert_awaited_once_with([777])
        acc.archive_chats.assert_not_awaited()
    asyncio.run(run())


def test_policy_change_waits_for_inflight_archive_rpc(policy):
    async def run():
        entered = asyncio.Event()
        finish = asyncio.Event()
        async def archive(peers):
            entered.set()
            await finish.wait()
            return True
        acc = SimpleNamespace(archive_chats=archive, unarchive_chats=AsyncMock(return_value=True))
        applying = asyncio.create_task(bot_archive.apply_bot_archive(acc, 777))
        await entered.wait()
        change = asyncio.create_task(policy.set_enabled(False))
        await asyncio.sleep(0)
        assert not change.done()
        finish.set()
        assert await applying is True
        await change
        assert await bot_archive.apply_bot_archive(acc, 777)
        acc.unarchive_chats.assert_awaited_once_with([777])
    asyncio.run(run())


def test_worker_new_dialog_honors_off(policy):
    async def run():
        await policy.set_enabled(False)
        worker = UserUploadWorker('synthetic', 123, bot_id=777, bot_username='testbot')
        acc = SimpleNamespace(get_chat=AsyncMock(side_effect=[RuntimeError('missing'), SimpleNamespace(id=777)]),
                              send_message=AsyncMock(), archive_chats=AsyncMock(),
                              unarchive_chats=AsyncMock(return_value=True))
        worker._client = acc
        await worker._prepare_bot_chat('testbot')
        acc.send_message.assert_awaited_once_with('testbot', '/start')
        acc.unarchive_chats.assert_awaited_once_with([777])
        acc.archive_chats.assert_not_awaited()
    asyncio.run(run())


def test_archive_client_snapshot_only_connected_workers():
    async def run():
        registry = UserWorkerRegistry()
        connected = SimpleNamespace(is_connected=True)
        registry._workers = {
            (1, 'one'): SimpleNamespace(_session_string='one', _user_id=1, _client=connected, _running=True),
            (2, 'two'): SimpleNamespace(_client=None, _running=False),
            (3, 'three'): SimpleNamespace(_client=SimpleNamespace(is_connected=False), _running=True),
        }
        assert await registry.archive_clients_snapshot() == [('one', 1, connected)]
    asyncio.run(run())


def test_command_write_failure_does_not_sync(policy, monkeypatch):
    monkeypatch.setattr(policy, 'set_enabled', AsyncMock(side_effect=OSError('disk failure')))
    sync = AsyncMock()
    monkeypatch.setattr(bot_archive, 'sync_bot_archive', sync)
    msg, _ = make_message()
    asyncio.run(owner_commands.cmd_autoarchive(SimpleNamespace(me=SimpleNamespace(username='testbot')), msg))
    assert policy.enabled is True
    sync.assert_not_awaited()
    assert "saqlab bo'lmadi" in msg.reply.await_args.args[0]


@pytest.mark.parametrize('text', ['/autoarchive wrong', '/autoarchive on extra'])
def test_invalid_command_leaves_policy_unchanged(policy, text):
    msg, _ = make_message(text=text)
    asyncio.run(owner_commands.cmd_autoarchive(None, msg))
    assert policy.enabled is True
    assert 'Usage:' in msg.reply.await_args.args[0]
