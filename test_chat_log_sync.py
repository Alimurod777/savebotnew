"""Offline persistence regressions: never connect to Telegram or MongoDB."""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from core import chat_log
from core.chat_log import ChatLogSettings, LogPolicy


@pytest.fixture
def collection(monkeypatch):
    class Collection:
        def __init__(self):
            self.docs = {}
            self.find_one = AsyncMock(side_effect=self.find)
            self.update_one = AsyncMock(side_effect=self.update)

        async def find(self, key):
            return self.docs.get(key['_id'])

        async def update(self, key, update, upsert):
            self.docs.setdefault(key['_id'], {}).update(update['$set'])

    coll = Collection()
    monkeypatch.setattr(chat_log, '_settings_collection', lambda: coll)
    return coll


@pytest.mark.parametrize('policy', [
    LogPolicy(-1009, True, 'owner'),
    LogPolicy(-1009, False, 'owner'),
    LogPolicy(source='owner'),
])
def test_settings_restore_on_new_server_including_off_and_remove(tmp_path, collection, policy):
    async def run():
        old = ChatLogSettings(tmp_path / 'old.json', -1001)
        await old.startup_sync(777)
        await old.save(policy)
        assert old.sync_status == 'synced'
        new = ChatLogSettings(tmp_path / 'new.json', -1002)
        await new.startup_sync(777)
        assert new.policy == policy
        assert new.sync_status == 'synced'
        assert not json.loads(new.path.read_text())['pending_sync']
        assert ChatLogSettings(new.path, -1003).policy == policy
    asyncio.run(run())


def test_reset_overwrites_remote_with_portable_env_tombstone(tmp_path, collection):
    async def run():
        settings = ChatLogSettings(tmp_path / 'old.json', -1001)
        await settings.startup_sync(777)
        await settings.save(LogPolicy(-1009, True, 'owner'))
        await settings.reset()
        assert collection.docs['chat_log:777'] == {
            'override': False, 'chat_id': 0, 'enabled': False, 'excluded_user_ids': []}
        new = ChatLogSettings(tmp_path / 'new.json', -1002)
        await new.startup_sync(777)
        assert new.policy == LogPolicy(-1002, True)
    asyncio.run(run())


def test_legacy_json_is_seeded_to_mongo_without_new_command(tmp_path, collection):
    async def run():
        path = tmp_path / 'old.json'
        path.write_text(json.dumps({'override': True, 'chat_id': -1009, 'enabled': True}))
        settings = ChatLogSettings(path, 0)
        await settings.startup_sync(777)
        assert collection.docs['chat_log:777']['chat_id'] == -1009
        assert settings.sync_status == 'synced'
    asyncio.run(run())


def test_pending_change_survives_restart_and_wins_over_stale_remote(tmp_path, collection):
    async def run():
        path = tmp_path / 'old.json'
        old = ChatLogSettings(path, 0)
        await old.startup_sync(777)
        await old.save(LogPolicy(-1009, True, 'owner'))
        collection.update_one.side_effect = OSError('offline')
        await old.save(LogPolicy(source='owner'))
        assert old.policy == LogPolicy(source='owner')
        assert old.sync_status == 'pending'
        assert json.loads(path.read_text())['pending_sync']
        collection.update_one.side_effect = collection.update
        restarted = ChatLogSettings(path, -1002)
        await restarted.startup_sync(777)
        assert restarted.policy == LogPolicy(source='owner')
        new = ChatLogSettings(tmp_path / 'new.json', -1003)
        await new.startup_sync(777)
        assert new.policy == LogPolicy(source='owner')
    asyncio.run(run())


def test_restore_failure_retries_without_overwriting_remote(tmp_path, collection):
    async def run():
        collection.docs['chat_log:777'] = {'override': True, 'chat_id': -1009, 'enabled': True}
        collection.find_one.side_effect = OSError('offline')
        settings = ChatLogSettings(tmp_path / 'new.json', 0)
        await settings.startup_sync(777)
        assert settings.sync_status == 'restore_failed'
        collection.update_one.assert_not_awaited()
        collection.find_one.side_effect = collection.find
        await settings.sync()
        assert settings.policy == LogPolicy(-1009, True, 'owner')
    asyncio.run(run())


def test_bot_identity_isolates_destinations_in_same_database(tmp_path, collection):
    async def run():
        one = ChatLogSettings(tmp_path / 'one.json', 0)
        await one.startup_sync(777)
        await one.save(LogPolicy(-1009, True, 'owner'))
        two = ChatLogSettings(tmp_path / 'two.json', 0)
        await two.startup_sync(888)
        assert two.policy == LogPolicy()
        assert not two.path.exists()
    asyncio.run(run())


def test_corrupt_local_override_does_not_publish_or_reactivate_env(tmp_path, collection):
    async def run():
        path = tmp_path / 'bad.json'
        path.write_text('{bad')
        settings = ChatLogSettings(path, -1001)
        await settings.startup_sync(777)
        assert settings.policy.source == 'invalid'
        assert settings.sync_status == 'invalid_local'
        collection.update_one.assert_not_awaited()
    asyncio.run(run())


def test_invalid_remote_preserves_local_and_is_not_overwritten(tmp_path, collection):
    async def run():
        collection.docs['chat_log:777'] = {'override': True, 'chat_id': 123, 'enabled': True}
        settings = ChatLogSettings(tmp_path / 'new.json', 0)
        await settings.startup_sync(777)
        assert settings.policy == LogPolicy()
        assert settings.sync_status == 'restore_failed'
        collection.update_one.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('source', ['owner', 'env'])
def test_exclusions_restore_locally_and_on_new_server(tmp_path, collection, source):
    async def run():
        settings = ChatLogSettings(tmp_path / 'old.json', -1001)
        await settings.startup_sync(777)
        await settings.save(LogPolicy(-1009, True, source, (123, 456)))
        if source == 'env':
            await settings.reset()
        new = ChatLogSettings(tmp_path / 'new.json', -1002)
        await new.startup_sync(777)
        assert new.policy.excluded_user_ids == (123, 456)
        assert ChatLogSettings(new.path, -1003).policy.excluded_user_ids == (123, 456)
        assert new.policy.chat_id == (-1009 if source == 'owner' else -1002)
        await new.save(LogPolicy(new.policy.chat_id, True, source))
        restored = ChatLogSettings(tmp_path / 'third.json', -1004)
        await restored.startup_sync(777)
        assert restored.policy.excluded_user_ids == ()
    asyncio.run(run())


@pytest.mark.parametrize('excluded', [None, '123', [True], [0], [-1], ['123']])
def test_invalid_exclusions_disable_logging(tmp_path, collection, excluded):
    path = tmp_path / 'bad.json'
    path.write_text(json.dumps({'override': False, 'excluded_user_ids': excluded}))
    assert ChatLogSettings(path, -1001).policy.source == 'invalid'
