import asyncio
import copy

from core.owner_settings import OwnerSettings


class Collection:
    def __init__(self):
        self.docs = {}
        self.fail = False

    async def find_one(self, query):
        if self.fail:
            raise OSError('offline')
        return copy.deepcopy(self.docs.get(query['_id']))

    async def update_one(self, query, update, upsert=False):
        if self.fail:
            raise OSError('offline')
        doc = self.docs.setdefault(query['_id'], {'values': {}})
        for key, value in update['$set'].items():
            doc['values'][key.split('.', 1)[1]] = copy.deepcopy(value)


def test_new_server_restore_and_off_tombstones(tmp_path):
    async def run():
        coll = Collection()
        old = OwnerSettings(tmp_path / 'old.json', coll)
        old.bot_id = 123
        for key, value in [('maintenance', True), ('archive_policy', {'enabled': False}),
                           ('role_policy', {'role': 'normal_user', 'overrides': {}}),
                           ('session_registry', []), ('system_session', None),
                           ('rate_limits', {'vip_user': 25}),
                           ('parallel_limits', {'normal_user': 6})]:
            await old.change(key, value)
        new = OwnerSettings(tmp_path / 'new.json', coll)
        new.bot_id = 123
        await new.sync(restore=True)
        assert new.values == old.values
        assert new.status == 'synced'
        assert not new.pending
        assert OwnerSettings(new.path, coll).values == old.values
    asyncio.run(run())


def test_pending_local_change_survives_restart_and_stale_remote(tmp_path):
    async def run():
        coll = Collection()
        store = OwnerSettings(tmp_path / 'settings.json', coll)
        store.bot_id = 1
        await store.change('maintenance', False)
        coll.fail = True
        await store.change('maintenance', True)
        assert store.status == 'pending'
        restarted = OwnerSettings(store.path, coll)
        restarted.bot_id = 1
        coll.fail = False
        await restarted.sync(restore=True)
        assert restarted.values['maintenance'] is True
        assert coll.docs['owner_settings:1']['values']['maintenance'] is True
    asyncio.run(run())


def test_existing_remote_overrides_legacy_local_defaults(tmp_path):
    async def run():
        coll = Collection()
        coll.docs['owner_settings:1'] = {'values': {'maintenance': True}}
        store = OwnerSettings(tmp_path / 'settings.json', coll)
        store.bot_id = 1
        store.values = {'maintenance': False, 'parallel_limits': {'vip_user': 7}}
        await store.sync(restore=True)
        assert store.values['maintenance'] is True
        assert coll.docs['owner_settings:1']['values']['parallel_limits'] == {'vip_user': 7}
    asyncio.run(run())


def test_different_bot_has_separate_remote_document(tmp_path):
    async def run():
        coll = Collection()
        for uid in (1, 2):
            store = OwnerSettings(tmp_path / f'{uid}.json', coll)
            store.bot_id = uid
            await store.change('maintenance', uid == 1)
        assert coll.docs['owner_settings:1']['values']['maintenance'] is True
        assert coll.docs['owner_settings:2']['values']['maintenance'] is False
    asyncio.run(run())


def test_disk_failure_does_not_publish_setting(tmp_path, monkeypatch):
    store = OwnerSettings(tmp_path / 'settings.json')
    def fail(*args):
        raise OSError('disk full')
    monkeypatch.setattr(store, '_write', fail)
    import pytest
    with pytest.raises(OSError):
        store.stage('maintenance', True)
    assert store.values == {}
    assert not store.pending


def test_checkpoint_records_deletion_and_empty_registry(tmp_path, monkeypatch):
    async def run():
        import core.owner_settings as module
        path = tmp_path / 'registry.json'
        monkeypatch.setattr(module, 'FILES', {'session_registry': path})
        coll = Collection()
        store = OwnerSettings(tmp_path / 'settings.json', coll)
        store.bot_id = 1
        path.write_text('[]')
        await store.checkpoint()
        assert store.values['session_registry'] == []
        path.unlink()
        await store.checkpoint()
        assert coll.docs['owner_settings:1']['values']['session_registry'] is None
    asyncio.run(run())


def test_failed_restore_does_not_overwrite_unrelated_remote_keys(tmp_path):
    async def run():
        coll = Collection()
        coll.docs['owner_settings:1'] = {'values': {
            'session_registry': [{'session_id': 'mock'}], 'maintenance': False}}
        store = OwnerSettings(tmp_path / 'settings.json', coll)
        store.bot_id = 1
        store.values['session_registry'] = []  # stale local migration candidate
        coll.fail = True
        await store.sync(restore=True)
        store.stage('maintenance', True)
        coll.fail = False
        await store.sync()
        assert store.values['session_registry'] == [{'session_id': 'mock'}]
        assert coll.docs['owner_settings:1']['values']['maintenance'] is True
    asyncio.run(run())


def test_checkpoint_skips_unchanged_local_files(tmp_path, monkeypatch):
    async def run():
        import core.owner_settings as module
        path = tmp_path / 'registry.json'
        path.write_text('[]')
        monkeypatch.setattr(module, 'FILES', {'session_registry': path})
        coll = Collection()
        store = OwnerSettings(tmp_path / 'settings.json', coll)
        store.bot_id = 1
        coll.docs['owner_settings:1'] = {'values': {'session_registry': ['remote']}}
        await store.checkpoint(before={'session_registry': []})
        assert coll.docs['owner_settings:1']['values']['session_registry'] == ['remote']
    asyncio.run(run())


def test_start_stop_restores_then_cancels_retry_task(tmp_path, monkeypatch):
    async def run():
        import core.owner_settings as module
        monkeypatch.setattr(module, 'FILES', {})
        coll = Collection()
        coll.docs['owner_settings:99'] = {'values': {'maintenance': True}}
        store = OwnerSettings(tmp_path / 'settings.json', coll)
        seen = []
        monkeypatch.setattr(store, 'apply', lambda: seen.append(store.values.copy()))
        await store.start(99)
        assert seen == [{'maintenance': True}]
        task = store.task
        await store.stop()
        assert task.done()
        assert store.task is None
    asyncio.run(run())


def test_enable_global_includes_disabled_records(monkeypatch):
    async def run():
        import core.session_manager as module
        import TechVJ.owner_commands as commands
        from core.session_manager.models import SessionType
        from types import SimpleNamespace
        enabled = []
        replies = []
        records = [SimpleNamespace(type=SessionType.GLOBAL, enabled=False, session_id='global'),
                   SimpleNamespace(type=SessionType.DEDICATED, enabled=False, session_id='dedicated')]
        async def enable(sid):
            enabled.append(sid)
            return True
        async def reply(text):
            replies.append(text)
        monkeypatch.setattr(module, 'session_manager', SimpleNamespace(
            registry=SimpleNamespace(get_all=lambda: records), enable_session=enable))
        monkeypatch.setattr(commands, '_owner', lambda message: True)
        message = SimpleNamespace(reply=reply)
        await commands.cmd_enable_global(None, message)
        assert enabled == ['global']
        assert 'Enabled 1' in replies[0]
    asyncio.run(run())


def test_apply_restores_runtime_limits_and_configuration_files(tmp_path, monkeypatch):
    import core.owner_settings as module
    from core.rate_limiter import rate_limiter
    from core.priority_queue import priority_queue
    from core.role_manager import UserRole
    from core.channel_monitor import channel_monitor
    from core.bot_archive import bot_archive_settings
    path = tmp_path / 'archive.json'
    monkeypatch.setattr(module, 'FILES', {'archive_policy': path})
    monkeypatch.setattr(channel_monitor, 'load', lambda: None)
    monkeypatch.setattr(bot_archive_settings, '_enabled', True)
    monkeypatch.setattr(rate_limiter, '_limits', dict(rate_limiter._limits))
    monkeypatch.setattr(priority_queue, '_limits', dict(priority_queue._limits))
    store = OwnerSettings(tmp_path / 'settings.json')
    store.values = {'rate_limits': {'vip_user': 25},
                    'parallel_limits': {'normal_user': 6},
                    'archive_policy': {'enabled': False}}
    store.apply()
    assert rate_limiter.get_limit(UserRole.VIP_USER) == (25, 10)
    assert priority_queue._limits[UserRole.NORMAL_USER] == 6
    assert not bot_archive_settings.enabled
    import json
    assert json.loads(path.read_text()) == {'enabled': False}
