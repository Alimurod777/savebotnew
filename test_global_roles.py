"""Global role policy regression checks; isolated files and mocked DB only."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import core.role_manager as roles
from core.role_manager import UserRole
from database.async_db import async_db


@pytest.fixture
def manager(monkeypatch, tmp_path):
    monkeypatch.setattr(roles, '_DATA_DIR', str(tmp_path))
    instance = roles.RoleManager()
    instance._owner_id = 999
    monkeypatch.setattr(roles, 'role_manager', instance)
    monkeypatch.setattr(async_db, 'find_user', AsyncMock(return_value={'role': 'vip_user'}))
    monkeypatch.setattr(async_db, 'update_user', AsyncMock())
    monkeypatch.setattr(async_db, 'ban_user', AsyncMock())
    monkeypatch.setattr(async_db, 'unban_user', AsyncMock())
    return instance


def test_global_resets_existing_roles_bans_and_future_users(manager, tmp_path):
    async def run():
        for uid, role in ((1, UserRole.BANNED), (2, UserRole.NEW_USER), (3, UserRole.VIP_USER)):
            await manager.set_role(uid, role)
        # Existing DB-only users must not restore their previous role either.
        await manager.set_global_role(UserRole.NORMAL_USER)
        async_db.find_user.reset_mock()
        for uid in (1, 2, 3, 4, 100000):
            assert await manager.get_role(uid) == UserRole.NORMAL_USER
            assert await async_db.is_banned(uid) is False
        async_db.find_user.assert_not_awaited()
        assert await manager.get_role(999) == UserRole.VIP_USER
        assert not (tmp_path / '999.txt').exists()
        restarted = roles.RoleManager()
        restarted._owner_id = 999
        assert await restarted.get_role(1) == UserRole.NORMAL_USER
        assert await restarted.get_role(200000) == UserRole.NORMAL_USER
    asyncio.run(run())


def test_individual_commands_override_until_next_global_reset(manager):
    async def run():
        await manager.set_global_role(UserRole.NORMAL_USER)
        await manager.set_role(1, UserRole.BANNED)
        await manager.set_role(2, UserRole.VIP_USER)
        assert await async_db.is_banned(1) is True
        assert await manager.get_role(2) == UserRole.VIP_USER
        await manager.set_role(1, UserRole.NEW_USER)
        assert await async_db.is_banned(1) is False
        assert await manager.get_role(1) == UserRole.NEW_USER
        await manager.set_role(999, UserRole.BANNED)
        assert await async_db.is_banned(999) is False
        # A separately created reader sees subsequent changes without cache reset.
        reader = roles.RoleManager()
        reader._owner_id = 999
        assert await reader.get_role(2) == UserRole.VIP_USER
        await manager.set_global_role(UserRole.NEW_USER)
        assert await reader.get_role(2) == UserRole.NEW_USER
        assert await manager.get_role(1) == UserRole.NEW_USER
        assert manager._read_policy()['overrides'] == {}
    asyncio.run(run())


def test_failed_atomic_write_keeps_previous_policy(manager, monkeypatch):
    async def run():
        await manager.set_global_role(UserRole.NORMAL_USER)
        def fail(*args):
            raise OSError('disk full')
        monkeypatch.setattr(roles.os, 'replace', fail)
        with pytest.raises(OSError):
            await manager.set_global_role(UserRole.VIP_USER)
        with pytest.raises(OSError):
            await manager.set_role(1, UserRole.BANNED)
        assert await manager.get_role(1) == UserRole.NORMAL_USER
        assert await manager.get_role(2) == UserRole.NORMAL_USER
    asyncio.run(run())


def test_global_rejects_banned_and_invalid_roles(manager):
    async def run():
        for role in (UserRole.BANNED, 'vip_user', None):
            with pytest.raises(ValueError):
                await manager.set_global_role(role)
        assert manager._read_policy() is None
    asyncio.run(run())


def test_legacy_default_and_fallback_remain_without_policy(manager):
    async def run():
        assert await manager.get_role(1) == UserRole.VIP_USER
        async_db.find_user.return_value = None
        assert await manager.get_role(2) == UserRole.NEW_USER
    asyncio.run(run())


def test_owner_command_global_authorization_validation_and_alias(manager, monkeypatch):
    import TechVJ.owner_commands as commands
    monkeypatch.setattr(commands, 'role_manager', manager)
    monkeypatch.setattr(commands, '_GOVERNANCE_AVAILABLE', True)
    monkeypatch.setattr(commands, 'OWNER_ID', 999)

    def message(uid, text):
        return SimpleNamespace(from_user=SimpleNamespace(id=uid), text=text, reply=AsyncMock())

    async def run():
        await commands.cmd_setrole(None, message(1, '/setrole global vip_user'))
        assert manager._read_policy() is None
        await commands.cmd_setrole(None, message(999, '/setrole global banned_user'))
        assert manager._read_policy() is None
        await commands.cmd_setrole(None, message(999, '/setrole global normal_user'))
        assert await manager.get_role(123) == UserRole.NORMAL_USER
        await commands.cmd_setrole(None, message(999, '/setrole globale vip_user'))
        assert await manager.get_role(123) == UserRole.VIP_USER
        own = message(999, '/setrole 999 new_user')
        await commands.cmd_setrole(None, own)
        assert await manager.get_role(999) == UserRole.VIP_USER
        own.reply.assert_awaited_once()
        assert manager._read_policy()['overrides'] == {}
    asyncio.run(run())


def test_banlist_uses_same_effective_policy(manager):
    async def run():
        await manager.set_global_role(UserRole.NEW_USER)
        assert await async_db.get_all_banned() == []
        await manager.set_role(123, UserRole.BANNED)
        assert await async_db.get_all_banned() == [{'user_id': 123, 'banned': True}]
        await manager.set_global_role(UserRole.VIP_USER)
        assert await async_db.get_all_banned() == []
    asyncio.run(run())


def test_individual_and_global_writes_are_serialized(manager, monkeypatch):
    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        async def slow_update(*args, **kwargs):
            started.set()
            await release.wait()
        monkeypatch.setattr(async_db, 'update_user', slow_update)
        await manager.set_global_role(UserRole.NEW_USER)
        ban = asyncio.create_task(manager.set_role(123, UserRole.BANNED))
        await started.wait()
        reset = asyncio.create_task(manager.set_global_role(UserRole.NORMAL_USER))
        await asyncio.sleep(0)
        assert not reset.done()
        release.set()
        await asyncio.gather(ban, reset)
        assert await manager.get_role(123) == UserRole.NORMAL_USER
        assert await async_db.is_banned(123) is False
        assert await async_db.get_all_banned() == []
    asyncio.run(run())
