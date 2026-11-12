from __future__ import annotations

import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from wzgram import StopPropagation

from TechVJ import maintenance, save
from core.owner_settings import OwnerSettings


@pytest.mark.parametrize('text', ['/start', '/help', '/login', 'https://t.me/example/1', '12345'])
def test_messages_stop_before_other_handlers(monkeypatch, text):
    monkeypatch.setattr(maintenance, 'is_maintenance', lambda: True)
    msg = NS(from_user=NS(id=123), text=text, reply=AsyncMock(),
             stop_propagation=lambda: (_ for _ in ()).throw(StopPropagation))
    with pytest.raises(StopPropagation):
        asyncio.run(maintenance.maintenance_message(NS(), msg))
    msg.reply.assert_awaited_once()


@pytest.mark.parametrize('text,owner,enabled', [
    ('/start', True, True), ('/start', False, False),
    ('/stop', False, True), ('/cancel', False, True), ('/logout@bot', False, True),
])
def test_owner_off_and_cleanup_commands_remain_accessible(monkeypatch, text, owner, enabled):
    monkeypatch.setattr(maintenance, 'is_maintenance', lambda: enabled)
    msg = NS(from_user=NS(id=maintenance.OWNER_ID if owner else 123), text=text,
             reply=AsyncMock(), stop_propagation=lambda: pytest.fail('blocked'))
    asyncio.run(maintenance.maintenance_message(NS(), msg))
    msg.reply.assert_not_awaited()


def test_callback_is_answered_and_stopped(monkeypatch):
    monkeypatch.setattr(maintenance, 'is_maintenance', lambda: True)
    query = NS(from_user=NS(id=123), answer=AsyncMock(),
               stop_propagation=lambda: (_ for _ in ()).throw(StopPropagation))
    with pytest.raises(StopPropagation):
        asyncio.run(maintenance.maintenance_callback(NS(), query))
    assert query.answer.await_args.kwargs['show_alert'] is True


def test_maintenance_blocks_even_without_governance(monkeypatch):
    monkeypatch.setattr(save, '_GOVERNANCE_AVAILABLE', False)
    monkeypatch.setattr(save, '_is_maintenance', lambda: True)
    queue = AsyncMock()
    monkeypatch.setattr(save, 'check_and_queue', queue)
    msg = NS(from_user=NS(id=123), reply=AsyncMock())
    asyncio.run(save._save_request(NS(), msg, 'https://t.me/example/1'))
    msg.reply.assert_awaited_once()
    queue.assert_not_awaited()


def test_queued_request_is_rechecked_and_released(monkeypatch):
    monkeypatch.setattr(save, '_is_maintenance', lambda: True)
    queue = NS(complete=AsyncMock(return_value=None))
    monkeypatch.setattr(save, 'user_queue', queue)
    handler = AsyncMock()
    monkeypatch.setattr(save, '_process_queued_public', handler)
    bot = NS(send_message=AsyncMock())
    asyncio.run(save._process_queued_item(bot, NS(chat_id=123)))
    bot.send_message.assert_awaited_once()
    handler.assert_not_awaited()
    queue.complete.assert_awaited_once_with(123)


def test_priority_job_rechecks_when_it_actually_starts(monkeypatch):
    from core import priority_queue

    async def run():
        enabled = False
        monkeypatch.setattr(save, '_is_maintenance', lambda: enabled)
        monkeypatch.setattr(save, '_GOVERNANCE_AVAILABLE', True)

        async def enqueue(job):
            nonlocal enabled
            enabled = True  # Owner turns maintenance on while the job waits.
            await job.handler(job.client, job.message, job.parsed_url)
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

        monkeypatch.setattr(priority_queue, 'priority_queue', NS(_started=True, enqueue=enqueue))
        bot = NS(send_message=AsyncMock())
        handler = AsyncMock()
        await save._dispatch_with_priority(bot, NS(), NS(), handler,
                                          NS(user_id=123, target_chat_id=123, task_id='test'))
        handler.assert_not_awaited()
        bot.send_message.assert_awaited_once()
    asyncio.run(run())


def test_durable_flag_is_shared_by_command_and_gate(monkeypatch, tmp_path):
    import core.owner_settings as module
    from TechVJ import owner_commands

    store = OwnerSettings(tmp_path / 'settings.json')
    monkeypatch.setattr(module, 'owner_settings', store)
    store.stage('maintenance', True)
    assert owner_commands.is_maintenance() is True
    assert maintenance.is_maintenance() is True
    restored = OwnerSettings(store.path)
    monkeypatch.setattr(module, 'owner_settings', restored)
    assert save._is_maintenance() is True
    restored.stage('maintenance', False)
    assert maintenance.is_maintenance() is False


def test_owner_command_controls_gate_and_explains_exceptions(monkeypatch, tmp_path):
    import core.owner_settings as module
    from TechVJ import owner_commands

    async def run():
        store = OwnerSettings(tmp_path / 'settings.json')
        monkeypatch.setattr(module, 'owner_settings', store)
        monkeypatch.setattr(owner_commands, 'owner_settings', store)
        msg = NS(from_user=NS(id=owner_commands.OWNER_ID), text='/maintenance on', reply=AsyncMock())
        await owner_commands.cmd_maintenance(NS(), msg)
        assert maintenance.is_maintenance() is True
        assert 'Owner' in msg.reply.await_args.args[0]
        msg.text = '/maintenance off'
        await owner_commands.cmd_maintenance(NS(), msg)
        assert maintenance.is_maintenance() is False
    asyncio.run(run())
