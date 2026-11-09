import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

from TechVJ import usersdata_command as command


def test_verification_requires_working_matching_user_session(monkeypatch):
    async def run():
        closed = []
        @asynccontextmanager
        async def session(value, uid, **kwargs):
            try:
                if value == 'broken':
                    raise RuntimeError('private session details')
                yield NS(get_me=AsyncMock(return_value=NS(id=uid if value == 'valid' else 999,
                    first_name='Ali', last_name='Valiyev', username='ali', is_premium=True)))
            finally:
                closed.append(value)
        monkeypatch.setattr(command, 'create_user_session', session)
        assert await command.verify_user(123, 'broken') is None
        assert await command.verify_user(123, 'mismatch') is None
        user = await command.verify_user(123, 'valid')
        assert user == {'id': 123, 'name': 'Ali Valiyev', 'username': 'ali', 'premium': True}
        assert closed == ['broken', 'mismatch', 'valid']
    asyncio.run(run())


def test_collection_deduplicates_and_excludes_missing_or_invalid_sessions(monkeypatch):
    async def run():
        async def registrations():
            for uid in [1, 1, 2, 3]:
                yield {'chat_id': uid}
        monkeypatch.setattr(command.async_db, 'iter_registered_users', registrations)
        monkeypatch.setattr(command.async_db, 'find_user', AsyncMock(side_effect=[
            {'session': 'valid'}, {'session': 'invalid'}, {}]))
        verifier = AsyncMock(side_effect=[{'id': 1}, None])
        monkeypatch.setattr(command, 'verify_user', verifier)
        users, checked, skipped = await command.collect_users()
        assert users == [{'id': 1}]
        assert (checked, skipped) == (2, 1)
        assert verifier.await_count == 2
    asyncio.run(run())


def test_html_escapes_profile_and_reports_premium(tmp_path):
    path = tmp_path / 'users.html'
    command.render_users([{'id': 123, 'name': '<script>name</script>',
        'username': 'ali', 'premium': True}], path)
    text = path.read_text()
    assert '<script>name</script>' not in text
    assert '&lt;script&gt;' in text
    assert '@ali' in text
    assert '<td>123</td>' in text
    assert '<td>Ha</td>' in text


def test_nonowner_cannot_start_collection(monkeypatch):
    async def run():
        collector = AsyncMock()
        monkeypatch.setattr(command, 'collect_users', collector)
        monkeypatch.setattr(command, 'OWNER_ID', 999)
        message = NS(from_user=NS(id=123), reply=AsyncMock())
        await command.cmd_usersdata(NS(), message)
        collector.assert_not_awaited()
        message.reply.assert_not_awaited()
    asyncio.run(run())
