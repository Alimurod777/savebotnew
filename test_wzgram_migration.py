"""Offline regressions for the native Wzgram migration."""
from __future__ import annotations

import asyncio
import base64
from collections import defaultdict
import inspect
import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import core
import wzgram
import pyrogram
from wzgram import raw, utils
from wzgram.storage import MemoryStorage
from wzgram.types import Chat, Message, ReplyParameters, User


def test_native_module_and_type_identity():
    assert wzgram is pyrogram
    assert Message is pyrogram.types.Message
    assert "wzgram" in wzgram.__fork__.lower()


def test_native_reply_builder_survives_core_import():
    async def run():
        from core.pyrofork_compat import _patch_pyrofork_get_reply_to
        from core.reply_compat import build_reply_kwargs
        original = utils.get_reply_to
        _patch_pyrofork_get_reply_to()
        assert utils.get_reply_to is original
        assert "reply_parameters" in inspect.signature(original).parameters
        kwargs = build_reply_kwargs(42, quote=True, allow_sending_without_reply=True)
        assert isinstance(kwargs["reply_parameters"], ReplyParameters)
        assert kwargs["reply_parameters"].quote is None
        reply = await utils.get_reply_to(SimpleNamespace(), **kwargs, message_thread_id=5)
        assert reply.reply_to_msg_id == 42
        assert reply.top_msg_id == 5
        assert reply.write()
    asyncio.run(run())


def test_absent_raw_vectors_parse_without_network():
    async def run():
        client = SimpleNamespace(message_cache={}, topic_cache=defaultdict(lambda: None),
                                 fetch_topics=False, fetch_replies=False, me=None)
        user = raw.types.User(id=9, first_name="Test")
        assert User._parse(client, user).id == 9
        assert Chat._parse_user_chat(client, raw.types.User(id=10, first_name="Other")).id == 10
        channel = raw.types.Channel(id=123, title="Forum", photo=raw.types.ChatPhotoEmpty(),
                                    date=0, megagroup=True, forum=True, access_hash=1)
        assert Chat._parse_channel_chat(client, channel).id == -1000000000123
        msg = raw.types.Message(id=42, peer_id=raw.types.PeerChannel(channel_id=123),
                                from_id=raw.types.PeerUser(user_id=9), date=1, message="hello")
        result = await Message._parse(client, msg, {9: user}, {123: channel}, replies=0)
        assert result.text == "hello"
        assert result.id == 42
    asyncio.run(run())


def test_legacy_pyrofork_session_and_wzgram_export_roundtrip():
    async def run():
        # Synthetic auth key only: no real account credentials are read.
        legacy = base64.urlsafe_b64encode(struct.pack(">BI?256sQ?", 2, 12345, False, b"x" * 256, 123456789, False)).decode().rstrip("=")
        storage = MemoryStorage("migration-test", session_string=legacy)
        try:
            await storage.open()
            assert await storage.user_id() == 123456789
            assert await storage.auth_key() == b"x" * 256
            exported = await storage.export_session_string()
            second = MemoryStorage("migration-test-second", session_string=exported)
            try:
                await second.open()
                assert await second.user_id() == 123456789
                assert await second.auth_key() == b"x" * 256
            finally:
                await second.close()
        finally:
            await storage.close()
    asyncio.run(run())


def test_opted_in_upload_returns_native_input_file_and_closes_session(tmp_path):
    async def run():
        from core.pyrofork_compat import _flood_safe_save_file
        path = tmp_path / "sample.bin"
        path.write_bytes(b"sample")
        session = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(), invoke=AsyncMock(return_value=True))
        client = SimpleNamespace(save_file_semaphore=asyncio.Semaphore(1),
            me=SimpleNamespace(is_premium=False), rnd_id=lambda: 7,
            storage=SimpleNamespace(dc_id=AsyncMock(return_value=2),
                auth_key=AsyncMock(return_value=b"x" * 256), test_mode=AsyncMock(return_value=False)),
            loop=asyncio.get_running_loop(), name="test")
        with patch("core.pyrofork_compat.PyrogramSession", return_value=session):
            result = await _flood_safe_save_file(client, path)
        assert isinstance(result, raw.types.InputFile)
        assert result.parts == 1
        session.invoke.assert_awaited_once()
        session.stop.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize("storage_kind", ["legacy", "client"])
def test_pyrofork_file_session_migrates_preserving_auth_and_peer_cache(tmp_path, storage_kind):
    import sqlite3
    from wzgram.storage import FileStorage
    from core.wzgram_compat import apply_patches

    if storage_kind == "client":
        from wzgram import Client
        assert type(Client("probe", workdir=tmp_path).storage) is not FileStorage

    def make_storage():
        if storage_kind == "client":
            return Client("legacy", workdir=tmp_path).storage
        return FileStorage("legacy", tmp_path)

    path = tmp_path / "legacy.session"
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE sessions(dc_id INTEGER PRIMARY KEY, api_id INTEGER, test_mode INTEGER,
                auth_key BLOB, date INTEGER NOT NULL, user_id INTEGER, is_bot INTEGER);
            CREATE TABLE peers(id INTEGER PRIMARY KEY, access_hash INTEGER, type TEXT NOT NULL,
                username TEXT, phone_number TEXT, last_update_on INTEGER DEFAULT 0);
            CREATE TABLE usernames(id TEXT PRIMARY KEY, peer_id INTEGER NOT NULL, last_update_on INTEGER DEFAULT 0);
            CREATE TABLE update_state(id INTEGER PRIMARY KEY, pts INTEGER, qts INTEGER, date INTEGER, seq INTEGER);
            CREATE TABLE version(number INTEGER PRIMARY KEY);
            INSERT INTO version VALUES (4);
            INSERT INTO peers VALUES (99, 123, 'user', 'sample_user', NULL, 0);
            INSERT INTO usernames VALUES ('another_alias', 99, 0);
            INSERT INTO update_state VALUES (0, 1234, 0, 0, 1);
        """)
        conn.execute("INSERT INTO sessions VALUES (2, 12345, 0, ?, 0, 123456789, 0)", (b"x" * 256,))

    async def run():
        apply_patches()  # idempotent on repeated startup imports
        storage = make_storage()
        try:
            await storage.open()
            assert await storage.version() == 8
            assert await storage.user_id() == 123456789
            assert await storage.auth_key() == b"x" * 256
            assert await storage.server_address()
            assert await storage.port() == 443
            rows = await (await storage.conn.execute("SELECT id, username FROM usernames")).fetchall()
            assert set(rows) == {(99, 'sample_user'), (99, 'another_alias')}
            assert (await (await storage.conn.execute("SELECT pts FROM update_state WHERE id=0")).fetchone())[0] == 1234
        finally:
            if storage.conn:
                await storage.close()
        # Native format is reopened without another migration or backup.
        storage = make_storage()
        try:
            await storage.open()
            assert await storage.user_id() == 123456789
        finally:
            await storage.close()
    asyncio.run(run())
    backup = tmp_path / "legacy.session.pyrofork-backup"
    assert backup.is_file()
    assert backup.stat().st_mode & 0o777 == 0o600
    with sqlite3.connect(backup) as conn:
        assert conn.execute("SELECT number FROM version").fetchone()[0] == 4


def test_failed_native_storage_open_releases_connection_and_lock(tmp_path):
    from wzgram import Client

    async def run():
        storage = Client("failed-open", workdir=tmp_path).storage
        await storage.open()
        await storage.close()
        with patch.object(type(storage), "update", new=AsyncMock(side_effect=RuntimeError("schema failure"))):
            with pytest.raises(RuntimeError, match="schema failure"):
                await storage.open()
        assert storage.conn is None
        assert storage._lock_fd is None
        # The same object can retry without leaking the previous connection.
        await storage.open()
        try:
            assert await storage.version() == 8
            assert storage.conn is not None
        finally:
            await storage.close()
    asyncio.run(run())
