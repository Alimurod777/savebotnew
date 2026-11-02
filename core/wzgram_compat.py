"""Narrow compatibility fixes for the pinned Wzgram 3.1.6 parser."""
from __future__ import annotations

import asyncio
import functools
import inspect
import sqlite3
from pathlib import Path

from wzgram import __version__
from wzgram.types import Chat, Message, User
from wzgram.storage import FileStorage
from wzgram.storage.sqlite_storage import SQLiteStorage


def _patch_optional_vectors(owner, method_name: str, argument: str, fields, *, asynchronous=False):
    original = getattr(owner, method_name)
    if getattr(original, "_savebot_optional_vectors", False):
        return
    signature = inspect.signature(original)

    def normalize(args, kwargs):
        value = signature.bind_partial(*args, **kwargs).arguments.get(argument)
        for field in fields:
            if hasattr(value, field) and getattr(value, field) is None:
                setattr(value, field, [])

    if asynchronous:
        @functools.wraps(original)
        async def wrapped(*args, **kwargs):
            normalize(args, kwargs)
            return await original(*args, **kwargs)
    else:
        @functools.wraps(original)
        def wrapped(*args, **kwargs):
            normalize(args, kwargs)
            return original(*args, **kwargs)

    wrapped._savebot_optional_vectors = True
    setattr(owner, method_name, staticmethod(wrapped))


def apply_patches() -> None:
    # Optional TL vectors arrive as None when absent. This release iterates
    # them without an `or []`, crashing ordinary messages/users/channels.
    # Retest before changing the pinned release; do not patch future versions.
    if __version__ != "3.1.6":
        return
    _patch_legacy_file_storage()
    _patch_optional_vectors(User, "_parse", "user", ("usernames", "restriction_reason"))
    _patch_optional_vectors(Chat, "_parse_user_chat", "user", ("usernames", "restriction_reason"))
    _patch_optional_vectors(Chat, "_parse_chat_chat", "chat", ("usernames",))
    _patch_optional_vectors(Chat, "_parse_channel_chat", "channel", ("usernames", "restriction_reason"))
    _patch_optional_vectors(Message, "_parse_message", "message", ("entities", "restriction_reason"), asynchronous=True)


def _migrate_pyrofork_database(path: Path) -> None:
    if not path.is_file():
        return
    with sqlite3.connect(str(path), timeout=5) as conn:
        peers = {row[1] for row in conn.execute("PRAGMA table_info(peers)")}
        sessions = {row[1] for row in conn.execute("PRAGMA table_info(sessions)")}
        usernames = {row[1] for row in conn.execute("PRAGMA table_info(usernames)")}
        # Pyrofork v4 has update_state already, plus a different usernames table.
        # Wzgram's v4 upgrader assumes neither, and would fail at startup.
        if not ({"username", "phone_number"} <= peers and "server_address" not in sessions
                and "peer_id" in usernames):
            return
        version = conn.execute("SELECT number FROM version").fetchone()[0]
        if version != 4:
            raise RuntimeError("Unsupported legacy Telegram session schema version")
        backup_path = path.with_name(path.name + ".pyrofork-backup")
        if not backup_path.exists():
            with sqlite3.connect(str(backup_path)) as backup:
                conn.backup(backup)
            backup_path.chmod(0o600)
        conn.execute("BEGIN IMMEDIATE")
        try:
            cached_usernames = conn.execute("SELECT peer_id, id FROM usernames").fetchall()
            cached_usernames += conn.execute("SELECT id, username FROM peers WHERE username IS NOT NULL").fetchall()
            conn.execute("DROP TABLE usernames")
            conn.execute("CREATE TABLE usernames (id INTEGER, username TEXT, FOREIGN KEY(id) REFERENCES peers(id))")
            conn.execute("CREATE INDEX idx_usernames_id ON usernames(id)")
            conn.execute("CREATE INDEX idx_usernames_username ON usernames(username)")
            conn.executemany("INSERT INTO usernames(id, username) VALUES (?, ?)",
                             list(set(cached_usernames)))
            # Both update_state and Wzgram's new username indices now exist.
            # Native migrations 6->8 add server_address/port and DC defaults.
            conn.execute("UPDATE version SET number=6")
            conn.commit()
        except BaseException:
            conn.rollback()
            raise


def _patch_legacy_file_storage() -> None:
    # Client uses SQLiteStorage directly in this release; FileStorage is only
    # a compatibility class. Cover both entry points, including memory sessions.
    for storage_type in (SQLiteStorage, FileStorage):
        _patch_storage_open(storage_type)


def _patch_storage_open(storage_type) -> None:
    original = storage_type.open
    if getattr(original, "_savebot_legacy_storage", False):
        return

    @functools.wraps(original)
    async def open_storage(self):
        try:
            if isinstance(self.database, Path):
                await asyncio.to_thread(_migrate_pyrofork_database, self.database)
            return await original(self)
        except BaseException:
            # A failed native open leaves an aiosqlite thread and file lock
            # behind, although Client.is_connected is still False. Release
            # both before Client retries opening this same storage instance.
            try:
                await self.close()
            except Exception:
                pass
            raise

    open_storage._savebot_legacy_storage = True
    storage_type.open = open_storage
