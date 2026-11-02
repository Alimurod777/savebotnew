"""Persistent owner policy for archiving the bot in connected user accounts."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class BotArchiveSettings:
    def __init__(self, path: Optional[Path] = None):
        self.path = path or Path('data/bot_archive.json')
        self._enabled: Optional[bool] = None
        self.change_lock = asyncio.Lock()
        self.operation_lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        if self._enabled is None:
            self._enabled = True  # Preserve the existing default.
            try:
                data = json.loads(self.path.read_text(encoding='utf-8'))
                if type(data.get('enabled')) is bool:
                    self._enabled = data['enabled']
            except FileNotFoundError:
                pass
            except (OSError, ValueError, AttributeError):
                logger.warning('Cannot read bot archive policy; using default ON')
        return self._enabled

    async def set_enabled(self, enabled: bool) -> None:
        # Wait for any previous archive RPC before changing its direction.
        async with self.operation_lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix='.bot_archive-', dir=str(self.path.parent))
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as output:
                    json.dump({'enabled': enabled}, output)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(name, self.path)
            finally:
                if os.path.exists(name):
                    os.unlink(name)
            self._enabled = enabled


bot_archive_settings = BotArchiveSettings()


async def apply_bot_archive(acc, bot_peer) -> bool:
    """Apply current policy using a user session; never send messages here."""
    try:
        async with bot_archive_settings.operation_lock:
            action = acc.archive_chats if bot_archive_settings.enabled else acc.unarchive_chats
            result = await asyncio.wait_for(action([bot_peer]), timeout=10)
            return result is not False
    except Exception as error:
        # Avoid exception text: session or account details need not enter logs.
        logger.debug('Bot archive update failed: %s', type(error).__name__)
        return False


async def _collect_archive_sessions() -> Tuple[Dict[str, int], int]:
    """Collect configured user/pool sessions; deduplicate without logging secrets."""
    sessions: Dict[str, int] = {}
    source_errors = 0

    def add(value, user_id=0):
        if isinstance(value, str) and value.strip():
            sessions.setdefault(value.strip(), int(user_id or 0))

    try:
        from database.async_db import async_db
        for row in await async_db.get_all_logged_in_users():
            add(row.get('session'), row.get('chat_id'))
    except Exception:
        source_errors += 1
    try:
        from core.session_manager import session_manager
        for record in session_manager.registry.get_all():
            add(record.session_string, record.owner_user_id)
    except Exception:
        source_errors += 1
    try:
        from core.premium_session_store import get_all_sessions
        for entry in await get_all_sessions():
            add(entry.session_string, entry.user_id)
        from core.premium_logic import get_system_session
        entry = get_system_session()
        if entry:
            add(entry.session_string, entry.user_id)
    except Exception:
        source_errors += 1

    # Login's text-file fallback is used when the DB was unavailable.
    for path in Path('sessions').glob('user_*.session'):
        try:
            uid = int(path.stem.removeprefix('user_'))
            # SQLite sessions are not session-string fallback files.
            with path.open('rb') as handle:
                raw = handle.read(4096)
            if not raw.startswith(b'SQLite format 3'):
                add(raw.decode('utf-8').strip(), uid)
        except (OSError, ValueError, UnicodeError):
            source_errors += 1
    return sessions, source_errors


async def sync_bot_archive(bot_username: str) -> dict:
    """Update existing sessions sequentially, reusing connected upload clients."""
    from TechVJ.session_handler import create_user_session
    from core.user_upload_worker import worker_registry

    sessions, source_errors = await _collect_archive_sessions()
    live: Dict[str, object] = {}
    for session, uid, acc in await worker_registry.archive_clients_snapshot():
        sessions.setdefault(session, uid)
        live[session] = acc

    result = {'total': len(sessions), 'updated': 0, 'failed': 0, 'source_errors': source_errors}
    uname = bot_username.lstrip('@')

    async def update(acc):
        chat = await asyncio.wait_for(acc.get_chat(uname), timeout=10)
        return await apply_bot_archive(acc, chat.id)

    for session, uid in sessions.items():
        try:
            if session in live:
                ok = await update(live[session])
            else:
                async def connect_and_update():
                    async with create_user_session(session, uid, timeout=10, validate=False) as acc:
                        return await update(acc)
                ok = await asyncio.wait_for(connect_and_update(), timeout=30)
            result['updated' if ok else 'failed'] += 1
        except Exception as error:
            result['failed'] += 1
            logger.debug('Bot archive session sync failed: %s', type(error).__name__)
    return result
