"""
database/local_storage.py - SQLite fallback when MongoDB is unavailable.

DESIGN:
- Drop-in replacement for MongoDB operations in async_db.py
- Uses aiosqlite for async SQLite operations
- Located in <base_path>/local_db/bot_storage.db
- Thread-safe WAL mode for Colab/Kaggle compatibility
- API matches AsyncDatabase method signatures

RULES:
- Never import motor or pymongo
- Auto-creates tables on first use
- JSON serialization for complex fields
- Colab/Kaggle safe (WAL mode, no file locking issues)
"""

import os
import json
import logging
import asyncio
from typing import Optional, List, Any, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

# Try to import aiosqlite
try:
    import aiosqlite
    _AIOSQLITE_AVAILABLE = True
except ImportError:
    _AIOSQLITE_AVAILABLE = False
    logger.warning("aiosqlite not available — local SQLite fallback disabled")

# Database path
try:
    from core.environment import get_local_db_path
    _DB_DIR = get_local_db_path()
except ImportError:
    _DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "local_db")
    os.makedirs(_DB_DIR, exist_ok=True)

_DB_PATH = os.path.join(_DB_DIR, "bot_storage.db")

# Lock for initialization
_init_lock = None
_initialized = False


def _get_init_lock():
    global _init_lock
    if _init_lock is None:
        _init_lock = asyncio.Lock()
    return _init_lock


async def _get_db() -> Optional['aiosqlite.Connection']:
    """Get an aiosqlite connection with WAL mode enabled."""
    if not _AIOSQLITE_AVAILABLE:
        return None
    try:
        db = await aiosqlite.connect(_DB_PATH)
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        db.row_factory = aiosqlite.Row
        return db
    except Exception as e:
        logger.warning(f"SQLite connection failed: {e}")
        return None


async def init_tables():
    """Create tables if they don't exist."""
    global _initialized
    if _initialized:
        return

    async with _get_init_lock():
        if _initialized:
            return

        db = await _get_db()
        if db is None:
            return

        try:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS sessions (
                    chat_id INTEGER PRIMARY KEY,
                    logged_in INTEGER DEFAULT 0,
                    session_string TEXT,
                    phone TEXT,
                    role TEXT DEFAULT 'new_user',
                    data_json TEXT DEFAULT '{}',
                    expecting_post_limit INTEGER DEFAULT 0,
                    post_limit_data_json TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );
                
                CREATE TABLE IF NOT EXISTS banned_users (
                    user_id INTEGER PRIMARY KEY,
                    banned_by INTEGER,
                    banned INTEGER DEFAULT 1,
                    created_at TEXT
                );
                
                CREATE TABLE IF NOT EXISTS sent_albums (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    media_group_id TEXT NOT NULL,
                    source_chat_id INTEGER,
                    sent_at TEXT,
                    UNIQUE(user_id, media_group_id)
                );

                CREATE TABLE IF NOT EXISTS failed_downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    user_id INTEGER,
                    chat_id TEXT,
                    post_id INTEGER,
                    url_type TEXT,
                    topic_id INTEGER,
                    thread_id INTEGER,
                    stage TEXT,
                    reason TEXT,
                    error TEXT,
                    retry_url TEXT,
                    retry_count INTEGER DEFAULT 0,
                    last_retry_at TEXT,
                    details_json TEXT DEFAULT '{}'
                );
                
                CREATE INDEX IF NOT EXISTS idx_sent_albums_user 
                    ON sent_albums(user_id);
                CREATE INDEX IF NOT EXISTS idx_sessions_logged_in 
                    ON sessions(logged_in);
                CREATE INDEX IF NOT EXISTS idx_failed_downloads_created
                    ON failed_downloads(created_at);
                CREATE INDEX IF NOT EXISTS idx_failed_downloads_user
                    ON failed_downloads(user_id);
                CREATE INDEX IF NOT EXISTS idx_failed_downloads_v2_created
                    ON failed_downloads(created_at);
                CREATE INDEX IF NOT EXISTS idx_failed_downloads_v2_user
                    ON failed_downloads(user_id);
            """)
            await db.commit()

            try:
                await db.execute(
                    """INSERT OR IGNORE INTO failed_downloads
                       (id, created_at, user_id, chat_id, post_id, url_type, topic_id,
                        thread_id, stage, reason, error, retry_url, retry_count,
                        last_retry_at, details_json)
                       SELECT id, created_at, user_id, chat_id, post_id, url_type,
                              topic_id, thread_id, stage, reason, error, retry_url,
                              retry_count, last_retry_at, details_json
                       FROM failed_downloads_log"""
                )
                await db.commit()
            except Exception:
                pass

            # Migration: add role column if missing (for existing DBs)
            try:
                await db.execute("SELECT role FROM sessions LIMIT 1")
            except Exception:
                try:
                    await db.execute("ALTER TABLE sessions ADD COLUMN role TEXT DEFAULT 'new_user'")
                    await db.commit()
                    logger.info("SQLite: added 'role' column to sessions table")
                except Exception:
                    pass  # Column might already exist in some edge cases

            _initialized = True
            logger.info(f"SQLite local storage initialized: {_DB_PATH}")
        except Exception as e:
            logger.error(f"SQLite table creation failed: {e}")
        finally:
            await db.close()


class LocalStorage:
    """SQLite-based local storage — API-compatible with AsyncDatabase."""
    
    # ==================== Session Operations ====================
    
    @staticmethod
    async def find_user(chat_id: int) -> Optional[dict]:
        await init_tables()
        db = await _get_db()
        if db is None:
            return None
        try:
            async with db.execute(
                "SELECT * FROM sessions WHERE chat_id = ?", (chat_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    result = dict(row)
                    # Merge JSON data
                    if result.get('data_json'):
                        try:
                            extra = json.loads(result['data_json'])
                            result.update(extra)
                        except json.JSONDecodeError:
                            pass
                    result['logged_in'] = bool(result.get('logged_in'))
                    result['session'] = result.pop('session_string', None)
                    return result
                return None
        except Exception as e:
            logger.warning(f"SQLite find_user failed: {e}")
            return None
        finally:
            await db.close()
    
    @staticmethod
    async def get_all_logged_in_users() -> list:
        await init_tables()
        db = await _get_db()
        if db is None:
            return []
        try:
            async with db.execute(
                "SELECT * FROM sessions WHERE logged_in = 1"
            ) as cursor:
                rows = await cursor.fetchall()
                results = []
                for row in rows:
                    r = dict(row)
                    r['logged_in'] = True
                    r['session'] = r.pop('session_string', None)
                    results.append(r)
                return results
        except Exception as e:
            logger.warning(f"SQLite get_all_logged_in_users failed: {e}")
            return []
        finally:
            await db.close()
    
    @staticmethod
    async def count_logged_in_users() -> int:
        await init_tables()
        db = await _get_db()
        if db is None:
            return 0
        try:
            async with db.execute(
                "SELECT COUNT(*) FROM sessions WHERE logged_in = 1"
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.warning(f"SQLite count_logged_in_users failed: {e}")
            return 0
        finally:
            await db.close()
    
    @staticmethod
    async def insert_user(chat_id: int) -> Optional[dict]:
        await init_tables()
        db = await _get_db()
        if db is None:
            return None
        try:
            now = datetime.utcnow().isoformat()
            await db.execute(
                """INSERT OR IGNORE INTO sessions (chat_id, logged_in, created_at, updated_at)
                   VALUES (?, 0, ?, ?)""",
                (chat_id, now, now)
            )
            await db.commit()
            return await LocalStorage.find_user(chat_id)
        except Exception as e:
            logger.warning(f"SQLite insert_user failed: {e}")
            return None
        finally:
            await db.close()
    
    @staticmethod
    async def update_user(chat_id: int, data: dict = None, **kwargs):
        """Update user — accepts dict and/or kwargs (matches async_db signature)."""
        update_data = {}
        if data and isinstance(data, dict):
            update_data.update(data)
        if kwargs:
            update_data.update(kwargs)
        if not update_data:
            return

        await init_tables()
        db = await _get_db()
        if db is None:
            return
        try:
            now = datetime.utcnow().isoformat()

            # Track explicit field presence so `None` can clear a column.
            has_session_string = 'session' in update_data or 'session_string' in update_data
            has_logged_in = 'logged_in' in update_data
            has_phone = 'phone' in update_data
            has_role = 'role' in update_data

            # Map known fields to columns
            session_string = update_data.pop('session', update_data.pop('session_string', None))
            logged_in = update_data.pop('logged_in', None)
            phone = update_data.pop('phone', None)
            role = update_data.pop('role', None)

            # Store remaining fields as JSON
            existing = await LocalStorage.find_user(chat_id)
            existing_data = {}
            if existing and existing.get('data_json'):
                try:
                    existing_data = json.loads(existing.get('data_json', '{}'))
                except (json.JSONDecodeError, TypeError):
                    pass
            existing_data.update(update_data)

            await db.execute(
                """INSERT INTO sessions (chat_id, session_string, logged_in, phone, role, data_json, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(chat_id) DO UPDATE SET
                       session_string = CASE WHEN ? THEN ? ELSE session_string END,
                       logged_in = CASE WHEN ? THEN ? ELSE logged_in END,
                       phone = CASE WHEN ? THEN ? ELSE phone END,
                       role = CASE WHEN ? THEN ? ELSE role END,
                       data_json = ?,
                       updated_at = ?""",
                (
                    chat_id, session_string,
                    1 if logged_in else (0 if logged_in is not None else None),
                    phone, role, json.dumps(existing_data), now,
                    # ON CONFLICT params:
                    1 if has_session_string else 0, session_string,
                    1 if has_logged_in else 0, 1 if logged_in else (0 if logged_in is not None else None),
                    1 if has_phone else 0, phone,
                    1 if has_role else 0, role,
                    json.dumps(existing_data), now
                )
            )
            await db.commit()
        except Exception as e:
            logger.warning(f"SQLite update_user failed: {e}")
        finally:
            await db.close()
    
    @staticmethod
    async def update_user_by_id(doc_id, data: dict):
        """SQLite doesn't use MongoDB _id — fall back to chat_id if present."""
        if isinstance(doc_id, int):
            await LocalStorage.update_user(doc_id, data)
    
    @staticmethod
    async def unset_user_fields(chat_id: int, fields: list):
        await init_tables()
        db = await _get_db()
        if db is None:
            return
        try:
            column_fields = {'session_string', 'logged_in', 'phone',
                           'expecting_post_limit', 'post_limit_data_json'}
            
            updates = []
            for field in fields:
                mapped = 'session_string' if field == 'session' else field
                if mapped in column_fields:
                    updates.append(f"{mapped} = NULL")
            
            if updates:
                sql = f"UPDATE sessions SET {', '.join(updates)} WHERE chat_id = ?"
                await db.execute(sql, (chat_id,))
                await db.commit()
        except Exception as e:
            logger.warning(f"SQLite unset_user_fields failed: {e}")
        finally:
            await db.close()

    # ==================== Ban System Operations ====================
    
    @staticmethod
    async def ban_user(user_id: int, banned_by: int):
        await init_tables()
        db = await _get_db()
        if db is None:
            return
        try:
            now = datetime.utcnow().isoformat()
            await db.execute(
                """INSERT INTO banned_users (user_id, banned_by, banned, created_at)
                   VALUES (?, ?, 1, ?)
                   ON CONFLICT(user_id) DO UPDATE SET 
                       banned_by = ?, banned = 1""",
                (user_id, banned_by, now, banned_by)
            )
            await db.commit()
        except Exception as e:
            logger.warning(f"SQLite ban_user failed: {e}")
        finally:
            await db.close()
    
    @staticmethod
    async def unban_user(user_id: int):
        await init_tables()
        db = await _get_db()
        if db is None:
            return
        try:
            await db.execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
            await db.commit()
        except Exception as e:
            logger.warning(f"SQLite unban_user failed: {e}")
        finally:
            await db.close()
    
    @staticmethod
    async def is_banned(user_id: int) -> bool:
        await init_tables()
        db = await _get_db()
        if db is None:
            return False
        try:
            async with db.execute(
                "SELECT 1 FROM banned_users WHERE user_id = ? AND banned = 1",
                (user_id,)
            ) as cursor:
                return await cursor.fetchone() is not None
        except Exception as e:
            logger.warning(f"SQLite is_banned failed: {e}")
            return False
        finally:
            await db.close()
    
    @staticmethod
    async def get_all_banned() -> list:
        await init_tables()
        db = await _get_db()
        if db is None:
            return []
        try:
            async with db.execute(
                "SELECT * FROM banned_users WHERE banned = 1"
            ) as cursor:
                return [dict(row) for row in await cursor.fetchall()]
        except Exception as e:
            logger.warning(f"SQLite get_all_banned failed: {e}")
            return []
        finally:
            await db.close()
    
    # ==================== Task State Operations ====================
    
    @staticmethod
    async def set_expecting_post_limit(chat_id: int, data: dict):
        await init_tables()
        db = await _get_db()
        if db is None:
            return
        try:
            await db.execute(
                """UPDATE sessions SET 
                       expecting_post_limit = 1,
                       post_limit_data_json = ?
                   WHERE chat_id = ?""",
                (json.dumps(data), chat_id)
            )
            await db.commit()
        except Exception as e:
            logger.warning(f"SQLite set_expecting_post_limit failed: {e}")
        finally:
            await db.close()
    
    @staticmethod
    async def clear_expecting_post_limit(chat_id: int):
        await init_tables()
        db = await _get_db()
        if db is None:
            return
        try:
            await db.execute(
                """UPDATE sessions SET 
                       expecting_post_limit = 0,
                       post_limit_data_json = NULL
                   WHERE chat_id = ?""",
                (chat_id,)
            )
            await db.commit()
        except Exception as e:
            logger.warning(f"SQLite clear_expecting_post_limit failed: {e}")
        finally:
            await db.close()
    
    @staticmethod
    async def get_expecting_post_limit(chat_id: int) -> Optional[dict]:
        await init_tables()
        db = await _get_db()
        if db is None:
            return None
        try:
            async with db.execute(
                "SELECT * FROM sessions WHERE chat_id = ? AND expecting_post_limit = 1",
                (chat_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    result = dict(row)
                    if result.get('post_limit_data_json'):
                        try:
                            result['post_limit_data'] = json.loads(
                                result['post_limit_data_json']
                            )
                        except json.JSONDecodeError:
                            pass
                    result['expecting_post_limit'] = True
                    return result
                return None
        except Exception as e:
            logger.warning(f"SQLite get_expecting_post_limit failed: {e}")
            return None
        finally:
            await db.close()
    
    # ==================== Sent Albums Tracking ====================
    
    @staticmethod
    async def is_album_sent(user_id: int, media_group_id: str) -> bool:
        await init_tables()
        db = await _get_db()
        if db is None:
            return False
        try:
            async with db.execute(
                "SELECT 1 FROM sent_albums WHERE user_id = ? AND media_group_id = ?",
                (user_id, media_group_id)
            ) as cursor:
                return await cursor.fetchone() is not None
        except Exception as e:
            logger.warning(f"SQLite is_album_sent failed: {e}")
            return False
        finally:
            await db.close()
    
    @staticmethod
    async def mark_album_sent(user_id: int, media_group_id: str, 
                              source_chat_id: int = None):
        await init_tables()
        db = await _get_db()
        if db is None:
            return
        try:
            now = datetime.utcnow().isoformat()
            await db.execute(
                """INSERT OR REPLACE INTO sent_albums 
                   (user_id, media_group_id, source_chat_id, sent_at)
                   VALUES (?, ?, ?, ?)""",
                (user_id, media_group_id, source_chat_id, now)
            )
            await db.commit()
        except Exception as e:
            logger.warning(f"SQLite mark_album_sent failed: {e}")
        finally:
            await db.close()
    
    @staticmethod
    async def get_user_sent_albums(user_id: int) -> list:
        await init_tables()
        db = await _get_db()
        if db is None:
            return []
        try:
            async with db.execute(
                "SELECT media_group_id FROM sent_albums WHERE user_id = ?",
                (user_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
        except Exception as e:
            logger.warning(f"SQLite get_user_sent_albums failed: {e}")
            return []
        finally:
            await db.close()
    
    @staticmethod
    async def clear_user_sent_albums(user_id: int):
        await init_tables()
        db = await _get_db()
        if db is None:
            return
        try:
            await db.execute(
                "DELETE FROM sent_albums WHERE user_id = ?", (user_id,)
            )
            await db.commit()
        except Exception as e:
            logger.warning(f"SQLite clear_user_sent_albums failed: {e}")
        finally:
            await db.close()

    # ==================== Failed Download Logs ====================

    @staticmethod
    async def log_failed_download(
        *,
        user_id: Optional[int],
        chat_id: Any,
        post_id: Optional[int],
        url_type: Optional[str] = None,
        topic_id: Optional[int] = None,
        thread_id: Optional[int] = None,
        stage: Optional[str] = None,
        reason: Optional[str] = None,
        error: Optional[str] = None,
        retry_url: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        await init_tables()
        db = await _get_db()
        if db is None:
            return None
        try:
            now = datetime.utcnow().isoformat()
            details_json = json.dumps(details or {}, ensure_ascii=False, default=str)
            cursor = await db.execute(
                """INSERT INTO failed_downloads
                   (created_at, user_id, chat_id, post_id, url_type, topic_id,
                    thread_id, stage, reason, error, retry_url, details_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    now,
                    user_id,
                    str(chat_id) if chat_id is not None else None,
                    post_id,
                    url_type,
                    topic_id,
                    thread_id,
                    stage,
                    reason,
                    error,
                    retry_url,
                    details_json,
                ),
            )
            log_id = cursor.lastrowid
            await db.execute(
                """DELETE FROM failed_downloads
                   WHERE id NOT IN (
                       SELECT id FROM failed_downloads
                       ORDER BY id DESC LIMIT 1000
                   )"""
            )
            await db.commit()
            return int(log_id) if log_id is not None else None
        except Exception as e:
            logger.warning(f"SQLite log_failed_download failed: {e}")
            return None
        finally:
            await db.close()

    @staticmethod
    async def get_failed_downloads(limit: int = 10) -> list:
        await init_tables()
        db = await _get_db()
        if db is None:
            return []
        try:
            safe_limit = max(1, min(int(limit), 50))
            async with db.execute(
                """SELECT * FROM failed_downloads
                   ORDER BY id DESC LIMIT ?""",
                (safe_limit,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.warning(f"SQLite get_failed_downloads failed: {e}")
            return []
        finally:
            await db.close()

    @staticmethod
    async def get_failed_download(log_id: int) -> Optional[dict]:
        await init_tables()
        db = await _get_db()
        if db is None:
            return None
        try:
            async with db.execute(
                "SELECT * FROM failed_downloads WHERE id = ?",
                (int(log_id),),
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.warning(f"SQLite get_failed_download failed: {e}")
            return None
        finally:
            await db.close()

    @staticmethod
    async def mark_failed_download_retry(log_id: int) -> None:
        await init_tables()
        db = await _get_db()
        if db is None:
            return
        try:
            now = datetime.utcnow().isoformat()
            await db.execute(
                """UPDATE failed_downloads
                   SET retry_count = COALESCE(retry_count, 0) + 1,
                       last_retry_at = ?
                   WHERE id = ?""",
                (now, int(log_id)),
            )
            await db.commit()
        except Exception as e:
            logger.warning(f"SQLite mark_failed_download_retry failed: {e}")
        finally:
            await db.close()


# Export availability flag
def is_local_storage_available() -> bool:
    return _AIOSQLITE_AVAILABLE
