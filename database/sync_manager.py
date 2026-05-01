"""
database/sync_manager.py - Background MongoDB sync for local-first architecture.

DESIGN:
- All writes go to LocalStorage (SQLite) first
- MongoDB sync happens in background via asyncio.create_task
- Failed Mongo writes queued to persistent file (data/cache/pending_sync.json)
- Periodic retry task (every 60s) retries pending writes
- Startup: Mongo -> Local full sync (one-time)

RULES:
- NEVER block the caller - all Mongo writes are fire-and-forget
- Pending queue survives bot restart (JSON file)
- Queue entries have max_retries=10 - after that, drop and log
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import List, Optional

logger = logging.getLogger(__name__)

_PENDING_FILE = os.path.join("data", "cache", "pending_sync.json")
_RETRY_INTERVAL = 60
_MAX_RETRIES = 10


class _PendingEntry:
    __slots__ = ("collection", "filter_dict", "update_dict", "op", "retries", "created_at")

    def __init__(
        self,
        collection: str,
        filter_dict: dict,
        update_dict: dict,
        op: str = "set",
        retries: int = 0,
        created_at: float = 0.0,
    ):
        self.collection = collection
        self.filter_dict = filter_dict
        self.update_dict = update_dict
        self.op = op
        self.retries = retries
        self.created_at = created_at or time.time()

    def to_dict(self) -> dict:
        return {
            "collection": self.collection,
            "filter": self.filter_dict,
            "update": self.update_dict,
            "op": self.op,
            "retries": self.retries,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "_PendingEntry":
        return cls(
            collection=data["collection"],
            filter_dict=data["filter"],
            update_dict=data["update"],
            op=data.get("op", "set"),
            retries=data.get("retries", 0),
            created_at=data.get("created_at", 0.0),
        )


class SyncManager:
    """Background Mongo sync with persistent pending queue."""

    def __init__(self) -> None:
        self._pending: List[_PendingEntry] = []
        self._lock = asyncio.Lock()
        self._retry_task: Optional[asyncio.Task] = None
        self._load_pending()

    def _load_pending(self) -> None:
        if not os.path.exists(_PENDING_FILE):
            return
        try:
            with open(_PENDING_FILE, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
            self._pending = [_PendingEntry.from_dict(entry) for entry in raw]
            if self._pending:
                logger.info("SyncManager: loaded %d pending sync entries", len(self._pending))
        except Exception as exc:
            logger.warning("SyncManager: failed to load pending file: %s", exc)

    def _save_pending(self) -> None:
        os.makedirs(os.path.dirname(_PENDING_FILE), exist_ok=True)
        try:
            with open(_PENDING_FILE, "w", encoding="utf-8") as handle:
                json.dump([entry.to_dict() for entry in self._pending], handle, ensure_ascii=False)
        except Exception as exc:
            logger.warning("SyncManager: failed to save pending file: %s", exc)

    def enqueue(self, collection: str, filter_dict: dict, update_dict: dict, op: str = "set") -> None:
        """Add a Mongo write to the background queue."""
        self._pending.append(_PendingEntry(collection, filter_dict, update_dict, op=op))
        self._save_pending()

    async def enqueue_and_try(
        self, collection: str, filter_dict: dict, update_dict: dict, op: str = "set"
    ) -> None:
        """Try Mongo write immediately; queue on failure."""
        ok = await self._try_write(collection, filter_dict, update_dict, op=op)
        if not ok:
            self.enqueue(collection, filter_dict, update_dict, op=op)

    def background_sync(
        self, collection: str, filter_dict: dict, update_dict: dict, op: str = "set"
    ) -> None:
        """Schedule a Mongo write as a background task without blocking caller."""
        try:
            asyncio.get_running_loop().create_task(
                self.enqueue_and_try(collection, filter_dict, update_dict, op=op)
            )
        except RuntimeError:
            self.enqueue(collection, filter_dict, update_dict, op=op)

    async def startup_sync(self) -> None:
        """One-time sync from Mongo to Local on bot startup."""
        try:
            from database.async_db import (
                get_banned_users_collection,
                get_sent_albums_collection,
                get_sessions_collection,
            )
            from database.local_storage import LocalStorage, _AIOSQLITE_AVAILABLE

            sessions_coll = get_sessions_collection()
            banned_coll = get_banned_users_collection()
            sent_albums_coll = get_sent_albums_collection()
            if sessions_coll is None and banned_coll is None and sent_albums_coll is None:
                logger.info("SyncManager: MongoDB unavailable - skipping startup sync")
                return

            if not _AIOSQLITE_AVAILABLE:
                logger.info("SyncManager: aiosqlite unavailable - skipping startup sync")
                return

            session_docs = []
            if sessions_coll is not None:
                session_docs = await sessions_coll.find({}).to_list(length=10000)

            synced = 0
            skipped_no_chat_id = 0
            skipped_no_sync_fields = 0
            failed_updates = 0

            for doc in session_docs:
                chat_id = doc.get("chat_id")
                if not chat_id:
                    skipped_no_chat_id += 1
                    continue

                update = {}
                if "session" in doc:
                    update["session"] = doc["session"]
                if "logged_in" in doc:
                    update["logged_in"] = doc["logged_in"]
                if "phone" in doc:
                    update["phone"] = doc["phone"]
                if "role" in doc:
                    update["role"] = doc["role"]

                has_post_limit = bool(doc.get("expecting_post_limit"))
                post_limit_data = doc.get("post_limit_data")
                if not update and not has_post_limit:
                    skipped_no_sync_fields += 1
                    logger.debug(
                        "SyncManager: startup sync skipped chat_id=%s (no synced session fields)",
                        chat_id,
                    )
                    continue

                try:
                    if update:
                        await LocalStorage.update_user(chat_id, update)
                    if has_post_limit:
                        await LocalStorage.set_expecting_post_limit(chat_id, post_limit_data or {})
                    else:
                        await LocalStorage.clear_expecting_post_limit(chat_id)
                    synced += 1
                except Exception as exc:
                    failed_updates += 1
                    logger.debug("SyncManager: startup sync user %s failed: %s", chat_id, exc)

            banned_synced = 0
            if banned_coll is not None:
                banned_docs = await banned_coll.find({"banned": True}).to_list(length=10000)
                for doc in banned_docs:
                    user_id = doc.get("user_id")
                    if not user_id:
                        continue
                    try:
                        await LocalStorage.ban_user(user_id, doc.get("banned_by") or 0)
                        banned_synced += 1
                    except Exception as exc:
                        logger.debug(
                            "SyncManager: startup sync banned user %s failed: %s",
                            user_id,
                            exc,
                        )

            sent_albums_synced = 0
            if sent_albums_coll is not None:
                sent_album_docs = await sent_albums_coll.find({}).to_list(length=10000)
                for doc in sent_album_docs:
                    user_id = doc.get("user_id")
                    media_group_id = doc.get("media_group_id")
                    if not user_id or not media_group_id:
                        continue
                    try:
                        await LocalStorage.mark_album_sent(
                            user_id,
                            media_group_id,
                            doc.get("source_chat_id"),
                        )
                        sent_albums_synced += 1
                    except Exception as exc:
                        logger.debug(
                            "SyncManager: startup sync sent album %s/%s failed: %s",
                            user_id,
                            media_group_id,
                            exc,
                        )

            if not session_docs and banned_synced == 0 and sent_albums_synced == 0:
                logger.info("SyncManager: no Mongo docs to sync")

            logger.info(
                "SyncManager: startup sync complete - %d/%d users synced",
                synced,
                len(session_docs),
            )
            logger.info(
                "SyncManager: startup sync details - skipped_no_chat_id=%d, "
                "skipped_no_sync_fields=%d, failed_updates=%d, banned_synced=%d, "
                "sent_albums_synced=%d",
                skipped_no_chat_id,
                skipped_no_sync_fields,
                failed_updates,
                banned_synced,
                sent_albums_synced,
            )
        except Exception as exc:
            logger.warning("SyncManager: startup sync failed (non-fatal): %s", exc)

        self._start_retry_loop()
        if self._pending:
            await self._flush_pending()

    def _start_retry_loop(self) -> None:
        if self._retry_task is None or self._retry_task.done():
            try:
                self._retry_task = asyncio.get_running_loop().create_task(self._retry_loop())
            except RuntimeError:
                pass

    async def _retry_loop(self) -> None:
        while True:
            await asyncio.sleep(_RETRY_INTERVAL)
            if self._pending:
                await self._flush_pending()

    async def _flush_pending(self) -> None:
        async with self._lock:
            remaining: List[_PendingEntry] = []
            for entry in self._pending:
                ok = await self._try_write(
                    entry.collection,
                    entry.filter_dict,
                    entry.update_dict,
                    op=entry.op,
                )
                if ok:
                    continue
                entry.retries += 1
                if entry.retries < _MAX_RETRIES:
                    remaining.append(entry)
                else:
                    logger.warning(
                        "SyncManager: dropping entry after %d retries: %s %s op=%s",
                        _MAX_RETRIES,
                        entry.collection,
                        entry.filter_dict,
                        entry.op,
                    )
            self._pending = remaining
            self._save_pending()

    @staticmethod
    async def _try_write(
        collection: str, filter_dict: dict, update_dict: dict, op: str = "set"
    ) -> bool:
        try:
            from database.async_db import (
                get_banned_users_collection,
                get_sent_albums_collection,
                get_sessions_collection,
            )

            if collection == "sessions":
                coll = get_sessions_collection()
            elif collection == "banned_users":
                coll = get_banned_users_collection()
            elif collection == "sent_albums":
                coll = get_sent_albums_collection()
            else:
                return True

            if coll is None:
                return False

            if op == "set":
                await coll.update_one(filter_dict, {"$set": update_dict}, upsert=True)
            elif op == "unset":
                await coll.update_one(filter_dict, {"$unset": update_dict}, upsert=False)
            elif op == "delete_one":
                await coll.delete_one(filter_dict)
            elif op == "delete_many":
                await coll.delete_many(filter_dict)
            else:
                logger.warning("SyncManager: unknown sync op=%s", op)
                return False

            return True
        except Exception as exc:
            logger.debug("SyncManager: Mongo write failed: %s", exc)
            return False


sync_manager = SyncManager()
