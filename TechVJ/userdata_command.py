"""
TechVJ/userdata_command.py — Owner-only Telegram Account Inventory.

Commands:
  /userdata <user_id>           — Start inventory of target user's account
  /userdata_status <job_id>     — Check job status
  /userdata_resume <job_id>     — Resume interrupted job
  /userdata_cancel <job_id>     — Cancel active job

All commands are owner-only. Progress is reported to the owner every ~5 seconds.
The final report is a UTF-8 TXT file delivered as a document.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from pyrogram import Client, filters
from pyrogram.enums import ChatType, ParseMode
from pyrogram.errors import (
    ChannelInvalid,
    ChannelPrivate,
    ChatAdminRequired,
    FloodWait,
    PeerIdInvalid,
    RPCError,
)
from pyrogram.raw import functions, types
from pyrogram.types import Message

from config import OWNER_ID
from core.retry_utils import (
    calculate_backoff,
    get_floodwait_seconds,
    is_floodwait_error,
    is_transient_error,
)
from database.async_db import async_db

logger = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────────────

PROGRESS_INTERVAL = 5.0          # seconds between progress edits
MAX_CONCURRENT_INSPECTIONS = 3   # parallel get_chat / member-count calls
DIALOG_PAGE_SIZE = 100           # Pyrogram default page size for get_dialogs
TOPIC_PAGE_SIZE = 100            # MTProto max for GetForumTopics
MAX_RETRY_ATTEMPTS = 3           # per-operation retries
CHECKPOINT_BATCH_SIZE = 25       # persist after this many completed dialogs
TEMP_DIR = "downloads/temp"

# FloodWait caps
MAX_FLOODWAIT = 120              # seconds — do not honour waits longer than this
MAX_TOPIC_FLOODWAIT = 60

# Job DB path
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_JOB_DB_DIR = os.path.join(_BASE_DIR, "local_db")
_JOB_DB_PATH = os.path.join(_JOB_DB_DIR, "userdata_jobs.db")

# ─── aiosqlite lazy import ──────────────────────────────────────────────────

try:
    import aiosqlite
    _AIOSQLITE_AVAILABLE = True
except ImportError:
    _AIOSQLITE_AVAILABLE = False
    logger.warning("aiosqlite not available — userdata checkpoint persistence disabled")


# ═══════════════════════════════════════════════════════════════════════════
# Data Models
# ═══════════════════════════════════════════════════════════════════════════

class JobStatus(str, Enum):
    PENDING = "PENDING"
    SCANNING = "SCANNING"
    INSPECTING = "INSPECTING"
    GENERATING = "GENERATING"
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class DialogCategory(str, Enum):
    CHANNEL = "channel"
    GROUP = "group"
    BOT = "bot"
    PRIVATE = "private"
    UNKNOWN = "unknown"


class InspectionStatus(str, Enum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    PENDING = "PENDING"


@dataclass
class TopicRecord:
    topic_id: int
    title: str
    top_message: int = 0
    closed: bool = False
    hidden: bool = False
    first_message_id: Optional[int] = None
    latest_message_id: Optional[int] = None
    inspection_status: str = InspectionStatus.COMPLETE

    def to_dict(self) -> dict:
        return {
            "topic_id": self.topic_id,
            "title": self.title,
            "top_message": self.top_message,
            "closed": self.closed,
            "hidden": self.hidden,
            "first_message_id": self.first_message_id,
            "latest_message_id": self.latest_message_id,
            "inspection_status": self.inspection_status,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TopicRecord":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class DialogRecord:
    chat_id: int
    category: str  # DialogCategory value
    title: str = ""
    username: Optional[str] = None
    visibility: str = "UNKNOWN"        # PUBLIC / PRIVATE / UNKNOWN
    description: Optional[str] = None
    member_count: Optional[int] = None
    latest_message_id: Optional[int] = None
    is_forum: Optional[bool] = None
    is_bot: Optional[bool] = None
    chat_type: str = ""
    # Channel-specific
    has_discussion: Optional[bool] = None
    discussion_group_id: Optional[int] = None
    discussion_group_title: Optional[str] = None
    latest_public_link: Optional[str] = None
    comment_availability: str = "UNKNOWN"
    # Forum topics
    topics: List[TopicRecord] = field(default_factory=list)
    # Status
    inspection_status: str = InspectionStatus.COMPLETE
    error_reason: Optional[str] = None

    def to_dict(self) -> dict:
        d = {
            "chat_id": self.chat_id,
            "category": self.category,
            "title": self.title,
            "username": self.username,
            "visibility": self.visibility,
            "description": self.description,
            "member_count": self.member_count,
            "latest_message_id": self.latest_message_id,
            "is_forum": self.is_forum,
            "is_bot": self.is_bot,
            "chat_type": self.chat_type,
            "has_discussion": self.has_discussion,
            "discussion_group_id": self.discussion_group_id,
            "discussion_group_title": self.discussion_group_title,
            "latest_public_link": self.latest_public_link,
            "comment_availability": self.comment_availability,
            "topics": [t.to_dict() for t in self.topics],
            "inspection_status": self.inspection_status,
            "error_reason": self.error_reason,
        }
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "DialogRecord":
        topics_raw = d.pop("topics", [])
        filtered = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        obj = cls(**filtered)
        obj.topics = [TopicRecord.from_dict(t) for t in topics_raw]
        return obj


@dataclass
class InventoryJob:
    job_id: str
    target_user_id: int
    status: str = JobStatus.PENDING
    created_at: str = ""
    updated_at: str = ""
    # Progress counters
    dialogs_discovered: int = 0
    channels_inspected: int = 0
    groups_inspected: int = 0
    bots_inspected: int = 0
    private_chats_inspected: int = 0
    topics_discovered: int = 0
    dialogs_completed: int = 0
    errors_requiring_retry: int = 0
    current_stage: str = "Initializing"
    # Completed records (chat_id set for dedup on resume)
    completed_chat_ids: Set[int] = field(default_factory=set)
    # All collected records
    records: List[DialogRecord] = field(default_factory=list)
    # Failures
    failures: List[Dict[str, Any]] = field(default_factory=list)
    # Progress message tracking
    progress_message_id: Optional[int] = None
    # Runtime (not persisted)
    _cancel_event: Optional[asyncio.Event] = field(default=None, repr=False)
    _task: Optional[asyncio.Task] = field(default=None, repr=False)

    def dialogs_remaining(self) -> int:
        return max(0, self.dialogs_discovered - self.dialogs_completed)


# ═══════════════════════════════════════════════════════════════════════════
# Job Persistence (SQLite)
# ═══════════════════════════════════════════════════════════════════════════

async def _get_job_db():
    """Get aiosqlite connection for job persistence."""
    if not _AIOSQLITE_AVAILABLE:
        return None
    os.makedirs(_JOB_DB_DIR, exist_ok=True)
    try:
        db = await aiosqlite.connect(_JOB_DB_PATH)
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        return db
    except Exception as e:
        logger.warning(f"userdata job DB connection failed: {e}")
        return None


async def _init_job_tables():
    """Create job persistence tables if they don't exist."""
    db = await _get_job_db()
    if db is None:
        return
    try:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS userdata_jobs (
                job_id TEXT PRIMARY KEY,
                target_user_id INTEGER NOT NULL,
                status TEXT DEFAULT 'PENDING',
                created_at TEXT,
                updated_at TEXT,
                dialogs_discovered INTEGER DEFAULT 0,
                channels_inspected INTEGER DEFAULT 0,
                groups_inspected INTEGER DEFAULT 0,
                bots_inspected INTEGER DEFAULT 0,
                private_chats_inspected INTEGER DEFAULT 0,
                topics_discovered INTEGER DEFAULT 0,
                dialogs_completed INTEGER DEFAULT 0,
                errors_requiring_retry INTEGER DEFAULT 0,
                current_stage TEXT DEFAULT 'Initializing',
                progress_message_id INTEGER,
                completed_chat_ids_json TEXT DEFAULT '[]',
                records_json TEXT DEFAULT '[]',
                failures_json TEXT DEFAULT '[]'
            );
            CREATE INDEX IF NOT EXISTS idx_userdata_jobs_target
                ON userdata_jobs(target_user_id);
            CREATE INDEX IF NOT EXISTS idx_userdata_jobs_status
                ON userdata_jobs(status);
        """)
        await db.commit()
    except Exception as e:
        logger.warning(f"userdata job table init failed: {e}")
    finally:
        await db.close()


async def _save_job(job: InventoryJob):
    """Persist job state to SQLite."""
    db = await _get_job_db()
    if db is None:
        return
    try:
        now = datetime.now(timezone.utc).isoformat()
        await db.execute(
            """INSERT OR REPLACE INTO userdata_jobs
               (job_id, target_user_id, status, created_at, updated_at,
                dialogs_discovered, channels_inspected, groups_inspected,
                bots_inspected, private_chats_inspected, topics_discovered,
                dialogs_completed, errors_requiring_retry, current_stage,
                progress_message_id, completed_chat_ids_json, records_json,
                failures_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job.job_id, job.target_user_id, job.status,
                job.created_at or now, now,
                job.dialogs_discovered, job.channels_inspected,
                job.groups_inspected, job.bots_inspected,
                job.private_chats_inspected, job.topics_discovered,
                job.dialogs_completed, job.errors_requiring_retry,
                job.current_stage, job.progress_message_id,
                json.dumps(list(job.completed_chat_ids)),
                json.dumps([r.to_dict() for r in job.records]),
                json.dumps(job.failures),
            ),
        )
        await db.commit()
    except Exception as e:
        logger.warning(f"userdata job save failed: {e}")
    finally:
        await db.close()


async def _load_job(job_id: str) -> Optional[InventoryJob]:
    """Load job from SQLite."""
    db = await _get_job_db()
    if db is None:
        return None
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM userdata_jobs WHERE job_id = ?", (job_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        row = dict(row)
        job = InventoryJob(
            job_id=row["job_id"],
            target_user_id=row["target_user_id"],
            status=row["status"],
            created_at=row.get("created_at", ""),
            updated_at=row.get("updated_at", ""),
            dialogs_discovered=row.get("dialogs_discovered", 0),
            channels_inspected=row.get("channels_inspected", 0),
            groups_inspected=row.get("groups_inspected", 0),
            bots_inspected=row.get("bots_inspected", 0),
            private_chats_inspected=row.get("private_chats_inspected", 0),
            topics_discovered=row.get("topics_discovered", 0),
            dialogs_completed=row.get("dialogs_completed", 0),
            errors_requiring_retry=row.get("errors_requiring_retry", 0),
            current_stage=row.get("current_stage", ""),
            progress_message_id=row.get("progress_message_id"),
        )
        try:
            job.completed_chat_ids = set(json.loads(row.get("completed_chat_ids_json", "[]")))
        except Exception:
            job.completed_chat_ids = set()
        try:
            job.records = [
                DialogRecord.from_dict(r) for r in json.loads(row.get("records_json", "[]"))
            ]
        except Exception:
            job.records = []
        try:
            job.failures = json.loads(row.get("failures_json", "[]"))
        except Exception:
            job.failures = []
        return job
    except Exception as e:
        logger.warning(f"userdata job load failed: {e}")
        return None
    finally:
        await db.close()


async def _find_active_job_for_user(target_user_id: int) -> Optional[str]:
    """Check if there is an active (non-terminal) job for a target user."""
    db = await _get_job_db()
    if db is None:
        return None
    try:
        cursor = await db.execute(
            "SELECT job_id FROM userdata_jobs WHERE target_user_id = ? AND status IN (?, ?, ?, ?)",
            (target_user_id, JobStatus.PENDING, JobStatus.SCANNING, JobStatus.INSPECTING, JobStatus.GENERATING),
        )
        row = await cursor.fetchone()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        await db.close()


async def _delete_job(job_id: str):
    """Delete a job from SQLite."""
    db = await _get_job_db()
    if db is None:
        return
    try:
        await db.execute("DELETE FROM userdata_jobs WHERE job_id = ?", (job_id,))
        await db.commit()
    except Exception as e:
        logger.warning(f"userdata job delete failed: {e}")
    finally:
        await db.close()


# ═══════════════════════════════════════════════════════════════════════════
# Active Jobs Registry (in-memory)
# ═══════════════════════════════════════════════════════════════════════════

_active_jobs: Dict[str, InventoryJob] = {}
_active_jobs_lock = None


def _get_active_lock() -> asyncio.Lock:
    global _active_jobs_lock
    if _active_jobs_lock is None:
        _active_jobs_lock = asyncio.Lock()
    return _active_jobs_lock


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _owner(message: Message) -> bool:
    return message.from_user and message.from_user.id == OWNER_ID


def _format_elapsed(seconds: float) -> str:
    h, r = divmod(int(seconds), 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _classify_dialog(dialog) -> str:
    """Classify a Pyrogram Dialog into a category."""
    chat = dialog.chat
    chat_type = chat.type

    if chat_type == ChatType.CHANNEL:
        return DialogCategory.CHANNEL
    elif chat_type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return DialogCategory.GROUP
    elif chat_type == ChatType.BOT:
        return DialogCategory.BOT
    elif chat_type == ChatType.PRIVATE:
        # Check if it's a bot by looking at is_bot attribute
        if getattr(chat, "is_bot", False):
            return DialogCategory.BOT
        return DialogCategory.PRIVATE
    else:
        return DialogCategory.UNKNOWN


def _visibility(chat) -> str:
    """Determine if a chat is public or private."""
    if getattr(chat, "username", None):
        return "PUBLIC"
    return "PRIVATE"


def _safe_str(value, default: str = "UNKNOWN") -> str:
    if value is None:
        return default
    return str(value)


# ═══════════════════════════════════════════════════════════════════════════
# Progress Reporter
# ═══════════════════════════════════════════════════════════════════════════

class ProgressReporter:
    """Edits a single progress message approximately every PROGRESS_INTERVAL seconds."""

    def __init__(self, bot_client: Client, job: InventoryJob):
        self._bot = bot_client
        self._job = job
        self._last_edit_time: float = 0.0
        self._start_time: float = time.monotonic()
        self._message_id: Optional[int] = None

    def _build_text(self) -> str:
        j = self._job
        elapsed = time.monotonic() - self._start_time
        return (
            "USER DATA INVENTORY\n\n"
            f"Target user: {j.target_user_id}\n"
            f"Job ID: {j.job_id[:12]}\n"
            f"Status: {j.status}\n\n"
            f"Dialogs discovered: {j.dialogs_discovered}\n"
            f"Channels inspected: {j.channels_inspected}\n"
            f"Groups inspected: {j.groups_inspected}\n"
            f"Bots inspected: {j.bots_inspected}\n"
            f"Private chats inspected: {j.private_chats_inspected}\n\n"
            f"Topics discovered: {j.topics_discovered}\n"
            f"Dialogs completed: {j.dialogs_completed}\n"
            f"Dialogs remaining: {j.dialogs_remaining()}\n\n"
            f"Current stage: {j.current_stage}\n"
            f"Elapsed: {_format_elapsed(elapsed)}\n"
            f"Errors requiring retry: {j.errors_requiring_retry}"
        )

    async def send_initial(self):
        """Send the first progress message."""
        try:
            msg = await self._bot.send_message(
                OWNER_ID, self._build_text(), parse_mode=ParseMode.DISABLED
            )
            self._message_id = msg.id
            self._job.progress_message_id = msg.id
            self._last_edit_time = time.monotonic()
        except Exception as e:
            logger.warning(f"userdata: failed to send initial progress: {e}")

    async def update(self, force: bool = False):
        """Edit the progress message if enough time has passed."""
        now = time.monotonic()
        if not force and (now - self._last_edit_time) < PROGRESS_INTERVAL:
            return
        if self._message_id is None:
            await self.send_initial()
            return
        try:
            await self._bot.edit_message_text(
                OWNER_ID, self._message_id, self._build_text(),
                parse_mode=ParseMode.DISABLED,
            )
            self._last_edit_time = now
        except FloodWait as e:
            # Don't block the scan — skip this edit
            logger.debug(f"userdata progress edit FloodWait: {e}")
        except RPCError as e:
            # Message may have been deleted — try sending a new one
            if "MESSAGE_NOT_MODIFIED" in str(e):
                pass  # Same content, ignore
            elif "MESSAGE_ID_INVALID" in str(e):
                self._message_id = None
            else:
                logger.debug(f"userdata progress edit error: {e}")
        except Exception as e:
            logger.debug(f"userdata progress edit error: {e}")

    async def finalize(self, summary_text: str):
        """Edit the progress message to show a final summary."""
        if self._message_id is None:
            try:
                await self._bot.send_message(
                    OWNER_ID, summary_text, parse_mode=ParseMode.DISABLED
                )
            except Exception:
                pass
            return
        try:
            await self._bot.edit_message_text(
                OWNER_ID, self._message_id, summary_text,
                parse_mode=ParseMode.DISABLED,
            )
        except Exception:
            try:
                await self._bot.send_message(
                    OWNER_ID, summary_text, parse_mode=ParseMode.DISABLED
                )
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════════
# Inventory Engine
# ═══════════════════════════════════════════════════════════════════════════

async def _safe_api_call(coro, default=None, context: str = ""):
    """Execute an API call with FloodWait / transient error handling."""
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            return await asyncio.wait_for(coro, timeout=30.0)
        except asyncio.TimeoutError:
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(calculate_backoff(attempt))
                continue
            logger.debug(f"userdata: timeout in {context}")
            return default
        except Exception as e:
            if is_floodwait_error(e):
                wait = min(get_floodwait_seconds(e), MAX_FLOODWAIT)
                logger.info(f"userdata: FloodWait {wait}s in {context}")
                await asyncio.sleep(wait)
                continue
            if is_transient_error(e) and attempt < MAX_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(calculate_backoff(attempt))
                continue
            logger.debug(f"userdata: error in {context}: {e}")
            return default
    return default


async def _inspect_channel(
    user_client: Client, chat, dialog, job: InventoryJob
) -> DialogRecord:
    """Inspect a channel dialog and collect metadata."""
    rec = DialogRecord(
        chat_id=chat.id,
        category=DialogCategory.CHANNEL,
        title=getattr(chat, "title", "") or "",
        username=getattr(chat, "username", None),
        visibility=_visibility(chat),
        chat_type=str(chat.type),
        latest_message_id=getattr(dialog, "top_message", None)
                          and getattr(dialog.top_message, "id", None),
    )

    # Try to get full chat info for description and member count
    full_chat = await _safe_api_call(
        user_client.get_chat(chat.id), context=f"get_chat(channel {chat.id})"
    )
    if full_chat:
        rec.description = getattr(full_chat, "description", None)
        rec.member_count = getattr(full_chat, "members_count", None)
        linked = getattr(full_chat, "linked_chat", None)
        if linked:
            rec.has_discussion = True
            rec.discussion_group_id = linked.id
            rec.discussion_group_title = getattr(linked, "title", None)
        else:
            rec.has_discussion = False

    # Build latest public link if channel is public and has a top message
    if rec.username and rec.latest_message_id:
        rec.latest_public_link = f"https://t.me/{rec.username}/{rec.latest_message_id}"

    # Check comment availability on latest post
    if rec.latest_message_id and rec.has_discussion:
        try:
            msg = await _safe_api_call(
                user_client.get_messages(chat.id, rec.latest_message_id),
                context=f"get_messages(channel {chat.id}, {rec.latest_message_id})",
            )
            if msg:
                # If the message has replies info, comments are available
                replies = getattr(msg, "replies", None)
                if replies is not None:
                    rec.comment_availability = "YES"
                else:
                    rec.comment_availability = "NO"
        except Exception:
            rec.comment_availability = "UNKNOWN"
    elif rec.has_discussion is False:
        rec.comment_availability = "NO"

    rec.inspection_status = InspectionStatus.COMPLETE
    return rec


async def _inspect_group(
    user_client: Client, chat, dialog, job: InventoryJob
) -> DialogRecord:
    """Inspect a group/supergroup dialog and collect metadata + topics."""
    rec = DialogRecord(
        chat_id=chat.id,
        category=DialogCategory.GROUP,
        title=getattr(chat, "title", "") or "",
        username=getattr(chat, "username", None),
        visibility=_visibility(chat),
        chat_type=str(chat.type),
        latest_message_id=getattr(dialog, "top_message", None)
                          and getattr(dialog.top_message, "id", None),
    )

    # Full chat info
    full_chat = await _safe_api_call(
        user_client.get_chat(chat.id), context=f"get_chat(group {chat.id})"
    )
    if full_chat:
        rec.description = getattr(full_chat, "description", None)
        rec.member_count = getattr(full_chat, "members_count", None)
        rec.is_forum = getattr(full_chat, "is_forum", None)
        if rec.is_forum is None:
            # Pyrofork might not expose is_forum on all versions
            rec.is_forum = getattr(full_chat, "has_protected_content", None) is not None and False
            # Can't determine — mark unknown
            rec.is_forum = None

    # Forum topic enumeration
    if rec.is_forum:
        job.current_stage = f"Fetching topics for group {chat.id}"
        topics = await _fetch_forum_topics(user_client, chat.id)
        rec.topics = topics
        job.topics_discovered += len(topics)

    rec.inspection_status = InspectionStatus.COMPLETE
    return rec


async def _fetch_forum_topics(
    user_client: Client, chat_id: int
) -> List[TopicRecord]:
    """Fetch all forum topics for a group using raw MTProto API."""
    topics: List[TopicRecord] = []
    offset_date = 0
    offset_id = 0
    offset_topic = 0

    try:
        peer = await user_client.resolve_peer(chat_id)
    except Exception as e:
        logger.debug(f"userdata: resolve_peer failed for topics in {chat_id}: {e}")
        return topics

    while True:
        try:
            result = await asyncio.wait_for(
                user_client.invoke(
                    functions.channels.GetForumTopics(
                        channel=peer,
                        offset_date=offset_date,
                        offset_id=offset_id,
                        offset_topic=offset_topic,
                        limit=TOPIC_PAGE_SIZE,
                        q="",
                    )
                ),
                timeout=30.0,
            )
        except Exception as e:
            if is_floodwait_error(e):
                wait = min(get_floodwait_seconds(e), MAX_TOPIC_FLOODWAIT)
                logger.info(f"userdata: FloodWait {wait}s in GetForumTopics({chat_id})")
                await asyncio.sleep(wait)
                continue
            logger.debug(f"userdata: GetForumTopics error for {chat_id}: {e}")
            break

        if not result or not result.topics:
            break

        for topic in result.topics:
            if isinstance(topic, types.ForumTopic):
                top_msg = getattr(topic, "top_message", 0)
                topics.append(TopicRecord(
                    topic_id=topic.id,
                    title=topic.title,
                    top_message=top_msg,
                    closed=getattr(topic, "closed", False),
                    hidden=getattr(topic, "hidden", False),
                    # top_message serves as the latest accessible message ID
                    latest_message_id=top_msg if top_msg else None,
                    # The topic_id itself is the creation/root message ID
                    first_message_id=topic.id,
                ))

        if len(result.topics) < TOPIC_PAGE_SIZE:
            break

        # Paginate
        last_topic = result.topics[-1]
        if isinstance(last_topic, types.ForumTopic):
            offset_topic = last_topic.id
            offset_id = getattr(last_topic, "top_message", 0)
        else:
            break

        # Brief delay between pages
        await asyncio.sleep(0.3)

    return topics


async def _inspect_bot(chat, dialog) -> DialogRecord:
    """Inspect a bot dialog."""
    return DialogRecord(
        chat_id=chat.id,
        category=DialogCategory.BOT,
        title=getattr(chat, "first_name", "") or getattr(chat, "title", "") or "",
        username=getattr(chat, "username", None),
        is_bot=True,
        chat_type=str(chat.type),
        inspection_status=InspectionStatus.COMPLETE,
    )


async def _inspect_private(chat, dialog) -> DialogRecord:
    """Inspect a private user chat dialog."""
    return DialogRecord(
        chat_id=chat.id,
        category=DialogCategory.PRIVATE,
        title=(getattr(chat, "first_name", "") or "")
              + (" " + getattr(chat, "last_name", "") if getattr(chat, "last_name", None) else ""),
        username=getattr(chat, "username", None),
        is_bot=getattr(chat, "is_bot", False),
        chat_type=str(chat.type),
        inspection_status=InspectionStatus.COMPLETE,
    )


async def _run_inventory(
    bot_client: Client,
    job: InventoryJob,
    session_string: str,
):
    """
    Main inventory coroutine. Runs as a background task.

    Uses the target user's session to iterate their dialogs,
    inspect channels/groups/bots/private chats, and collect metadata.
    """
    from TechVJ.session_handler import (
        SessionConnectionError,
        SessionInvalidError,
        create_user_session,
    )

    reporter = ProgressReporter(bot_client, job)
    cancel_event = job._cancel_event or asyncio.Event()
    job._cancel_event = cancel_event
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_INSPECTIONS)
    checkpoint_counter = 0

    try:
        # Send initial progress
        await reporter.send_initial()
        job.status = JobStatus.SCANNING
        job.current_stage = "Connecting to user session"
        await reporter.update(force=True)

        async with create_user_session(
            session_string, job.target_user_id, timeout=60, validate=True
        ) as user_client:
            # Phase 1: Discover all dialogs
            job.current_stage = "Scanning dialogs"
            await reporter.update(force=True)

            raw_dialogs: List[Tuple[Any, Any]] = []  # (dialog, chat) pairs
            try:
                async for dialog in user_client.get_dialogs():
                    if cancel_event.is_set():
                        job.status = JobStatus.CANCELLED
                        job.current_stage = "Cancelled during dialog scan"
                        await _save_job(job)
                        await reporter.update(force=True)
                        return

                    chat = dialog.chat
                    raw_dialogs.append((dialog, chat))
                    job.dialogs_discovered += 1

                    # Periodic progress update during scan
                    if job.dialogs_discovered % 50 == 0:
                        await reporter.update()
            except Exception as e:
                logger.warning(f"userdata: dialog scan error: {e}")
                if is_floodwait_error(e):
                    wait = min(get_floodwait_seconds(e), MAX_FLOODWAIT)
                    await asyncio.sleep(wait)
                job.current_stage = f"Dialog scan interrupted ({type(e).__name__})"

            await reporter.update(force=True)
            logger.info(
                f"userdata: discovered {job.dialogs_discovered} dialogs for user {job.target_user_id}"
            )

            # Phase 2: Inspect each dialog
            job.status = JobStatus.INSPECTING

            async def _inspect_one(dialog, chat):
                nonlocal checkpoint_counter

                if cancel_event.is_set():
                    return

                chat_id = chat.id
                if chat_id in job.completed_chat_ids:
                    # Already processed (resume case)
                    return

                category = _classify_dialog(dialog)
                record: Optional[DialogRecord] = None

                async with semaphore:
                    try:
                        if category == DialogCategory.CHANNEL:
                            job.current_stage = f"Inspecting channel: {getattr(chat, 'title', chat_id)}"
                            record = await _inspect_channel(user_client, chat, dialog, job)
                            job.channels_inspected += 1
                        elif category == DialogCategory.GROUP:
                            job.current_stage = f"Inspecting group: {getattr(chat, 'title', chat_id)}"
                            record = await _inspect_group(user_client, chat, dialog, job)
                            job.groups_inspected += 1
                        elif category == DialogCategory.BOT:
                            record = await _inspect_bot(chat, dialog)
                            job.bots_inspected += 1
                        elif category == DialogCategory.PRIVATE:
                            record = await _inspect_private(chat, dialog)
                            job.private_chats_inspected += 1
                        else:
                            record = DialogRecord(
                                chat_id=chat_id,
                                category=DialogCategory.UNKNOWN,
                                title=getattr(chat, "title", "") or getattr(chat, "first_name", "") or "",
                                chat_type=str(chat.type),
                                inspection_status=InspectionStatus.PARTIAL,
                                error_reason="Unknown chat type",
                            )
                    except Exception as e:
                        logger.warning(f"userdata: inspect error for {chat_id}: {e}")
                        record = DialogRecord(
                            chat_id=chat_id,
                            category=category,
                            title=getattr(chat, "title", "") or getattr(chat, "first_name", "") or "",
                            chat_type=str(getattr(chat, "type", "")),
                            inspection_status=InspectionStatus.FAILED,
                            error_reason=f"{type(e).__name__}: {e}",
                        )
                        job.errors_requiring_retry += 1
                        job.failures.append({
                            "chat_id": chat_id,
                            "stage": "inspection",
                            "reason": f"{type(e).__name__}: {e}",
                        })

                if record:
                    job.records.append(record)
                    job.completed_chat_ids.add(chat_id)
                    job.dialogs_completed += 1
                    checkpoint_counter += 1

                    # Periodic checkpoint
                    if checkpoint_counter >= CHECKPOINT_BATCH_SIZE:
                        checkpoint_counter = 0
                        await _save_job(job)

                await reporter.update()

            # Process dialogs sequentially with bounded concurrency for API calls
            # We use sequential iteration but each inspection can do parallel
            # sub-calls (get_chat, get_members_count) via the semaphore
            for dialog, chat in raw_dialogs:
                if cancel_event.is_set():
                    break
                await _inspect_one(dialog, chat)

            # Final checkpoint
            await _save_job(job)

        # Phase 3: Generate report
        job.status = JobStatus.GENERATING
        job.current_stage = "Generating TXT report"
        await reporter.update(force=True)

        report_path = await _generate_report(job)

        # Phase 4: Deliver report
        if report_path and os.path.exists(report_path):
            try:
                await bot_client.send_document(
                    OWNER_ID,
                    report_path,
                    caption=(
                        f"User Data Inventory — {job.target_user_id}\n"
                        f"Job: {job.job_id[:12]}\n"
                        f"Dialogs: {job.dialogs_completed}/{job.dialogs_discovered}"
                    ),
                    parse_mode=ParseMode.DISABLED,
                )
            except Exception as e:
                logger.warning(f"userdata: failed to send report: {e}")
                await bot_client.send_message(
                    OWNER_ID,
                    f"Failed to send report file: {e}",
                    parse_mode=ParseMode.DISABLED,
                )
        else:
            await bot_client.send_message(
                OWNER_ID,
                "Failed to generate report file.",
                parse_mode=ParseMode.DISABLED,
            )

        # Determine final status
        has_failures = bool(job.failures) or job.dialogs_completed < job.dialogs_discovered
        job.status = JobStatus.PARTIAL if has_failures else JobStatus.COMPLETE
        job.current_stage = "Done"
        await _save_job(job)

        summary = (
            f"USER DATA INVENTORY — COMPLETE\n\n"
            f"Target user: {job.target_user_id}\n"
            f"Job ID: {job.job_id[:12]}\n"
            f"Status: {job.status}\n\n"
            f"Channels: {job.channels_inspected}\n"
            f"Groups: {job.groups_inspected}\n"
            f"Topics: {job.topics_discovered}\n"
            f"Bots: {job.bots_inspected}\n"
            f"Private chats: {job.private_chats_inspected}\n"
            f"Total: {job.dialogs_completed}/{job.dialogs_discovered}\n"
            f"Failures: {len(job.failures)}"
        )
        await reporter.finalize(summary)

        # Cleanup temp file
        if report_path and os.path.exists(report_path):
            try:
                os.remove(report_path)
            except Exception:
                pass

    except SessionInvalidError as e:
        job.status = JobStatus.FAILED
        job.current_stage = f"Session invalid: {e.error_type}"
        await _save_job(job)
        await reporter.finalize(
            f"USER DATA INVENTORY — FAILED\n\n"
            f"Target user: {job.target_user_id}\n"
            f"Reason: Session is invalid ({e.error_type})\n"
            f"The user needs to /login again."
        )

    except SessionConnectionError as e:
        job.status = JobStatus.FAILED
        job.current_stage = f"Connection failed: {e}"
        await _save_job(job)
        await reporter.finalize(
            f"USER DATA INVENTORY — FAILED\n\n"
            f"Target user: {job.target_user_id}\n"
            f"Reason: Cannot connect to session\n"
            f"Error: {e}\n"
            f"Use /userdata_resume {job.job_id[:12]} to retry."
        )

    except asyncio.CancelledError:
        job.status = JobStatus.CANCELLED
        job.current_stage = "Cancelled"
        await _save_job(job)
        await reporter.finalize(
            f"USER DATA INVENTORY — CANCELLED\n\n"
            f"Target user: {job.target_user_id}\n"
            f"Job ID: {job.job_id[:12]}\n"
            f"Completed: {job.dialogs_completed}/{job.dialogs_discovered}\n"
            f"Use /userdata_resume {job.job_id[:12]} to continue."
        )

    except Exception as e:
        logger.exception(f"userdata: unexpected error for user {job.target_user_id}")
        job.status = JobStatus.FAILED
        job.current_stage = f"Unexpected error: {type(e).__name__}"
        await _save_job(job)
        await reporter.finalize(
            f"USER DATA INVENTORY — FAILED\n\n"
            f"Target user: {job.target_user_id}\n"
            f"Error: {type(e).__name__}: {e}\n"
            f"Use /userdata_resume {job.job_id[:12]} to retry."
        )

    finally:
        # Remove from active jobs
        async with _get_active_lock():
            _active_jobs.pop(job.job_id, None)


# ═══════════════════════════════════════════════════════════════════════════
# TXT Report Generator
# ═══════════════════════════════════════════════════════════════════════════

async def _generate_report(job: InventoryJob) -> Optional[str]:
    """Generate a UTF-8 TXT report and return the file path."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"userdata_{job.target_user_id}_{timestamp}.txt"
    filepath = os.path.join(TEMP_DIR, filename)

    # Categorize records
    channels = [r for r in job.records if r.category == DialogCategory.CHANNEL]
    groups = [r for r in job.records if r.category == DialogCategory.GROUP]
    bots = [r for r in job.records if r.category == DialogCategory.BOT]
    privates = [r for r in job.records if r.category == DialogCategory.PRIVATE]
    failures = [r for r in job.records if r.inspection_status in (
        InspectionStatus.FAILED, InspectionStatus.PARTIAL
    )]

    total_topics = sum(len(r.topics) for r in groups)
    has_failures = bool(failures) or bool(job.failures)
    scan_status = "PARTIAL" if has_failures else "COMPLETE"

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            # Header
            f.write("TELEGRAM USER DATA INVENTORY\n")
            f.write(f"Target User ID: {job.target_user_id}\n")
            f.write(f"Job ID: {job.job_id}\n")
            f.write(f"Generated At: {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"Scan Status: {scan_status}\n")
            f.write("\n")

            # Summary
            f.write("SUMMARY\n")
            f.write(f"Dialogs discovered: {job.dialogs_discovered}\n")
            f.write(f"Dialogs completed: {job.dialogs_completed}\n")
            f.write(f"Channels: {len(channels)}\n")
            f.write(f"Groups: {len(groups)}\n")
            f.write(f"Forum topics: {total_topics}\n")
            f.write(f"Bots: {len(bots)}\n")
            f.write(f"Private user chats: {len(privates)}\n")
            f.write(f"Failed inspections: {len(failures)}\n")
            f.write("\n")

            # Channels
            f.write("=" * 50 + "\n")
            f.write("CHANNELS\n")
            f.write("=" * 50 + "\n\n")

            for i, ch in enumerate(channels, 1):
                f.write(f"[{i}] {ch.title}\n")
                f.write(f"    Chat ID: {ch.chat_id}\n")
                f.write(f"    Username: {_safe_str(ch.username, 'N/A')}\n")
                f.write(f"    Visibility: {ch.visibility}\n")
                f.write(f"    Description: {_safe_str(ch.description, 'NOT ACCESSIBLE')}\n")
                f.write(f"    Members: {_safe_str(ch.member_count, 'NOT ACCESSIBLE')}\n")
                f.write(f"    Latest post ID: {_safe_str(ch.latest_message_id, 'UNKNOWN')}\n")
                f.write(f"    Latest public link: {_safe_str(ch.latest_public_link, 'N/A')}\n")
                disc = "UNKNOWN"
                if ch.has_discussion is True:
                    disc = f"YES (ID: {ch.discussion_group_id}, Title: {_safe_str(ch.discussion_group_title)})"
                elif ch.has_discussion is False:
                    disc = "NO"
                f.write(f"    Discussion group: {disc}\n")
                f.write(f"    Comment availability: {ch.comment_availability}\n")
                f.write(f"    Inspection status: {ch.inspection_status}\n")
                if ch.error_reason:
                    f.write(f"    Error: {ch.error_reason}\n")
                f.write("\n")

            # Groups
            f.write("=" * 50 + "\n")
            f.write("GROUPS AND SUPERGROUPS\n")
            f.write("=" * 50 + "\n\n")

            for i, gr in enumerate(groups, 1):
                f.write(f"[{i}] {gr.title}\n")
                f.write(f"    Chat ID: {gr.chat_id}\n")
                f.write(f"    Username: {_safe_str(gr.username, 'N/A')}\n")
                f.write(f"    Visibility: {gr.visibility}\n")
                f.write(f"    Description: {_safe_str(gr.description, 'NOT ACCESSIBLE')}\n")
                f.write(f"    Members: {_safe_str(gr.member_count, 'NOT ACCESSIBLE')}\n")
                f.write(f"    Latest message ID: {_safe_str(gr.latest_message_id, 'UNKNOWN')}\n")

                forum_str = "UNKNOWN"
                if gr.is_forum is True:
                    forum_str = "YES"
                elif gr.is_forum is False:
                    forum_str = "NO"
                f.write(f"    Forum enabled: {forum_str}\n")
                f.write(f"    Inspection status: {gr.inspection_status}\n")
                if gr.error_reason:
                    f.write(f"    Error: {gr.error_reason}\n")

                if gr.topics:
                    f.write(f"\n    Topics ({len(gr.topics)}):\n")
                    for t in gr.topics:
                        closed_str = " [CLOSED]" if t.closed else ""
                        hidden_str = " [HIDDEN]" if t.hidden else ""
                        f.write(f"      - Topic: {t.title}{closed_str}{hidden_str}\n")
                        f.write(f"        Topic ID: {t.topic_id}\n")
                        f.write(f"        First accessible message ID: {_safe_str(t.first_message_id, 'UNKNOWN')}\n")
                        f.write(f"        Latest accessible message ID: {_safe_str(t.latest_message_id, 'UNKNOWN')}\n")
                        f.write(f"        Topic creation/root message ID: {t.topic_id}\n")
                        f.write(f"        Inspection status: {t.inspection_status}\n")
                f.write("\n")

            # Bots
            f.write("=" * 50 + "\n")
            f.write("BOTS\n")
            f.write("=" * 50 + "\n\n")

            for i, bt in enumerate(bots, 1):
                f.write(f"[{i}] Name: {bt.title}\n")
                f.write(f"    Username: {_safe_str(bt.username, 'N/A')}\n")
                f.write(f"    Chat ID: {bt.chat_id}\n")
                f.write(f"    Chat type: {bt.chat_type}\n")
                f.write(f"    Inspection status: {bt.inspection_status}\n")
                f.write("\n")

            # Private chats
            f.write("=" * 50 + "\n")
            f.write("PRIVATE USER CHATS\n")
            f.write("=" * 50 + "\n\n")

            for i, pc in enumerate(privates, 1):
                f.write(f"[{i}] Name: {pc.title}\n")
                f.write(f"    Username: {_safe_str(pc.username, 'N/A')}\n")
                f.write(f"    Chat ID: {pc.chat_id}\n")
                f.write(f"    Chat type: {pc.chat_type}\n")
                f.write(f"    Is bot: {'YES' if pc.is_bot else 'NO'}\n")
                f.write(f"    Inspection status: {pc.inspection_status}\n")
                f.write("\n")

            # Failures
            all_failures = []
            for rec in failures:
                all_failures.append({
                    "chat_id": rec.chat_id,
                    "stage": "inspection",
                    "reason": rec.error_reason or "Unknown",
                    "retry_status": "not retried",
                })
            for fl in job.failures:
                if fl.get("chat_id") not in [f.get("chat_id") for f in all_failures]:
                    all_failures.append(fl)

            if all_failures:
                f.write("=" * 50 + "\n")
                f.write("INCOMPLETE OR FAILED INSPECTIONS\n")
                f.write("=" * 50 + "\n\n")

                for fl in all_failures:
                    f.write(f"Chat ID: {fl.get('chat_id', 'UNKNOWN')}\n")
                    f.write(f"Stage: {fl.get('stage', 'UNKNOWN')}\n")
                    f.write(f"Reason: {fl.get('reason', 'UNKNOWN')}\n")
                    f.write(f"Retry status: {fl.get('retry_status', 'not retried')}\n")
                    f.write("\n")

            # Footer
            f.write("=" * 50 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 50 + "\n")

        return filepath

    except Exception as e:
        logger.error(f"userdata: report generation failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Command Handlers
# ═══════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("userdata") & filters.private)
async def cmd_userdata(client: Client, message: Message):
    """Owner: /userdata <user_id> — Start user data inventory."""
    if not _owner(message):
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply(
            "Usage: /userdata <user_id>\n\n"
            "Exports the target user's Telegram dialogs, channels, groups, "
            "bots, and forum topics to a TXT file.\n\n"
            "Related commands:\n"
            "/userdata_status <job_id>\n"
            "/userdata_resume <job_id>\n"
            "/userdata_cancel <job_id>",
            parse_mode=ParseMode.DISABLED,
        )
        return

    try:
        target_uid = int(parts[1])
    except ValueError:
        await message.reply("user_id must be an integer.", parse_mode=ParseMode.DISABLED)
        return

    if target_uid <= 0:
        await message.reply(
            "user_id must be a positive integer (Telegram user ID).",
            parse_mode=ParseMode.DISABLED,
        )
        return

    # Initialize job tables
    await _init_job_tables()

    # Check for duplicate active job
    existing_job_id = await _find_active_job_for_user(target_uid)
    if existing_job_id:
        # Also check in-memory
        async with _get_active_lock():
            if existing_job_id in _active_jobs:
                await message.reply(
                    f"An inventory job is already running for user {target_uid}.\n"
                    f"Job ID: {existing_job_id[:12]}\n\n"
                    f"Use /userdata_status {existing_job_id[:12]} to check progress.\n"
                    f"Use /userdata_cancel {existing_job_id[:12]} to cancel.",
                    parse_mode=ParseMode.DISABLED,
                )
                return

    # Look up user session
    user_data = await async_db.find_user(target_uid)
    if not user_data or not user_data.get("logged_in") or not user_data.get("session"):
        await message.reply(
            f"User {target_uid} has no active session.\n"
            "The user must /login first.",
            parse_mode=ParseMode.DISABLED,
        )
        return

    session_string = user_data["session"]

    # Create job
    job_id = uuid.uuid4().hex
    job = InventoryJob(
        job_id=job_id,
        target_user_id=target_uid,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    job._cancel_event = asyncio.Event()

    # Register as active
    async with _get_active_lock():
        _active_jobs[job_id] = job

    # Save initial state
    await _save_job(job)

    await message.reply(
        f"Starting inventory for user {target_uid}.\n"
        f"Job ID: {job_id[:12]}\n"
        f"Progress updates will follow.",
        parse_mode=ParseMode.DISABLED,
    )

    # Launch background task
    from TechVJ.task_manager import task_manager

    task = await task_manager.create_task(
        OWNER_ID,
        _run_inventory(client, job, session_string),
        name=f"userdata-{target_uid}-{job_id[:8]}",
    )
    job._task = task


@Client.on_message(filters.command("userdata_status") & filters.private)
async def cmd_userdata_status(client: Client, message: Message):
    """Owner: /userdata_status <job_id> — Check job status."""
    if not _owner(message):
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Usage: /userdata_status <job_id>", parse_mode=ParseMode.DISABLED)
        return

    job_id_prefix = parts[1].strip()
    await _init_job_tables()

    # Search by prefix
    job = await _find_job_by_prefix(job_id_prefix)
    if not job:
        await message.reply(
            f"Job not found: {job_id_prefix}",
            parse_mode=ParseMode.DISABLED,
        )
        return

    elapsed = ""
    if job.created_at:
        try:
            created = datetime.fromisoformat(job.created_at)
            elapsed_secs = (datetime.now(timezone.utc) - created).total_seconds()
            elapsed = f"\nElapsed: {_format_elapsed(elapsed_secs)}"
        except Exception:
            pass

    await message.reply(
        f"USER DATA JOB STATUS\n\n"
        f"Job ID: {job.job_id[:12]}\n"
        f"Target user: {job.target_user_id}\n"
        f"Status: {job.status}\n"
        f"Stage: {job.current_stage}\n\n"
        f"Dialogs: {job.dialogs_completed}/{job.dialogs_discovered}\n"
        f"Channels: {job.channels_inspected}\n"
        f"Groups: {job.groups_inspected}\n"
        f"Topics: {job.topics_discovered}\n"
        f"Bots: {job.bots_inspected}\n"
        f"Private: {job.private_chats_inspected}\n"
        f"Errors: {job.errors_requiring_retry}"
        f"{elapsed}",
        parse_mode=ParseMode.DISABLED,
    )


@Client.on_message(filters.command("userdata_resume") & filters.private)
async def cmd_userdata_resume(client: Client, message: Message):
    """Owner: /userdata_resume <job_id> — Resume interrupted job."""
    if not _owner(message):
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Usage: /userdata_resume <job_id>", parse_mode=ParseMode.DISABLED)
        return

    job_id_prefix = parts[1].strip()
    await _init_job_tables()

    job = await _find_job_by_prefix(job_id_prefix)
    if not job:
        await message.reply(f"Job not found: {job_id_prefix}", parse_mode=ParseMode.DISABLED)
        return

    # Check if job is in a terminal state that can be resumed
    if job.status in (JobStatus.SCANNING, JobStatus.INSPECTING, JobStatus.GENERATING):
        # Check if it's actually running in memory
        async with _get_active_lock():
            if job.job_id in _active_jobs:
                await message.reply(
                    f"Job {job.job_id[:12]} is still running.\n"
                    f"Use /userdata_status {job.job_id[:12]} to check progress.",
                    parse_mode=ParseMode.DISABLED,
                )
                return

    if job.status == JobStatus.COMPLETE:
        await message.reply(
            f"Job {job.job_id[:12]} is already complete.",
            parse_mode=ParseMode.DISABLED,
        )
        return

    # Look up user session
    user_data = await async_db.find_user(job.target_user_id)
    if not user_data or not user_data.get("logged_in") or not user_data.get("session"):
        await message.reply(
            f"User {job.target_user_id} has no active session.\n"
            "Cannot resume without a valid session.",
            parse_mode=ParseMode.DISABLED,
        )
        return

    session_string = user_data["session"]

    # Reset job status for resume
    job.status = JobStatus.PENDING
    job.current_stage = "Resuming"
    job._cancel_event = asyncio.Event()

    # Register as active
    async with _get_active_lock():
        _active_jobs[job.job_id] = job

    await _save_job(job)

    await message.reply(
        f"Resuming inventory job {job.job_id[:12]} for user {job.target_user_id}.\n"
        f"Previously completed: {job.dialogs_completed} dialogs.\n"
        f"Progress updates will follow.",
        parse_mode=ParseMode.DISABLED,
    )

    # Launch background task
    from TechVJ.task_manager import task_manager

    task = await task_manager.create_task(
        OWNER_ID,
        _run_inventory(client, job, session_string),
        name=f"userdata-resume-{job.target_user_id}-{job.job_id[:8]}",
    )
    job._task = task


@Client.on_message(filters.command("userdata_cancel") & filters.private)
async def cmd_userdata_cancel(client: Client, message: Message):
    """Owner: /userdata_cancel <job_id> — Cancel active job."""
    if not _owner(message):
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Usage: /userdata_cancel <job_id>", parse_mode=ParseMode.DISABLED)
        return

    job_id_prefix = parts[1].strip()
    await _init_job_tables()

    job = await _find_job_by_prefix(job_id_prefix)
    if not job:
        await message.reply(f"Job not found: {job_id_prefix}", parse_mode=ParseMode.DISABLED)
        return

    # Signal cancellation
    async with _get_active_lock():
        active_job = _active_jobs.get(job.job_id)
        if active_job:
            if active_job._cancel_event:
                active_job._cancel_event.set()
            if active_job._task and not active_job._task.done():
                active_job._task.cancel()
            await message.reply(
                f"Cancellation requested for job {job.job_id[:12]}.\n"
                f"Completed progress ({job.dialogs_completed} dialogs) will be preserved.",
                parse_mode=ParseMode.DISABLED,
            )
            return

    # Job not active in memory — just update DB
    if job.status not in (JobStatus.COMPLETE, JobStatus.CANCELLED):
        job.status = JobStatus.CANCELLED
        job.current_stage = "Cancelled by owner"
        await _save_job(job)

    await message.reply(
        f"Job {job.job_id[:12]} marked as cancelled.\n"
        f"It was not running in memory.",
        parse_mode=ParseMode.DISABLED,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Helper: Find job by prefix
# ═══════════════════════════════════════════════════════════════════════════

async def _find_job_by_prefix(prefix: str) -> Optional[InventoryJob]:
    """Find a job by job_id prefix (for convenience)."""
    # First try exact match
    job = await _load_job(prefix)
    if job:
        return job

    # Try prefix search
    if not _AIOSQLITE_AVAILABLE:
        return None

    db = await _get_job_db()
    if db is None:
        return None
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT job_id FROM userdata_jobs WHERE job_id LIKE ? LIMIT 1",
            (prefix + "%",),
        )
        row = await cursor.fetchone()
        if row:
            await db.close()
            return await _load_job(row[0])
        return None
    except Exception:
        return None
    finally:
        try:
            await db.close()
        except Exception:
            pass
