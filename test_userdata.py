"""
Tests for TechVJ/userdata_command.py — /userdata owner-only inventory.

Uses mocks and fixtures to avoid Telegram network access.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, List, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure project root is on sys.path
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


# ─── Stub config before importing module ────────────────────────────────────

# Patch config.OWNER_ID to a known test value before importing userdata_command
_OWNER_ID = 999999


# ─── Helpers ────────────────────────────────────────────────────────────────

def _make_chat(
    chat_id: int,
    chat_type,
    title: str = "",
    username: str = None,
    is_bot: bool = False,
    first_name: str = "",
    last_name: str = "",
    is_forum: bool = False,
    description: str = None,
    members_count: int = None,
):
    """Build a mock Chat object."""
    chat = SimpleNamespace(
        id=chat_id,
        type=chat_type,
        title=title,
        username=username,
        is_bot=is_bot,
        first_name=first_name,
        last_name=last_name,
        is_forum=is_forum,
        description=description,
        members_count=members_count,
    )
    return chat


def _make_dialog(chat, top_message_id: int = None):
    """Build a mock Dialog object."""
    top_msg = None
    if top_message_id:
        top_msg = SimpleNamespace(id=top_message_id)
    return SimpleNamespace(chat=chat, top_message=top_msg)


def _make_message(from_user_id: int, text: str):
    """Build a mock Message object for command testing."""
    msg = MagicMock()
    msg.from_user = SimpleNamespace(id=from_user_id)
    msg.text = text
    msg.chat = SimpleNamespace(id=from_user_id)
    msg.reply = AsyncMock()
    return msg


# ─── Tests ──────────────────────────────────────────────────────────────────

class TestOwnerAuthorization(unittest.TestCase):
    """Ensure commands are owner-only."""

    def test_owner_check_passes(self):
        from TechVJ.userdata_command import _owner
        msg = _make_message(_OWNER_ID, "/userdata 123")
        with patch("TechVJ.userdata_command.OWNER_ID", _OWNER_ID):
            self.assertTrue(_owner(msg))

    def test_owner_check_fails_for_other_user(self):
        from TechVJ.userdata_command import _owner
        msg = _make_message(111111, "/userdata 123")
        with patch("TechVJ.userdata_command.OWNER_ID", _OWNER_ID):
            self.assertFalse(_owner(msg))

    def test_owner_check_fails_no_from_user(self):
        from TechVJ.userdata_command import _owner
        msg = MagicMock()
        msg.from_user = None
        with patch("TechVJ.userdata_command.OWNER_ID", _OWNER_ID):
            self.assertFalse(_owner(msg))


class TestDialogClassification(unittest.TestCase):
    """Test dialog category classification."""

    def test_channel_classification(self):
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import DialogCategory, _classify_dialog
        chat = _make_chat(100, ChatType.CHANNEL, title="Test Channel")
        dialog = _make_dialog(chat)
        self.assertEqual(_classify_dialog(dialog), DialogCategory.CHANNEL)

    def test_supergroup_classification(self):
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import DialogCategory, _classify_dialog
        chat = _make_chat(200, ChatType.SUPERGROUP, title="Test Group")
        dialog = _make_dialog(chat)
        self.assertEqual(_classify_dialog(dialog), DialogCategory.GROUP)

    def test_group_classification(self):
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import DialogCategory, _classify_dialog
        chat = _make_chat(300, ChatType.GROUP, title="Basic Group")
        dialog = _make_dialog(chat)
        self.assertEqual(_classify_dialog(dialog), DialogCategory.GROUP)

    def test_bot_classification_via_type(self):
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import DialogCategory, _classify_dialog
        chat = _make_chat(400, ChatType.BOT, title="Bot")
        dialog = _make_dialog(chat)
        self.assertEqual(_classify_dialog(dialog), DialogCategory.BOT)

    def test_bot_classification_via_is_bot(self):
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import DialogCategory, _classify_dialog
        chat = _make_chat(500, ChatType.PRIVATE, first_name="Bot", is_bot=True)
        dialog = _make_dialog(chat)
        self.assertEqual(_classify_dialog(dialog), DialogCategory.BOT)

    def test_private_chat_classification(self):
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import DialogCategory, _classify_dialog
        chat = _make_chat(600, ChatType.PRIVATE, first_name="John")
        dialog = _make_dialog(chat)
        self.assertEqual(_classify_dialog(dialog), DialogCategory.PRIVATE)


class TestVisibility(unittest.TestCase):
    """Test public vs private visibility detection."""

    def test_public_with_username(self):
        from TechVJ.userdata_command import _visibility
        chat = SimpleNamespace(username="testchannel")
        self.assertEqual(_visibility(chat), "PUBLIC")

    def test_private_without_username(self):
        from TechVJ.userdata_command import _visibility
        chat = SimpleNamespace(username=None)
        self.assertEqual(_visibility(chat), "PRIVATE")

    def test_private_empty_username(self):
        from TechVJ.userdata_command import _visibility
        chat = SimpleNamespace(username="")
        self.assertEqual(_visibility(chat), "PRIVATE")


class TestDataModels(unittest.TestCase):
    """Test data model serialization and deserialization."""

    def test_topic_record_roundtrip(self):
        from TechVJ.userdata_command import TopicRecord
        t = TopicRecord(
            topic_id=42, title="Test Topic", top_message=100,
            closed=True, hidden=False,
            first_message_id=42, latest_message_id=100,
        )
        d = t.to_dict()
        t2 = TopicRecord.from_dict(d)
        self.assertEqual(t.topic_id, t2.topic_id)
        self.assertEqual(t.title, t2.title)
        self.assertTrue(t2.closed)
        self.assertFalse(t2.hidden)

    def test_dialog_record_roundtrip(self):
        from TechVJ.userdata_command import DialogRecord, DialogCategory, TopicRecord
        rec = DialogRecord(
            chat_id=-1001234567890,
            category=DialogCategory.GROUP,
            title="Test Group",
            username="testgroup",
            visibility="PUBLIC",
            description="A test group",
            member_count=500,
            is_forum=True,
            topics=[
                TopicRecord(topic_id=1, title="General"),
                TopicRecord(topic_id=2, title="Off-topic", closed=True),
            ],
        )
        d = rec.to_dict()
        rec2 = DialogRecord.from_dict(d)
        self.assertEqual(rec.chat_id, rec2.chat_id)
        self.assertEqual(rec.category, rec2.category)
        self.assertEqual(len(rec2.topics), 2)
        self.assertEqual(rec2.topics[0].title, "General")
        self.assertTrue(rec2.topics[1].closed)

    def test_dialog_record_with_none_values(self):
        from TechVJ.userdata_command import DialogRecord, DialogCategory
        rec = DialogRecord(
            chat_id=123,
            category=DialogCategory.CHANNEL,
        )
        d = rec.to_dict()
        rec2 = DialogRecord.from_dict(d)
        self.assertIsNone(rec2.description)
        self.assertIsNone(rec2.member_count)
        self.assertEqual(rec2.topics, [])

    def test_inventory_job_remaining(self):
        from TechVJ.userdata_command import InventoryJob
        job = InventoryJob(
            job_id="test",
            target_user_id=123,
            dialogs_discovered=100,
            dialogs_completed=75,
        )
        self.assertEqual(job.dialogs_remaining(), 25)

    def test_inventory_job_remaining_zero(self):
        from TechVJ.userdata_command import InventoryJob
        job = InventoryJob(
            job_id="test",
            target_user_id=123,
            dialogs_discovered=50,
            dialogs_completed=50,
        )
        self.assertEqual(job.dialogs_remaining(), 0)


class TestFormatElapsed(unittest.TestCase):
    """Test elapsed time formatting."""

    def test_seconds_only(self):
        from TechVJ.userdata_command import _format_elapsed
        self.assertEqual(_format_elapsed(45), "00:00:45")

    def test_minutes_and_seconds(self):
        from TechVJ.userdata_command import _format_elapsed
        self.assertEqual(_format_elapsed(125), "00:02:05")

    def test_hours(self):
        from TechVJ.userdata_command import _format_elapsed
        self.assertEqual(_format_elapsed(3661), "01:01:01")


class TestReportGeneration(unittest.TestCase):
    """Test TXT report generation."""

    def setUp(self):
        self.maxDiff = None

    def test_report_creates_file(self):
        from TechVJ.userdata_command import (
            DialogCategory,
            DialogRecord,
            InspectionStatus,
            InventoryJob,
            TopicRecord,
            _generate_report,
        )

        job = InventoryJob(
            job_id="test-report-job-123",
            target_user_id=12345,
            dialogs_discovered=4,
            dialogs_completed=4,
            channels_inspected=1,
            groups_inspected=1,
            bots_inspected=1,
            private_chats_inspected=1,
            topics_discovered=2,
        )
        job.records = [
            DialogRecord(
                chat_id=-1001111111111,
                category=DialogCategory.CHANNEL,
                title="News Channel",
                username="newschannel",
                visibility="PUBLIC",
                description="Latest news",
                member_count=10000,
                latest_message_id=500,
                latest_public_link="https://t.me/newschannel/500",
                has_discussion=True,
                discussion_group_id=-1002222222222,
                discussion_group_title="News Discussion",
                comment_availability="YES",
            ),
            DialogRecord(
                chat_id=-1002222222222,
                category=DialogCategory.GROUP,
                title="Dev Group",
                visibility="PRIVATE",
                member_count=100,
                is_forum=True,
                topics=[
                    TopicRecord(topic_id=1, title="General", top_message=200),
                    TopicRecord(topic_id=5, title="Bugs", closed=True, top_message=150),
                ],
            ),
            DialogRecord(
                chat_id=777,
                category=DialogCategory.BOT,
                title="Helper Bot",
                username="helperbot",
                chat_type="ChatType.BOT",
            ),
            DialogRecord(
                chat_id=888,
                category=DialogCategory.PRIVATE,
                title="John Doe",
                username="johndoe",
                is_bot=False,
                chat_type="ChatType.PRIVATE",
            ),
        ]

        report_path = asyncio.run(_generate_report(job))

        self.assertIsNotNone(report_path)
        self.assertTrue(os.path.exists(report_path))

        with open(report_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Verify structure
        self.assertIn("TELEGRAM USER DATA INVENTORY", content)
        self.assertIn("Target User ID: 12345", content)
        self.assertIn("Scan Status: COMPLETE", content)
        self.assertIn("CHANNELS", content)
        self.assertIn("News Channel", content)
        self.assertIn("https://t.me/newschannel/500", content)
        self.assertIn("GROUPS AND SUPERGROUPS", content)
        self.assertIn("Dev Group", content)
        self.assertIn("General", content)
        self.assertIn("[CLOSED]", content)
        self.assertIn("BOTS", content)
        self.assertIn("Helper Bot", content)
        self.assertIn("PRIVATE USER CHATS", content)
        self.assertIn("John Doe", content)
        self.assertIn("END OF REPORT", content)

        # Verify no secrets
        self.assertNotIn("session", content.lower().split("inspection")[0])

        # Cleanup
        try:
            os.remove(report_path)
        except Exception:
            pass

    def test_report_partial_status_on_failures(self):
        from TechVJ.userdata_command import (
            DialogCategory,
            DialogRecord,
            InspectionStatus,
            InventoryJob,
            _generate_report,
        )

        job = InventoryJob(
            job_id="test-partial",
            target_user_id=99999,
            dialogs_discovered=2,
            dialogs_completed=1,
        )
        job.records = [
            DialogRecord(
                chat_id=111,
                category=DialogCategory.CHANNEL,
                title="Working Channel",
            ),
        ]
        job.failures = [
            {"chat_id": 222, "stage": "inspection", "reason": "ChannelPrivate"},
        ]

        report_path = asyncio.run(_generate_report(job))

        self.assertIsNotNone(report_path)
        with open(report_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("Scan Status: PARTIAL", content)
        self.assertIn("INCOMPLETE OR FAILED INSPECTIONS", content)
        self.assertIn("ChannelPrivate", content)

        try:
            os.remove(report_path)
        except Exception:
            pass

    def test_report_utf8_encoding(self):
        from TechVJ.userdata_command import (
            DialogCategory,
            DialogRecord,
            InventoryJob,
            _generate_report,
        )

        job = InventoryJob(
            job_id="test-utf8",
            target_user_id=11111,
            dialogs_discovered=1,
            dialogs_completed=1,
        )
        job.records = [
            DialogRecord(
                chat_id=100,
                category=DialogCategory.CHANNEL,
                title="Канал Новости 🇷🇺",
                description="Описание канала с эмодзи 🎉",
            ),
        ]

        report_path = asyncio.run(_generate_report(job))

        self.assertIsNotNone(report_path)
        with open(report_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("Канал Новости 🇷🇺", content)
        self.assertIn("🎉", content)

        try:
            os.remove(report_path)
        except Exception:
            pass


class TestDeduplication(unittest.TestCase):
    """Test that resume properly deduplicates."""

    def test_completed_chat_ids_set(self):
        from TechVJ.userdata_command import InventoryJob
        job = InventoryJob(
            job_id="test-dedup",
            target_user_id=123,
        )
        job.completed_chat_ids.add(100)
        job.completed_chat_ids.add(200)
        job.completed_chat_ids.add(100)  # Duplicate
        self.assertEqual(len(job.completed_chat_ids), 2)

    def test_completed_ids_json_roundtrip(self):
        """Verify completed_chat_ids survive JSON serialization."""
        ids = {100, 200, 300}
        serialized = json.dumps(list(ids))
        deserialized = set(json.loads(serialized))
        self.assertEqual(ids, deserialized)


class TestJobStatusValues(unittest.TestCase):
    """Test JobStatus enum values."""

    def test_all_statuses_are_strings(self):
        from TechVJ.userdata_command import JobStatus
        for status in JobStatus:
            self.assertIsInstance(status.value, str)

    def test_terminal_statuses(self):
        from TechVJ.userdata_command import JobStatus
        terminal = {JobStatus.COMPLETE, JobStatus.PARTIAL, JobStatus.CANCELLED, JobStatus.FAILED}
        self.assertEqual(len(terminal), 4)


class TestBotInspection(unittest.TestCase):
    """Test bot dialog inspection."""

    def test_bot_record(self):
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import DialogCategory, InspectionStatus, _inspect_bot

        chat = _make_chat(12345, ChatType.BOT, first_name="TestBot", username="testbot")
        dialog = _make_dialog(chat)

        record = asyncio.run(_inspect_bot(chat, dialog))

        self.assertEqual(record.chat_id, 12345)
        self.assertEqual(record.category, DialogCategory.BOT)
        self.assertEqual(record.title, "TestBot")
        self.assertEqual(record.username, "testbot")
        self.assertTrue(record.is_bot)
        self.assertEqual(record.inspection_status, InspectionStatus.COMPLETE)


class TestPrivateChatInspection(unittest.TestCase):
    """Test private chat dialog inspection."""

    def test_private_record(self):
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import DialogCategory, InspectionStatus, _inspect_private

        chat = _make_chat(
            67890, ChatType.PRIVATE,
            first_name="Jane", last_name="Smith",
            username="janesmith",
        )
        dialog = _make_dialog(chat)

        record = asyncio.run(_inspect_private(chat, dialog))

        self.assertEqual(record.chat_id, 67890)
        self.assertEqual(record.category, DialogCategory.PRIVATE)
        self.assertEqual(record.title, "Jane Smith")
        self.assertEqual(record.username, "janesmith")
        self.assertFalse(record.is_bot)
        self.assertEqual(record.inspection_status, InspectionStatus.COMPLETE)

    def test_private_no_last_name(self):
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import _inspect_private

        chat = _make_chat(11111, ChatType.PRIVATE, first_name="Alice")
        dialog = _make_dialog(chat)

        record = asyncio.run(_inspect_private(chat, dialog))

        self.assertEqual(record.title, "Alice")


class TestSafeStr(unittest.TestCase):
    """Test _safe_str helper."""

    def test_none_returns_default(self):
        from TechVJ.userdata_command import _safe_str
        self.assertEqual(_safe_str(None), "UNKNOWN")
        self.assertEqual(_safe_str(None, "N/A"), "N/A")

    def test_value_returns_string(self):
        from TechVJ.userdata_command import _safe_str
        self.assertEqual(_safe_str(42), "42")
        self.assertEqual(_safe_str("hello"), "hello")


class TestReadOnlySafety(unittest.TestCase):
    """Ensure the inventory never sends messages or /start to bots or contacts."""

    def test_bot_inspection_is_read_only(self):
        """Verify _inspect_bot does NOT call any send methods."""
        from pyrogram.enums import ChatType
        from TechVJ.userdata_command import _inspect_bot

        chat = _make_chat(999, ChatType.BOT, first_name="Bot")
        dialog = _make_dialog(chat)

        # _inspect_bot is sync-like (no client needed) — if it tried to send
        # something, it would need a client argument. Verify signature.
        import inspect
        sig = inspect.signature(_inspect_bot)
        params = list(sig.parameters.keys())
        # Should NOT have a client parameter
        self.assertNotIn("client", params)
        self.assertNotIn("user_client", params)

    def test_private_inspection_is_read_only(self):
        """Verify _inspect_private does NOT call any send methods."""
        import inspect
        from TechVJ.userdata_command import _inspect_private
        sig = inspect.signature(_inspect_private)
        params = list(sig.parameters.keys())
        self.assertNotIn("client", params)
        self.assertNotIn("user_client", params)


if __name__ == "__main__":
    unittest.main()
