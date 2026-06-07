import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from core.copy_utils import get_bot_copy_source_chat_id
from core.channel_monitor import MonitoredChannel, channel_monitor
from core.failure_classifier import (
    FailureCategory,
    classify_failure,
    is_reportable_to_owner,
    should_notify_user,
)
from core.permission_guard import permission_guard
from core.priority_queue import PriorityJob, PriorityQueue
from core.rate_limiter import RateLimiter
from core.restricted_channel_guard import validate_restricted_channel_id
from core.role_manager import UserRole
from core.session_manager.models import SessionRecord, SessionType
from core.session_manager.session_manager import SessionManager


def test_rate_limiter_zero_limit_does_not_crash():
    limiter = RateLimiter()
    limiter.set_limit(UserRole.NEW_USER, 0, window_seconds=10)

    allowed, retry_after = limiter.check(123, UserRole.NEW_USER)

    assert allowed is False
    assert retry_after == 10


def test_permission_guard_rejects_empty_post_ids_for_new_user():
    parsed = SimpleNamespace(post_ids=[])

    allowed, message = permission_guard.validate(parsed, UserRole.NEW_USER)

    assert allowed is False
    assert message


def test_permission_guard_allows_exactly_one_post_for_new_user():
    parsed = SimpleNamespace(post_ids=[101])

    allowed, message = permission_guard.validate(parsed, UserRole.NEW_USER)

    assert allowed is True
    assert message == ""


def test_restricted_channel_guard_rejects_enabled_monitored_channel_for_user():
    original = dict(channel_monitor._channels)
    try:
        channel_monitor._channels = {
            -1001234567890: MonitoredChannel(
                channel_id=-1001234567890,
                target_chat_id=999,
                enabled=True,
            )
        }

        allowed, message = validate_restricted_channel_id(
            -1001234567890,
            user_id=111,
            owner_id=222,
        )

        assert allowed is False
        assert "taqiqlangan" in message
    finally:
        channel_monitor._channels = original


def test_restricted_channel_guard_allows_owner_for_monitored_channel():
    original = dict(channel_monitor._channels)
    try:
        channel_monitor._channels = {
            -1001234567890: MonitoredChannel(
                channel_id=-1001234567890,
                target_chat_id=999,
                enabled=True,
            )
        }

        allowed, message = validate_restricted_channel_id(
            -1001234567890,
            user_id=222,
            owner_id=222,
        )

        assert allowed is True
        assert message == ""
    finally:
        channel_monitor._channels = original


def test_restricted_channel_guard_ignores_disabled_monitored_channel():
    original = dict(channel_monitor._channels)
    try:
        channel_monitor._channels = {
            -1001234567890: MonitoredChannel(
                channel_id=-1001234567890,
                target_chat_id=999,
                enabled=False,
            )
        }

        allowed, message = validate_restricted_channel_id(
            -1001234567890,
            user_id=111,
            owner_id=222,
        )

        assert allowed is True
        assert message == ""
    finally:
        channel_monitor._channels = original


def test_failure_classifier_deleted_post_is_silent_expected_state():
    category = classify_failure("fetch", "post is deleted or inaccessible")

    assert category == FailureCategory.EXPECTED_TELEGRAM_STATE
    assert should_notify_user(category) is False
    assert is_reportable_to_owner(
        category,
        message_fetched=False,
        processing_started=False,
        system_exception=False,
    ) is False


def test_failure_classifier_album_missing_does_not_page_owner():
    category = classify_failure("topic_album", "message not found")

    assert category == FailureCategory.EXPECTED_TELEGRAM_STATE
    assert is_reportable_to_owner(
        category,
        message_fetched=False,
        processing_started=True,
        system_exception=False,
    ) is False


def test_failure_classifier_upload_failure_pages_owner_after_fetch():
    category = classify_failure("send_media", "download_and_send_media returned False")

    assert category == FailureCategory.SYSTEM_FAILURE
    assert should_notify_user(category) is True
    assert is_reportable_to_owner(
        category,
        message_fetched=True,
        processing_started=True,
        system_exception=False,
    ) is True


def test_failure_classifier_peer_id_invalid_is_system_failure():
    category = classify_failure("public_bot_copy", "copy failed", RuntimeError("PEER_ID_INVALID"))

    assert category == FailureCategory.SYSTEM_FAILURE
    assert is_reportable_to_owner(
        category,
        message_fetched=False,
        processing_started=True,
        system_exception=True,
    ) is True


def test_copy_source_prefers_sender_user_id():
    sent = SimpleNamespace(
        chat=SimpleNamespace(id=999_000),
        from_user=SimpleNamespace(id=111),
    )

    assert get_bot_copy_source_chat_id(sent) == 111


def test_copy_source_falls_back_to_message_chat_id():
    sent = SimpleNamespace(chat=SimpleNamespace(id=222), from_user=None)

    assert get_bot_copy_source_chat_id(sent) == 222


def test_priority_queue_spills_over_idle_capacity():
    async def run():
        pq = PriorityQueue()
        pq._init_queues()
        pq._limits = {
            UserRole.VIP_USER: 1,
            UserRole.NORMAL_USER: 1,
            UserRole.NEW_USER: 1,
        }

        for idx in range(3):
            await pq._queues[UserRole.VIP_USER].put(
                PriorityJob(
                    user_id=idx,
                    role=UserRole.VIP_USER,
                    parsed_url=None,
                    message=None,
                    handler=None,
                    client=None,
                )
            )

        picked = [await pq._pick_and_acquire() for _ in range(3)]

        assert all(item is not None for item in picked)
        assert [role for _, role in picked] == [UserRole.VIP_USER] * 3
        assert pq._active[UserRole.VIP_USER] == 3

    asyncio.run(run())


def test_session_manager_pool_copy_uses_sender_chat_and_deletes_after_success():
    async def run():
        sent = SimpleNamespace(
            id=10,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=777),
            from_user=SimpleNamespace(id=222),
            document=SimpleNamespace(file_unique_id="u1", file_size=123, file_name="a.bin"),
            caption=None,
        )
        bot_side = SimpleNamespace(
            id=99,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=222),
            from_user=SimpleNamespace(id=222),
            document=SimpleNamespace(file_unique_id="u1", file_size=123, file_name="a.bin"),
            caption=None,
        )
        old_bot_side = SimpleNamespace(
            id=50,
            date=datetime(2023, 12, 31, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=222),
            from_user=SimpleNamespace(id=222),
            document=SimpleNamespace(file_unique_id="u1", file_size=123, file_name="a.bin"),
            caption=None,
        )

        class FakeBridge:
            def build_send_fn(self, **kwargs):
                return lambda client: None

            async def get_worker(self, record, user_id):
                return SimpleNamespace(session_user_id=222)

            async def enqueue_task(self, worker, send_fn, is_media=True, owner_user_id=None):
                assert owner_user_id == 123
                return sent

        class FakeBot:
            def __init__(self):
                self.me = SimpleNamespace(id=777)
                self.copy_calls = []
                self.delete_calls = []

            async def copy_message(self, **kwargs):
                self.copy_calls.append(kwargs)

            async def delete_messages(self, chat_id, message_ids):
                self.delete_calls.append((chat_id, message_ids))

            def get_chat_history(self, chat_id, limit=1):
                async def gen():
                    if limit == 1:
                        yield old_bot_side
                    else:
                        yield bot_side
                        yield old_bot_side

                return gen()

        manager = SessionManager()
        manager._initialized = True
        manager._bridge = FakeBridge()
        record = SessionRecord(
            session_id="test-copy-success",
            session_string="session",
            phone="",
            type=SessionType.BORROWABLE,
            owner_user_id=999,
            allow_borrow=True,
            max_parallel_tasks=1,
        )
        bot = FakeBot()

        result = await manager.upload_with_session(
            record=record,
            user_id=123,
            target_chat_id=123,
            msg_type="Document",
            file_path="dummy.bin",
            send_kwargs={},
            bot_client=bot,
            bot_user_id=777,
        )

        assert result is sent
        assert bot.copy_calls[0]["from_chat_id"] == 222
        assert bot.copy_calls[0]["message_id"] == 99
        assert bot.delete_calls == [(222, [99])]
        assert record.current_tasks == 0

    asyncio.run(run())


def test_session_manager_pool_copy_failure_returns_none_without_delete():
    async def run():
        sent = SimpleNamespace(
            id=11,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=777),
            from_user=SimpleNamespace(id=333),
            document=SimpleNamespace(file_unique_id="u2", file_size=456, file_name="b.bin"),
            caption=None,
        )
        bot_side = SimpleNamespace(
            id=88,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=333),
            from_user=SimpleNamespace(id=333),
            document=SimpleNamespace(file_unique_id="u2", file_size=456, file_name="b.bin"),
            caption=None,
        )
        old_bot_side = SimpleNamespace(
            id=40,
            date=datetime(2023, 12, 31, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=333),
            from_user=SimpleNamespace(id=333),
            document=SimpleNamespace(file_unique_id="u2", file_size=456, file_name="b.bin"),
            caption=None,
        )

        class FakeBridge:
            def build_send_fn(self, **kwargs):
                return lambda client: None

            async def get_worker(self, record, user_id):
                return SimpleNamespace(session_user_id=333)

            async def enqueue_task(self, worker, send_fn, is_media=True, owner_user_id=None):
                assert owner_user_id == 123
                return sent

        class FakeBot:
            def __init__(self):
                self.me = SimpleNamespace(id=777)
                self.copy_calls = []
                self.delete_calls = []

            async def copy_message(self, **kwargs):
                self.copy_calls.append(kwargs)
                raise RuntimeError("copy failed")

            async def delete_messages(self, chat_id, message_ids):
                self.delete_calls.append((chat_id, message_ids))

            def get_chat_history(self, chat_id, limit=1):
                async def gen():
                    if limit == 1:
                        yield old_bot_side
                    else:
                        yield bot_side
                        yield old_bot_side

                return gen()

        manager = SessionManager()
        manager._initialized = True
        manager._bridge = FakeBridge()
        record = SessionRecord(
            session_id="test-copy-failure",
            session_string="session",
            phone="",
            type=SessionType.GLOBAL,
            owner_user_id=None,
            allow_system_use=True,
            max_parallel_tasks=1,
        )
        bot = FakeBot()

        result = await manager.upload_with_session(
            record=record,
            user_id=123,
            target_chat_id=123,
            msg_type="Document",
            file_path="dummy.bin",
            send_kwargs={},
            bot_client=bot,
            bot_user_id=777,
        )

        assert result is None
        assert len(bot.copy_calls) == 3
        assert all(call["from_chat_id"] == 333 for call in bot.copy_calls)
        assert all(call["message_id"] == 88 for call in bot.copy_calls)
        assert bot.delete_calls == []
        assert record.current_tasks == 0

    asyncio.run(run())


def test_session_manager_pool_copy_does_not_fallback_to_sender_id_when_unresolved():
    async def run():
        sent = SimpleNamespace(
            id=11,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=777),
            from_user=SimpleNamespace(id=333),
            document=SimpleNamespace(file_unique_id="missing", file_size=456, file_name="b.bin"),
            caption=None,
        )
        old_bot_side = SimpleNamespace(
            id=11,
            date=datetime(2023, 12, 31, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=333),
            from_user=SimpleNamespace(id=333),
            document=SimpleNamespace(file_unique_id="other", file_size=999, file_name="old.bin"),
            caption=None,
        )

        class FakeBridge:
            def build_send_fn(self, **kwargs):
                return lambda client: None

            async def get_worker(self, record, user_id):
                return SimpleNamespace(session_user_id=333)

            async def enqueue_task(self, worker, send_fn, is_media=True, owner_user_id=None):
                assert owner_user_id == 333
                return sent

        class FakeBot:
            def __init__(self):
                self.me = SimpleNamespace(id=777)
                self.copy_calls = []
                self.delete_calls = []

            async def copy_message(self, **kwargs):
                self.copy_calls.append(kwargs)

            async def delete_messages(self, chat_id, message_ids):
                self.delete_calls.append((chat_id, message_ids))

            def get_chat_history(self, chat_id, limit=1):
                async def gen():
                    yield old_bot_side

                return gen()

        manager = SessionManager()
        manager._initialized = True
        manager._bridge = FakeBridge()
        record = SessionRecord(
            session_id="test-copy-unresolved",
            session_string="session",
            phone="",
            type=SessionType.GLOBAL,
            owner_user_id=None,
            allow_system_use=True,
            max_parallel_tasks=1,
        )
        bot = FakeBot()

        result = await manager.upload_with_session(
            record=record,
            user_id=123,
            target_chat_id=123,
            msg_type="Document",
            file_path="dummy.bin",
            send_kwargs={},
            bot_client=bot,
            bot_user_id=777,
        )

        assert result is None
        assert bot.copy_calls == []
        assert bot.delete_calls == []
        assert record.current_tasks == 0

    asyncio.run(run())


def test_topic_extractor_range_uses_chronological_topic_order():
    async def run():
        from core.topic_extractor import TopicExtractor, TopicExtractorConfig

        topic_id = 5
        messages = [
            SimpleNamespace(id=50, date=datetime(2024, 1, 4, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            SimpleNamespace(id=1000, date=datetime(2024, 1, 3, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            SimpleNamespace(id=100, date=datetime(2024, 1, 2, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            SimpleNamespace(id=5, date=datetime(2024, 1, 1, tzinfo=timezone.utc), reply_to_top_message_id=None),
        ]

        class FakeClient:
            def get_chat_history(self, chat_id, **kwargs):
                async def gen():
                    for msg in messages:
                        yield msg

                return gen()

        extractor = TopicExtractor(
            FakeClient(),
            TopicExtractorConfig(chat_id=-100123, topic_id=topic_id, fetch_batch_size=10),
        )

        result = await extractor.extract_between(100, 50)

        assert [msg.id for msg in result] == [100, 1000, 50]

    asyncio.run(run())
