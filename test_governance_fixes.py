import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from core.copy_utils import get_bot_copy_source_chat_id, get_bot_real_message_id
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


def test_copy_utils_resolves_location_and_venue_fingerprints():
    async def run():
        sent_location = SimpleNamespace(
            id=10,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=777),
            from_user=SimpleNamespace(id=333),
            location=SimpleNamespace(latitude=41.311081, longitude=69.240562),
            caption=None,
        )
        bot_location = SimpleNamespace(
            id=20,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=333),
            from_user=SimpleNamespace(id=333),
            location=SimpleNamespace(latitude=41.311081, longitude=69.240562),
            caption=None,
        )
        sent_venue = SimpleNamespace(
            id=11,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=777),
            from_user=SimpleNamespace(id=333),
            venue=SimpleNamespace(
                location=SimpleNamespace(latitude=41.311081, longitude=69.240562),
                title="Tashkent",
                address="Center",
            ),
            caption=None,
        )
        bot_venue = SimpleNamespace(
            id=21,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=333),
            from_user=SimpleNamespace(id=333),
            venue=SimpleNamespace(
                location=SimpleNamespace(latitude=41.311081, longitude=69.240562),
                title="Tashkent",
                address="Center",
            ),
            caption=None,
        )

        class FakeBot:
            def __init__(self, items):
                self.items = items

            def get_chat_history(self, chat_id, limit=80):
                async def gen():
                    for item in self.items:
                        yield item

                return gen()

        assert await get_bot_real_message_id(FakeBot([bot_location]), 333, sent_location) == 20
        assert await get_bot_real_message_id(FakeBot([bot_venue]), 333, sent_venue) == 21

    asyncio.run(run())


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
        copied = SimpleNamespace(id=199, chat=SimpleNamespace(id=123))
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
                return copied

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

        assert result is copied
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
            TopicExtractorConfig(
                chat_id=-100123,
                topic_id=topic_id,
                fetch_batch_size=10,
                allow_history_scan_fallback=True,
                use_cache=False,
            ),
        )

        result = await extractor.extract_between(100, 50)

        assert [msg.id for msg in result] == [100, 1000, 50]

    asyncio.run(run())


def test_save_topic_membership_accepts_message_thread_id():
    from TechVJ.save import _message_belongs_to_topic

    topic_id = 15
    msg = SimpleNamespace(
        id=806,
        empty=False,
        message_thread_id=topic_id,
        reply_to_top_message_id=None,
        reply_to_message_id=None,
    )

    assert _message_belongs_to_topic(msg, topic_id)


def test_topic_extractor_range_stops_after_anchors_found():
    async def run():
        from core.topic_extractor import TopicExtractor, TopicExtractorConfig

        topic_id = 5
        pages = [
            [
                SimpleNamespace(id=400, date=datetime(2024, 1, 5, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
                SimpleNamespace(id=50, date=datetime(2024, 1, 4, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            ],
            [
                SimpleNamespace(id=1000, date=datetime(2024, 1, 3, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
                SimpleNamespace(id=100, date=datetime(2024, 1, 2, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            ],
            [
                SimpleNamespace(id=5, date=datetime(2024, 1, 1, tzinfo=timezone.utc), reply_to_top_message_id=None),
            ],
        ]

        class FakeClient:
            def __init__(self):
                self.calls = 0

            def get_chat_history(self, chat_id, **kwargs):
                async def gen():
                    idx = self.calls
                    self.calls += 1
                    for msg in pages[idx]:
                        yield msg

                return gen()

        client = FakeClient()
        extractor = TopicExtractor(
            client,
            TopicExtractorConfig(
                chat_id=-100123,
                topic_id=topic_id,
                fetch_batch_size=2,
                stop_after_ids={100, 50},
                allow_history_scan_fallback=True,
                use_cache=False,
            ),
        )

        result = await extractor.extract_between(100, 50)

        assert [msg.id for msg in result] == [100, 1000, 50]
        assert client.calls == 2

    asyncio.run(run())


def test_topic_extractor_returns_collected_topic_messages_when_end_anchor_missing():
    async def run():
        from core.topic_extractor import TopicExtractor, TopicExtractorConfig

        topic_id = 8
        messages = [
            SimpleNamespace(id=171769, date=datetime(2024, 1, 4, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            SimpleNamespace(id=1000, date=datetime(2024, 1, 3, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            SimpleNamespace(id=75, date=datetime(2024, 1, 2, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            SimpleNamespace(id=8, date=datetime(2024, 1, 1, tzinfo=timezone.utc), reply_to_top_message_id=None),
        ]

        class FakeClient:
            def get_chat_history(self, chat_id, **kwargs):
                async def gen():
                    for msg in messages:
                        yield msg

                return gen()

        extractor = TopicExtractor(
            FakeClient(),
            TopicExtractorConfig(
                chat_id=-1002234521267,
                topic_id=topic_id,
                fetch_batch_size=10,
                allow_history_scan_fallback=True,
                use_cache=False,
            ),
        )

        result = await extractor.extract_between(75, 171796)

        assert [msg.id for msg in result] == [75, 1000, 171769]

    asyncio.run(run())


def test_topic_extractor_hydrates_existing_missing_range_anchor():
    async def run():
        from core.topic_extractor import TopicExtractor, TopicExtractorConfig

        topic_id = 8
        messages = [
            SimpleNamespace(id=171769, date=datetime(2024, 1, 4, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            SimpleNamespace(id=1000, date=datetime(2024, 1, 3, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            SimpleNamespace(id=75, date=datetime(2024, 1, 2, tzinfo=timezone.utc), reply_to_top_message_id=topic_id),
            SimpleNamespace(id=8, date=datetime(2024, 1, 1, tzinfo=timezone.utc), reply_to_top_message_id=None),
        ]
        hydrated = SimpleNamespace(
            id=171796,
            date=datetime(2024, 1, 5, tzinfo=timezone.utc),
            reply_to_top_message_id=topic_id,
            empty=False,
        )

        class FakeClient:
            def __init__(self):
                self.get_message_ids = []

            def get_chat_history(self, chat_id, **kwargs):
                async def gen():
                    for msg in messages:
                        yield msg

                return gen()

            async def get_messages(self, chat_id, ids, **kwargs):
                if not isinstance(ids, list):
                    ids = [ids]
                self.get_message_ids.extend(ids)
                return [hydrated for msg_id in ids if msg_id == hydrated.id]

        client = FakeClient()
        extractor = TopicExtractor(
            client,
            TopicExtractorConfig(
                chat_id=-1002234521267,
                topic_id=topic_id,
                fetch_batch_size=10,
                allow_history_scan_fallback=True,
                use_cache=False,
            ),
        )

        result = await extractor.extract_between(75, 171796)

        assert [msg.id for msg in result] == [75, 1000, 171769, 171796]
        assert 171796 in client.get_message_ids

    asyncio.run(run())


def test_topic_extractor_uses_raw_search_when_thread_filter_missing():
    async def run():
        from pyrogram import raw
        from core.topic_extractor import TopicExtractor, TopicExtractorConfig

        topic_id = 5
        pages = [
            raw.types.messages.Messages(
                messages=[
                    raw.types.Message(
                        id=50,
                        peer_id=raw.types.PeerChannel(channel_id=123),
                        date=int(datetime(2024, 1, 4, tzinfo=timezone.utc).timestamp()),
                        message="last",
                        out=False,
                        mentioned=False,
                        media_unread=False,
                        silent=False,
                        post=False,
                        from_scheduled=False,
                        legacy=False,
                        edit_hide=False,
                        pinned=False,
                        noforwards=False,
                        invert_media=False,
                        offline=False,
                        from_id=raw.types.PeerUser(user_id=10),
                        reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=topic_id, reply_to_top_id=topic_id),
                    ),
                    raw.types.Message(
                        id=1000,
                        peer_id=raw.types.PeerChannel(channel_id=123),
                        date=int(datetime(2024, 1, 3, tzinfo=timezone.utc).timestamp()),
                        message="middle",
                        out=False,
                        mentioned=False,
                        media_unread=False,
                        silent=False,
                        post=False,
                        from_scheduled=False,
                        legacy=False,
                        edit_hide=False,
                        pinned=False,
                        noforwards=False,
                        invert_media=False,
                        offline=False,
                        from_id=raw.types.PeerUser(user_id=10),
                        reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=topic_id, reply_to_top_id=topic_id),
                    ),
                ],
                topics=[],
                chats=[raw.types.Channel(id=123, title="Forum", photo=raw.types.ChatPhotoEmpty(), date=0, creator=False, left=False, broadcast=False, verified=False, megagroup=True, restricted=False, signatures=False, min=False, scam=False, has_link=False, has_geo=False, slowmode_enabled=False, call_active=False, call_not_empty=False, fake=False, gigagroup=False, noforwards=False, join_to_send=False, join_request=False, forum=True, stories_hidden=False, stories_hidden_min=False, stories_unavailable=False, signature_profiles=False, autotranslation=False, broadcast_messages_allowed=False, monoforum=False, forum_tabs=False, access_hash=1)],
                users=[raw.types.User(id=10, is_self=False, contact=False, mutual_contact=False, deleted=False, bot=False, bot_chat_history=False, bot_nochats=False, verified=False, restricted=False, min=False, bot_inline_geo=False, support=False, scam=False, apply_min_photo=True, fake=False, bot_attach_menu=False, premium=False, attach_menu_enabled=False, bot_can_edit=False, close_friend=False, stories_hidden=False, stories_unavailable=False, contact_require_premium=False, bot_business=False, bot_has_main_app=False, first_name="A")],
            ),
            raw.types.messages.Messages(
                messages=[
                    raw.types.Message(
                        id=100,
                        peer_id=raw.types.PeerChannel(channel_id=123),
                        date=int(datetime(2024, 1, 2, tzinfo=timezone.utc).timestamp()),
                        message="first",
                        out=False,
                        mentioned=False,
                        media_unread=False,
                        silent=False,
                        post=False,
                        from_scheduled=False,
                        legacy=False,
                        edit_hide=False,
                        pinned=False,
                        noforwards=False,
                        invert_media=False,
                        offline=False,
                        from_id=raw.types.PeerUser(user_id=10),
                        reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=topic_id, reply_to_top_id=topic_id),
                    ),
                ],
                topics=[],
                chats=[raw.types.Channel(id=123, title="Forum", photo=raw.types.ChatPhotoEmpty(), date=0, creator=False, left=False, broadcast=False, verified=False, megagroup=True, restricted=False, signatures=False, min=False, scam=False, has_link=False, has_geo=False, slowmode_enabled=False, call_active=False, call_not_empty=False, fake=False, gigagroup=False, noforwards=False, join_to_send=False, join_request=False, forum=True, stories_hidden=False, stories_hidden_min=False, stories_unavailable=False, signature_profiles=False, autotranslation=False, broadcast_messages_allowed=False, monoforum=False, forum_tabs=False, access_hash=1)],
                users=[raw.types.User(id=10, is_self=False, contact=False, mutual_contact=False, deleted=False, bot=False, bot_chat_history=False, bot_nochats=False, verified=False, restricted=False, min=False, bot_inline_geo=False, support=False, scam=False, apply_min_photo=True, fake=False, bot_attach_menu=False, premium=False, attach_menu_enabled=False, bot_can_edit=False, close_friend=False, stories_hidden=False, stories_unavailable=False, contact_require_premium=False, bot_business=False, bot_has_main_app=False, first_name="A")],
            ),
        ]

        class FakeClient:
            def __init__(self):
                self.history_calls = 0
                self.invoke_calls = 0
                self.message_cache = {}

            def get_chat_history(self, chat_id, **kwargs):
                self.history_calls += 1
                raise TypeError("get_chat_history() got an unexpected keyword argument 'message_thread_id'")

            async def resolve_peer(self, chat_id):
                return raw.types.InputPeerChannel(channel_id=123, access_hash=1)

            async def invoke(self, query, **kwargs):
                assert isinstance(query, raw.functions.messages.Search)
                assert query.top_msg_id == topic_id
                assert query.add_offset == self.invoke_calls * 2
                assert query.offset_id == 0
                result = pages[self.invoke_calls]
                self.invoke_calls += 1
                return result

            async def get_messages(self, chat_id, message_ids, **kwargs):
                return SimpleNamespace(id=topic_id, date=datetime(2024, 1, 1, tzinfo=timezone.utc), reply_to_top_message_id=None, empty=False)

        client = FakeClient()
        extractor = TopicExtractor(
            client,
            TopicExtractorConfig(
                chat_id=-100123,
                topic_id=topic_id,
                fetch_batch_size=2,
                stop_after_ids={100, 50},
                use_cache=False,
            ),
        )

        result = await extractor.extract_between(100, 50)

        assert [msg.id for msg in result] == [100, 1000, 50]
        assert client.history_calls == 0
        assert client.invoke_calls == 2

    asyncio.run(run())


def test_topic_extractor_falls_back_to_raw_get_replies_when_search_missing():
    async def run():
        from pyrogram import raw
        from core.topic_extractor import TopicExtractor, TopicExtractorConfig

        topic_id = 5

        def raw_message(message_id, text, day):
            return raw.types.Message(
                id=message_id,
                peer_id=raw.types.PeerChannel(channel_id=123),
                date=int(datetime(2024, 1, day, tzinfo=timezone.utc).timestamp()),
                message=text,
                out=False,
                mentioned=False,
                media_unread=False,
                silent=False,
                post=False,
                from_scheduled=False,
                legacy=False,
                edit_hide=False,
                pinned=False,
                noforwards=False,
                invert_media=False,
                offline=False,
                from_id=raw.types.PeerUser(user_id=10),
                reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=topic_id, reply_to_top_id=topic_id),
            )

        channel = raw.types.Channel(id=123, title="Forum", photo=raw.types.ChatPhotoEmpty(), date=0, creator=False, left=False, broadcast=False, verified=False, megagroup=True, restricted=False, signatures=False, min=False, scam=False, has_link=False, has_geo=False, slowmode_enabled=False, call_active=False, call_not_empty=False, fake=False, gigagroup=False, noforwards=False, join_to_send=False, join_request=False, forum=True, stories_hidden=False, stories_hidden_min=False, stories_unavailable=False, signature_profiles=False, autotranslation=False, broadcast_messages_allowed=False, monoforum=False, forum_tabs=False, access_hash=1)
        user = raw.types.User(id=10, is_self=False, contact=False, mutual_contact=False, deleted=False, bot=False, bot_chat_history=False, bot_nochats=False, verified=False, restricted=False, min=False, bot_inline_geo=False, support=False, scam=False, apply_min_photo=True, fake=False, bot_attach_menu=False, premium=False, attach_menu_enabled=False, bot_can_edit=False, close_friend=False, stories_hidden=False, stories_unavailable=False, contact_require_premium=False, bot_business=False, bot_has_main_app=False, first_name="A")

        search_page = raw.types.messages.Messages(
            messages=[],
            topics=[],
            chats=[channel],
            users=[user],
        )
        replies_page = raw.types.messages.Messages(
            messages=[
                raw_message(50, "last", 4),
                raw_message(1000, "middle", 3),
                raw_message(100, "first", 2),
            ],
            topics=[],
            chats=[channel],
            users=[user],
        )

        class FakeClient:
            def __init__(self):
                self.history_calls = 0
                self.search_calls = 0
                self.reply_calls = 0
                self.message_cache = {}

            def get_chat_history(self, chat_id, **kwargs):
                self.history_calls += 1
                raise TypeError("get_chat_history() got an unexpected keyword argument 'message_thread_id'")

            async def resolve_peer(self, chat_id):
                return raw.types.InputPeerChannel(channel_id=123, access_hash=1)

            async def invoke(self, query, **kwargs):
                if isinstance(query, raw.functions.messages.Search):
                    self.search_calls += 1
                    return search_page
                assert isinstance(query, raw.functions.messages.GetReplies)
                self.reply_calls += 1
                return replies_page

            async def get_messages(self, chat_id, message_ids, **kwargs):
                return SimpleNamespace(id=topic_id, date=datetime(2024, 1, 1, tzinfo=timezone.utc), reply_to_top_message_id=None, empty=False)

        client = FakeClient()
        extractor = TopicExtractor(
            client,
            TopicExtractorConfig(
                chat_id=-100123,
                topic_id=topic_id,
                fetch_batch_size=10,
                stop_after_ids={100, 50},
                use_cache=False,
            ),
        )

        result = await extractor.extract_between(100, 50)

        assert [msg.id for msg in result] == [100, 1000, 50]
        assert client.history_calls == 0
        assert client.search_calls == 1
        assert client.reply_calls == 1

    asyncio.run(run())


def test_topic_extractor_serves_range_from_topic_cache():
    async def run():
        import tempfile
        from pyrogram import raw
        import core.topic_extractor as topic_extractor_module
        from core.topic_cache import TopicCache, TopicCacheEntry
        from core.topic_extractor import TopicExtractor, TopicExtractorConfig

        topic_id = 5
        tmp = tempfile.TemporaryDirectory()
        cache = TopicCache(f"{tmp.name}/topics.json")
        cache.put(
            TopicCacheEntry(
                chat_id=-100123,
                topic_id=topic_id,
                root_message_id=topic_id,
                known_message_ids=[topic_id, 100, 1000, 50],
                last_processed_message_id=50,
                fully_scanned=True,
            )
        )

        def raw_message(message_id, text, day):
            return raw.types.Message(
                id=message_id,
                peer_id=raw.types.PeerChannel(channel_id=123),
                date=int(datetime(2024, 1, day, tzinfo=timezone.utc).timestamp()),
                message=text,
                out=False,
                mentioned=False,
                media_unread=False,
                silent=False,
                post=False,
                from_scheduled=False,
                legacy=False,
                edit_hide=False,
                pinned=False,
                noforwards=False,
                invert_media=False,
                offline=False,
                from_id=raw.types.PeerUser(user_id=10),
                reply_to=raw.types.MessageReplyHeader(reply_to_msg_id=topic_id, reply_to_top_id=topic_id),
            )

        channel = raw.types.Channel(id=123, title="Forum", photo=raw.types.ChatPhotoEmpty(), date=0, creator=False, left=False, broadcast=False, verified=False, megagroup=True, restricted=False, signatures=False, min=False, scam=False, has_link=False, has_geo=False, slowmode_enabled=False, call_active=False, call_not_empty=False, fake=False, gigagroup=False, noforwards=False, join_to_send=False, join_request=False, forum=True, stories_hidden=False, stories_hidden_min=False, stories_unavailable=False, signature_profiles=False, autotranslation=False, broadcast_messages_allowed=False, monoforum=False, forum_tabs=False, access_hash=1)
        user = raw.types.User(id=10, is_self=False, contact=False, mutual_contact=False, deleted=False, bot=False, bot_chat_history=False, bot_nochats=False, verified=False, restricted=False, min=False, bot_inline_geo=False, support=False, scam=False, apply_min_photo=True, fake=False, bot_attach_menu=False, premium=False, attach_menu_enabled=False, bot_can_edit=False, close_friend=False, stories_hidden=False, stories_unavailable=False, contact_require_premium=False, bot_business=False, bot_has_main_app=False, first_name="A")

        class FakeClient:
            def __init__(self):
                self.history_calls = 0
                self.search_calls = 0
                self.get_messages_calls = 0
                self.message_cache = {}

            def get_chat_history(self, chat_id, **kwargs):
                self.history_calls += 1
                raise AssertionError("history should not be used on cache hit")

            async def resolve_peer(self, chat_id):
                return raw.types.InputPeerChannel(channel_id=123, access_hash=1)

            async def invoke(self, query, **kwargs):
                if isinstance(query, raw.functions.messages.Search):
                    self.search_calls += 1
                    raise AssertionError("search should not be used on cache hit")
                assert isinstance(query, raw.functions.channels.GetMessages)
                self.get_messages_calls += 1
                requested = [item.id for item in query.id]
                by_id = {
                    topic_id: raw_message(topic_id, "root", 1),
                    100: raw_message(100, "first", 2),
                    1000: raw_message(1000, "middle", 3),
                    50: raw_message(50, "last", 4),
                }
                return raw.types.messages.Messages(
                    messages=[by_id[msg_id] for msg_id in requested],
                    topics=[],
                    chats=[channel],
                    users=[user],
                )

        original_cache = topic_extractor_module.topic_cache
        topic_extractor_module.topic_cache = cache
        try:
            client = FakeClient()
            extractor = TopicExtractor(
                client,
                TopicExtractorConfig(
                    chat_id=-100123,
                    topic_id=topic_id,
                    fetch_batch_size=10,
                    stop_after_ids={100, 50},
                ),
            )

            result = await extractor.extract_between(100, 50)

            assert [msg.id for msg in result] == [100, 1000, 50]
            assert client.history_calls == 0
            assert client.search_calls == 0
            assert client.get_messages_calls == 1
        finally:
            topic_extractor_module.topic_cache = original_cache
            tmp.cleanup()

    asyncio.run(run())


def test_retry_utils_detects_flood_premium_wait_text():
    from core.retry_utils import get_floodwait_seconds, is_floodwait_error

    class FloodPremiumWait(Exception):
        pass

    err = FloodPremiumWait(
        'Telegram says: [420 FLOOD_PREMIUM_WAIT_X] '
        '(caused by "upload.SaveBigFilePart") '
        'Pyrogram thinks: A wait of 11 seconds is required'
    )

    assert is_floodwait_error(err)
    assert get_floodwait_seconds(err) == 11
