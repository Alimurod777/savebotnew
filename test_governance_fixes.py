import asyncio
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from pyrogram.enums import MessageEntityType
from pyrogram.types import MessageEntity

from core.copy_utils import (
    get_bot_copy_source_chat_id,
    get_bot_real_message_id,
    start_bot_upload_update_waiter,
)
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


def test_copy_source_prefers_explicit_fallback_over_bot_chat_id():
    sent = SimpleNamespace(chat=SimpleNamespace(id=999_000), from_user=None)

    assert get_bot_copy_source_chat_id(sent, fallback_chat_id=333) == 333


def test_source_context_message_replies_to_original_link_with_source_name_only():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import _send_source_context_message

        class FakeClient:
            def __init__(self):
                self.kwargs = None

            async def send_message(self, chat_id, text, **kwargs):
                self.kwargs = kwargs
                return SimpleNamespace(id=10, chat=SimpleNamespace(id=chat_id), text=text)

        request = SimpleNamespace(
            id=55,
            chat=SimpleNamespace(id=123),
            reply_to_message_id=None,
        )
        parsed = SimpleNamespace(channel_id=-1001, topic_id=None, thread_id=None)
        client = FakeClient()

        msg = await _send_source_context_message(
            client,
            user_id=123,
            parsed=parsed,
            source_chat=SimpleNamespace(title="Maqsad Club"),
            source_msg=SimpleNamespace(text="Post preview"),
            request_message=request,
        )

        assert msg is not None
        assert msg.text == "📡 Manba: Maqsad Club"
        assert client.kwargs.get("reply_to_message_id") == 55
        assert "reply_parameters" not in client.kwargs

    asyncio.run(run())


def test_send_source_name_message_uses_user_session_title_and_replies():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import send_source_name_message

        class FakeBot:
            def __init__(self):
                self.sent = None

            async def get_chat(self, chat_id):
                raise AssertionError("bot get_chat should not be needed when user session resolves")

            async def send_message(self, chat_id, text, **kwargs):
                self.sent = (chat_id, text, kwargs)
                return SimpleNamespace(id=77, chat=SimpleNamespace(id=chat_id), text=text)

        class FakeAcc:
            async def get_chat(self, chat_id):
                return SimpleNamespace(title="Courses Hub")

        bot = FakeBot()
        msg = await send_source_name_message(
            bot,
            FakeAcc(),
            target_chat_id=123,
            source_chat_id=-1001823169797,
            reply_to_message_id=55,
        )

        assert msg.text == "📡 Manba: Courses Hub"
        assert bot.sent[0] == 123
        assert bot.sent[2].get("reply_to_message_id") == 55

    asyncio.run(run())


def test_split_part_progress_callback_edits_status_with_throttle():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import _create_split_part_progress_callback

        class FakeClient:
            def __init__(self):
                self.edits = []

            async def edit_message_text(self, chat_id, message_id, text):
                self.edits.append((chat_id, message_id, text))

        client = FakeClient()
        callback = _create_split_part_progress_callback(
            client=client,
            chat_id=123,
            status_msg=SimpleNamespace(id=88),
            part_num=2,
            total_parts=4,
            part_size=10 * 1024 * 1024,
            interval=100.0,
        )

        callback(1 * 1024 * 1024, 10 * 1024 * 1024)
        await asyncio.sleep(0.05)
        callback(2 * 1024 * 1024, 10 * 1024 * 1024)
        await asyncio.sleep(0.05)
        callback(10 * 1024 * 1024, 10 * 1024 * 1024)
        await asyncio.sleep(0.05)

        assert len(client.edits) == 2
        assert client.edits[0] == (123, 88, "Qism 2/4 - 1.0/10.0 MB (10.0%)")
        assert client.edits[1] == (123, 88, "Qism 2/4 - 10.0/10.0 MB (100.0%)")

    asyncio.run(run())


def test_public_copy_caption_too_long_falls_back_to_captionless_copy_and_text():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import _copy_public_message_with_caption_fallback

        class FakeBot:
            def __init__(self):
                self.copy_calls = []
                self.send_calls = []

            async def copy_message(self, **kwargs):
                self.copy_calls.append(kwargs)
                if len(self.copy_calls) == 1:
                    raise RuntimeError(
                        'Telegram says: [400 MEDIA_CAPTION_TOO_LONG] (caused by "messages.SendMedia")'
                    )
                return SimpleNamespace(id=99, chat=SimpleNamespace(id=kwargs["chat_id"]))

            async def send_message(self, **kwargs):
                self.send_calls.append(kwargs)
                return SimpleNamespace(id=100 + len(self.send_calls), chat=SimpleNamespace(id=kwargs["chat_id"]))

        caption = "Long caption " * 120
        source_msg = SimpleNamespace(
            id=218,
            chat=SimpleNamespace(id="zapislar_efir"),
            caption=caption,
            caption_entities=None,
        )
        source_anchor = SimpleNamespace(id=55, chat=SimpleNamespace(id=123))
        bot = FakeBot()

        copied = await _copy_public_message_with_caption_fallback(
            bot,
            target_chat_id=123,
            source_msg=source_msg,
            reply_target_message=source_anchor,
        )

        assert copied.id == 99
        assert "caption" not in bot.copy_calls[0]
        assert bot.copy_calls[1]["caption"] == ""
        assert bot.copy_calls[1]["reply_to_message_id"] == 55
        assert bot.send_calls[0]["text"] == caption
        assert bot.send_calls[0]["reply_to_message_id"] == 55

    asyncio.run(run())


def test_parse_native_comment_link_preserves_source_post_for_discussion_route():
    import sys

    sys.modules.setdefault(
        "psutil",
        SimpleNamespace(
            virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
        ),
    )

    from TechVJ.save import parse_telegram_url

    parsed, error = parse_telegram_url("https://t.me/zapislar_efir/218?comment=901")

    assert error is None
    assert parsed.url_type == "thread"
    assert parsed.channel_id == "zapislar_efir"
    assert parsed.post_ids == [901]
    assert parsed.thread_id is None
    assert parsed.thread_source_chat_id == "zapislar_efir"
    assert parsed.thread_source_post_id == 218


def test_prepare_discussion_route_joins_linked_group_from_channel_post():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import _prepare_discussion_thread_route

        discussion_chat = SimpleNamespace(id=-100777, username="zapislar_chat")
        discussion_root = SimpleNamespace(id=345, chat=discussion_chat, empty=False)
        source_chat = SimpleNamespace(id=-100111, title="Zapislar")

        class FakeAcc:
            def __init__(self):
                self.joined = []
                self.resolved = []

            async def get_chat(self, chat_id):
                if chat_id == "zapislar_efir":
                    return source_chat
                if chat_id == -100777:
                    return discussion_chat
                raise RuntimeError(f"unexpected chat {chat_id}")

            async def get_discussion_message(self, chat_id, message_id):
                assert chat_id == "zapislar_efir"
                assert message_id == 218
                return discussion_root

            async def join_chat(self, chat_id):
                self.joined.append(chat_id)
                return discussion_chat

            async def resolve_peer(self, peer_id):
                self.resolved.append(peer_id)
                return SimpleNamespace(channel_id=abs(int(peer_id)))

        acc = FakeAcc()

        result = await _prepare_discussion_thread_route(
            acc,
            source_chat_id="zapislar_efir",
            source_post_id=218,
            fallback_discussion_chat_id="zapislar_efir",
            fallback_thread_id=None,
            context=None,
        )

        discussion_chat_id, thread_id, resolved_source_chat, root_msg, join_status = result

        assert discussion_chat_id == -100777
        assert thread_id == 345
        assert resolved_source_chat is source_chat
        assert root_msg is discussion_root
        assert join_status == "joined"
        assert acc.joined == ["zapislar_chat"]
        assert acc.resolved == [-100777]

    asyncio.run(run())


def test_caption_splitter_does_not_partialize_text_link_entity():
    from core.caption_splitter import CAPTION_LIMIT, split_caption
    from core.utf16_utils import char_to_utf16_offset, utf16_len

    prefix = "a" * (CAPTION_LIMIT - 10)
    link_text = "L" * 40
    suffix = " end"
    text = prefix + link_text + suffix
    entity = MessageEntity(
        type=MessageEntityType.TEXT_LINK,
        offset=char_to_utf16_offset(text, len(prefix)),
        length=utf16_len(link_text),
        url="https://example.com/path",
    )

    primary, overflow = split_caption(text, [entity])

    assert primary.text == prefix.rstrip()
    assert primary.entities == []
    assert overflow
    assert overflow[0].entities
    assert overflow[0].entities[0].type == MessageEntityType.TEXT_LINK


def test_copy_utils_resolves_location_and_venue_fingerprints():
    async def run():
        sent_location = SimpleNamespace(
            id=10,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=777),
            from_user=SimpleNamespace(id=333, username=None),
            location=SimpleNamespace(latitude=41.311081, longitude=69.240562),
            caption=None,
        )
        bot_location = SimpleNamespace(
            id=20,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=333),
            from_user=SimpleNamespace(id=333, username=None),
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


def test_bot_upload_update_waiter_resolves_without_history():
    async def run():
        bot_side = SimpleNamespace(
            id=77,
            chat=SimpleNamespace(id=333, username=None),
            from_user=SimpleNamespace(id=333),
            document=SimpleNamespace(file_unique_id="waiter", file_size=456, file_name="w.bin"),
            caption=None,
        )
        sent = SimpleNamespace(
            id=12,
            chat=SimpleNamespace(id=777, username=None),
            from_user=SimpleNamespace(id=333),
            document=SimpleNamespace(file_unique_id="waiter", file_size=456, file_name="w.bin"),
            caption=None,
        )

        class FakeBot:
            def __init__(self):
                self.handler = None
                self.group = None

            def add_handler(self, handler, group=0):
                self.handler = handler
                self.group = group
                return (handler, group)

            def remove_handler(self, handler, group=0):
                assert handler is self.handler
                assert group == self.group

            def get_chat_history(self, chat_id, limit=1):
                raise AssertionError("waiter must not read bot history")

            async def emit(self, message):
                try:
                    await self.handler.original_callback(self, message)
                except Exception as err:
                    if err.__class__.__name__ != "StopPropagation":
                        raise

        bot = FakeBot()
        waiter = await start_bot_upload_update_waiter(bot, 333, timeout=1)
        wait_task = asyncio.create_task(waiter.wait_for(sent))
        await asyncio.sleep(0)
        await bot.emit(bot_side)

        assert await wait_task == 77
        await waiter.close()

    asyncio.run(run())


def test_own_user_session_upload_skips_bot_copy():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import _copy_user_session_upload_to_user

        sent = SimpleNamespace(id=10)

        class FakeBot:
            async def copy_message(self, **kwargs):
                raise AssertionError("own user-session media must not be bot-copied")

            async def delete_messages(self, *args, **kwargs):
                raise AssertionError("own user-session media must not be deleted by bot")

        result = await _copy_user_session_upload_to_user(
            bot_client=FakeBot(),
            sent_message=sent,
            source_user_id=123,
            target_user_id=123,
            request_message=SimpleNamespace(),
            pre_upload_latest_id=None,
        )

        assert result is sent

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
                from_user=SimpleNamespace(id=222, username=None),
            document=SimpleNamespace(file_unique_id="u1", file_size=123, file_name="a.bin"),
            caption=None,
        )
        bot_side = SimpleNamespace(
            id=99,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=222, username=None),
            from_user=SimpleNamespace(id=222, username=None),
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

        bot = None

        class FakeBridge:
            def build_send_fn(self, **kwargs):
                return lambda client: None

            async def get_worker(self, record, user_id):
                return SimpleNamespace(session_user_id=222)

            async def enqueue_task(self, worker, send_fn, is_media=True, owner_user_id=None):
                assert owner_user_id == 123
                await bot.emit(bot_side)
                return sent

        class FakeBot:
            def __init__(self):
                self.me = SimpleNamespace(id=777)
                self.copy_calls = []
                self.delete_calls = []
                self.handler = None
                self.group = None

            def add_handler(self, handler, group=0):
                self.handler = handler
                self.group = group
                return (handler, group)

            def remove_handler(self, handler, group=0):
                assert handler is self.handler
                assert group == self.group

            async def copy_message(self, **kwargs):
                self.copy_calls.append(kwargs)
                return copied

            async def delete_messages(self, chat_id, message_ids):
                self.delete_calls.append((chat_id, message_ids))

            async def emit(self, message):
                await asyncio.sleep(0)
                try:
                    await self.handler.original_callback(self, message)
                except Exception as err:
                    if err.__class__.__name__ != "StopPropagation":
                        raise

            def get_chat_history(self, chat_id, limit=1):
                raise AssertionError("pool copy should resolve from bot update, not history")

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


def test_session_manager_user_owned_upload_does_not_use_bot_history_or_copy():
    async def run():
        sent = SimpleNamespace(
            id=12,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=777),
            from_user=SimpleNamespace(id=123),
            photo=SimpleNamespace(file_unique_id="p1", file_size=123),
            caption=None,
        )

        class FakeBridge:
            def __init__(self):
                self.send_kwargs = None

            def build_send_fn(self, **kwargs):
                self.send_kwargs = dict(kwargs["send_kwargs"])
                return lambda client: None

            async def get_worker(self, record, user_id):
                class FakeWorker:
                    session_user_id = 123

                    async def resolve_bot_reply_message_id(self, message):
                        return None

                return FakeWorker()

            async def enqueue_task(self, worker, send_fn, is_media=True, owner_user_id=None):
                assert owner_user_id == 123
                return sent

        class FakeBot:
            def __init__(self):
                self.me = SimpleNamespace(id=777)
                self.copy_calls = []

            async def copy_message(self, **kwargs):
                self.copy_calls.append(kwargs)
                raise AssertionError("non-pool upload must not copy")

            def get_chat_history(self, chat_id, limit=1):
                raise AssertionError("non-pool upload must not read bot history")

        manager = SessionManager()
        manager._initialized = True
        bridge = FakeBridge()
        manager._bridge = bridge
        record = SessionRecord(
            session_id="test-user-owned-direct",
            session_string="session",
            phone="",
            type=SessionType.USER_OWNED,
            owner_user_id=123,
            max_parallel_tasks=1,
        )
        bot = FakeBot()

        result = await manager.upload_with_session(
            record=record,
            user_id=123,
            target_chat_id=123,
            msg_type="Photo",
            file_path="photo.jpg",
            send_kwargs={"reply_to_message_id": 55},
            bot_client=bot,
            bot_user_id=777,
        )

        assert result is sent
        assert "reply_to_message_id" not in bridge.send_kwargs
        assert bot.copy_calls == []
        assert record.current_tasks == 0

    asyncio.run(run())


def test_session_manager_user_owned_upload_resolves_reply_anchor_before_send():
    async def run():
        sent = SimpleNamespace(
            id=12,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=777),
            from_user=SimpleNamespace(id=123),
            photo=SimpleNamespace(file_unique_id="p1", file_size=123),
            caption=None,
        )
        reply_anchor = SimpleNamespace(id=55, text="Manba: Maqsad Club")

        class FakeWorker:
            session_user_id = 123

            async def resolve_bot_reply_message_id(self, message):
                assert message is reply_anchor
                return 88

        class FakeBridge:
            def __init__(self):
                self.send_kwargs = None

            def build_send_fn(self, **kwargs):
                self.send_kwargs = dict(kwargs["send_kwargs"])
                return lambda client: None

            async def get_worker(self, record, user_id):
                return FakeWorker()

            async def enqueue_task(self, worker, send_fn, is_media=True, owner_user_id=None):
                assert owner_user_id == 123
                return sent

        class FakeBot:
            def __init__(self):
                self.me = SimpleNamespace(id=777)

            async def copy_message(self, **kwargs):
                raise AssertionError("direct user session upload must not bot-copy")

            def get_chat_history(self, chat_id, limit=1):
                raise AssertionError("direct user session upload must not read bot history")

        manager = SessionManager()
        manager._initialized = True
        bridge = FakeBridge()
        manager._bridge = bridge
        record = SessionRecord(
            session_id="test-user-owned-reply-resolve",
            session_string="session",
            phone="",
            type=SessionType.USER_OWNED,
            owner_user_id=123,
            max_parallel_tasks=1,
        )

        result = await manager.upload_with_session(
            record=record,
            user_id=123,
            target_chat_id=123,
            msg_type="Photo",
            file_path="photo.jpg",
            send_kwargs={"reply_to_message_id": 55},
            bot_client=FakeBot(),
            bot_user_id=777,
            reply_message=reply_anchor,
        )

        assert result is sent
        assert bridge.send_kwargs["reply_to_message_id"] == 88
        assert record.current_tasks == 0

    asyncio.run(run())


def test_media_route_guard_rejects_user_client_for_text_route():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import RoutingGuardError, _ensure_media_route_clients

        user_client = SimpleNamespace(me=SimpleNamespace(id=123, is_bot=False))
        user_session = SimpleNamespace(me=SimpleNamespace(id=123, is_bot=False))

        try:
            await _ensure_media_route_clients(
                user_client,
                user_session,
                user_id=123,
                target_chat_id=123,
                msg_type="Photo",
            )
        except RoutingGuardError as err:
            assert "Bot client is not verified" in str(err)
        else:
            raise AssertionError("routing guard must reject non-bot text/status client")

    asyncio.run(run())


def test_media_route_guard_swaps_accidental_bot_user_order():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import _ensure_media_route_clients

        user_session = SimpleNamespace(me=SimpleNamespace(id=123, is_bot=False))
        bot_client = SimpleNamespace(me=SimpleNamespace(id=777, is_bot=True))

        fixed_client, fixed_acc, bot_id, uploader_id = await _ensure_media_route_clients(
            user_session,
            bot_client,
            user_id=123,
            target_chat_id=123,
            msg_type="Video",
        )

        assert fixed_client is bot_client
        assert fixed_acc is user_session
        assert bot_id == 777
        assert uploader_id == 123

    asyncio.run(run())


def test_split_part_upload_retries_peer_invalid_with_bot_username():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import _send_split_part_with_peer_retry

        class FakeAcc:
            def __init__(self):
                self.targets = []
                self.started = []
                self.unblocked = []

            async def send_video(self, target, path, **kwargs):
                self.targets.append(target)
                if len(self.targets) == 1:
                    raise RuntimeError("Telegram says: [400 PEER_ID_INVALID]")
                return SimpleNamespace(id=90, chat=SimpleNamespace(id=target), caption=kwargs.get("caption"))

            async def get_chat(self, username):
                return SimpleNamespace(id=777, username=username)

            async def unblock_user(self, username):
                self.unblocked.append(username)

            async def send_message(self, username, text):
                self.started.append((username, text))

        bot = SimpleNamespace(me=SimpleNamespace(id=777, is_bot=True, username="kursbotactiv"))
        acc = FakeAcc()

        sent = await _send_split_part_with_peer_retry(
            acc=acc,
            target_chat_id=777,
            bot_client=bot,
            msg_type="Video",
            chunk_path="part1.mp4",
            send_kwargs={"caption": "Part 1/2"},
        )

        assert sent.id == 90
        assert acc.targets == [777, "kursbotactiv"]
        assert acc.started == [("kursbotactiv", "/start")]
        assert acc.unblocked == ["kursbotactiv"]

    asyncio.run(run())


def test_split_bot_peer_resolve_is_cached_per_session():
    async def run():
        import sys

        sys.modules.setdefault(
            "psutil",
            SimpleNamespace(
                virtual_memory=lambda: SimpleNamespace(total=8 * 1024**3),
            ),
        )

        from TechVJ.save import _ensure_user_session_bot_peer

        class FakeAcc:
            def __init__(self):
                self.get_chat_calls = 0
                self.started = []

            async def get_chat(self, username):
                self.get_chat_calls += 1
                return SimpleNamespace(id=777, username=username)

            async def unblock_user(self, username):
                return None

            async def send_message(self, username, text):
                self.started.append((username, text))

            async def resolve_peer(self, peer_id):
                return SimpleNamespace(user_id=peer_id)

        bot = SimpleNamespace(me=SimpleNamespace(id=777, is_bot=True, username="kursbotactiv"))
        acc = FakeAcc()

        first = await _ensure_user_session_bot_peer(acc, bot, 777)
        second = await _ensure_user_session_bot_peer(acc, bot, 777)

        assert first == (777, "kursbotactiv")
        assert second == (777, "kursbotactiv")
        assert acc.started == [("kursbotactiv", "/start")]
        assert acc.get_chat_calls == 2

    asyncio.run(run())


def test_album_send_retries_peer_invalid_with_bot_username():
    async def run():
        from TechVJ.album_collector_v2 import AlbumPhoto, CollectedAlbum, send_album

        class FakeUserSession:
            def __init__(self):
                self.targets = []
                self.started = []
                self.unblocked = []

            async def send_media_group(self, target, media):
                self.targets.append(target)
                if len(self.targets) == 1:
                    raise RuntimeError("Telegram says: [400 PEER_ID_INVALID]")
                return [SimpleNamespace(id=91)]

            async def get_chat(self, username):
                return SimpleNamespace(id=777, username=username)

            async def unblock_user(self, username):
                self.unblocked.append(username)

            async def send_message(self, username, text):
                self.started.append((username, text))

        async def not_cancelled():
            return False

        with TemporaryDirectory() as temp_dir:
            photo_paths = []
            for index in range(2):
                photo_path = Path(temp_dir) / f"photo_{index}.jpg"
                photo_path.write_bytes(b"test-photo")
                photo_paths.append(str(photo_path))

            album = CollectedAlbum(
                media_group_id="album-1",
                first_message_id=10,
                last_message_id=11,
                photos=[
                    AlbumPhoto(message_id=10, file_path=photo_paths[0], order_index=0),
                    AlbumPhoto(message_id=11, file_path=photo_paths[1], order_index=1),
                ],
                temp_dir=temp_dir,
            )
            bot = SimpleNamespace(
                me=SimpleNamespace(id=777, is_bot=True, username="kursbotactiv")
            )
            user_session = FakeUserSession()

            sent = await send_album(
                bot,
                user_session,
                album,
                target_chat_id=123,
                reply_to_message_id=1,
                check_cancelled=not_cancelled,
            )

            assert sent is True
            assert user_session.targets == ["kursbotactiv", "kursbotactiv"]
            assert user_session.started == [("kursbotactiv", "/start")]
            assert user_session.unblocked == ["kursbotactiv"]

    asyncio.run(run())


def test_session_manager_pool_copy_failure_returns_none_without_delete():
    async def run():
        sent = SimpleNamespace(
            id=11,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=777),
                from_user=SimpleNamespace(id=333, username=None),
            document=SimpleNamespace(file_unique_id="u2", file_size=456, file_name="b.bin"),
            caption=None,
        )
        bot_side = SimpleNamespace(
            id=88,
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            chat=SimpleNamespace(id=333, username=None),
            from_user=SimpleNamespace(id=333, username=None),
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

        bot = None

        class FakeBridge:
            def build_send_fn(self, **kwargs):
                return lambda client: None

            async def get_worker(self, record, user_id):
                return SimpleNamespace(session_user_id=333)

            async def enqueue_task(self, worker, send_fn, is_media=True, owner_user_id=None):
                assert owner_user_id == 123
                await bot.emit(bot_side)
                return sent

        class FakeBot:
            def __init__(self):
                self.me = SimpleNamespace(id=777)
                self.copy_calls = []
                self.delete_calls = []
                self.handler = None
                self.group = None

            def add_handler(self, handler, group=0):
                self.handler = handler
                self.group = group
                return (handler, group)

            def remove_handler(self, handler, group=0):
                assert handler is self.handler
                assert group == self.group

            async def copy_message(self, **kwargs):
                self.copy_calls.append(kwargs)
                raise RuntimeError("copy failed")

            async def delete_messages(self, chat_id, message_ids):
                self.delete_calls.append((chat_id, message_ids))

            async def emit(self, message):
                await asyncio.sleep(0)
                try:
                    await self.handler.original_callback(self, message)
                except Exception as err:
                    if err.__class__.__name__ != "StopPropagation":
                        raise

            def get_chat_history(self, chat_id, limit=1):
                raise AssertionError("pool copy should resolve from bot update, not history")

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

            def add_handler(self, handler, group=0):
                raise RuntimeError("listener unavailable")

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


def test_pyrofork_upload_part_retries_same_part_after_premium_flood():
    import core.pyrofork_compat as compat

    class FloodPremiumWait(Exception):
        def __init__(self, seconds):
            self.value = seconds
            super().__init__(
                f'Telegram says: [420 FLOOD_PREMIUM_WAIT_{seconds}] '
                '(caused by "upload.SaveBigFilePart")'
            )

    class FakeQuery:
        QUALNAME = "upload.SaveBigFilePart"

    class FakeSession:
        def __init__(self):
            self.calls = 0
            self.thresholds = []

        async def invoke(self, query, sleep_threshold=None):
            assert isinstance(query, FakeQuery)
            self.calls += 1
            self.thresholds.append(sleep_threshold)
            if self.calls == 1:
                raise FloodPremiumWait(11)
            return True

    async def run():
        session = FakeSession()
        sleeps = []
        floods = []
        original_sleep = compat.asyncio.sleep

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        compat.asyncio.sleep = fake_sleep
        try:
            result = await compat._invoke_upload_part_with_retry(
                session,
                FakeQuery(),
                client_name="user_worker_8883410579",
                on_flood=floods.append,
            )
        finally:
            compat.asyncio.sleep = original_sleep

        assert result is True
        assert session.calls == 2
        assert session.thresholds == [0, 0]
        assert sleeps == [12.0]
        assert floods == [11]

    asyncio.run(run())


def test_pyrofork_upload_part_surfaces_long_premium_flood_without_sleeping():
    import core.pyrofork_compat as compat

    class FloodPremiumWait(Exception):
        def __init__(self, seconds):
            self.value = seconds
            super().__init__(
                f'Telegram says: [420 FLOOD_PREMIUM_WAIT_{seconds}] '
                '(caused by "upload.SaveBigFilePart")'
            )

    class FakeQuery:
        QUALNAME = "upload.SaveBigFilePart"

    class FakeSession:
        def __init__(self):
            self.calls = 0

        async def invoke(self, query, sleep_threshold=None):
            self.calls += 1
            raise FloodPremiumWait(60)

    async def run():
        session = FakeSession()
        sleeps = []
        original_sleep = compat.asyncio.sleep

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        compat.asyncio.sleep = fake_sleep
        try:
            try:
                await compat._invoke_upload_part_with_retry(
                    session,
                    FakeQuery(),
                    client_name="user_worker_8883410579",
                )
                assert False, "long FloodPremiumWait must be surfaced"
            except FloodPremiumWait as exc:
                assert exc.value == 60
        finally:
            compat.asyncio.sleep = original_sleep

        assert session.calls == 1
        assert sleeps == []

    asyncio.run(run())


def test_pyrofork_flood_safe_save_file_retries_without_losing_part():
    import core.pyrofork_compat as compat

    class FloodPremiumWait(Exception):
        def __init__(self, seconds):
            self.value = seconds
            super().__init__(
                f'Telegram says: [420 FLOOD_PREMIUM_WAIT_{seconds}] '
                '(caused by "upload.SaveFilePart")'
            )

    class FakeStorage:
        async def dc_id(self):
            return 2

        async def auth_key(self):
            return b"auth-key"

        async def test_mode(self):
            return False

    class FakeSession:
        instances = []

        def __init__(self, *args, **kwargs):
            self.calls = []
            self.stopped = False
            self.__class__.instances.append(self)

        async def start(self):
            return None

        async def invoke(self, query, sleep_threshold=None):
            self.calls.append((query.file_part, sleep_threshold))
            if len(self.calls) == 1:
                raise FloodPremiumWait(11)
            return True

        async def stop(self):
            self.stopped = True

    class FakeClient:
        def __init__(self):
            self.save_file_semaphore = asyncio.Semaphore(1)
            self.me = SimpleNamespace(is_premium=False)
            self.storage = FakeStorage()
            self.loop = asyncio.get_running_loop()
            self.executor = None
            self.name = "user_worker_8883410579"
            self._techvj_upload_part_workers = 2
            self._techvj_upload_part_flood_retries = 5
            self._techvj_upload_part_max_total_wait = 180
            self._techvj_upload_part_long_wait = 60
            self.floods = []
            self._techvj_upload_flood_callback = self.floods.append

        @staticmethod
        def rnd_id():
            return 123456789

    async def run():
        with TemporaryDirectory() as tmp_dir:
            file_path = Path(tmp_dir) / "upload.bin"
            file_path.write_bytes(b"x" * (1024 * 1024 + 17))
            client = FakeClient()
            sleeps = []
            original_session = compat.PyrogramSession
            original_sleep = compat.asyncio.sleep

            async def fake_sleep(seconds):
                sleeps.append(seconds)

            compat.PyrogramSession = FakeSession
            compat.asyncio.sleep = fake_sleep
            try:
                result = await compat._flood_safe_save_file(client, str(file_path))
            finally:
                compat.PyrogramSession = original_session
                compat.asyncio.sleep = original_sleep

            session = FakeSession.instances[-1]
            assert result.parts == 3
            assert [part for part, _ in session.calls] == [0, 0, 1, 2]
            assert all(threshold == 0 for _, threshold in session.calls)
            assert sleeps == [12.0]
            assert client.floods == [11]
            assert session.stopped is True

    asyncio.run(run())


def test_user_upload_worker_enables_flood_safe_part_upload_defaults():
    from core import user_upload_worker as worker_module
    from pyrogram import Client
    from pyrogram.methods.advanced.save_file import SaveFile

    assert worker_module.UPLOAD_PART_WORKERS == 2
    assert worker_module.UPLOAD_PART_FLOOD_RETRIES == 5
    assert worker_module.UPLOAD_PART_MAX_TOTAL_WAIT == 180
    assert getattr(SaveFile.save_file, "_techvj_flood_safe", False) is True
    assert getattr(Client.save_file, "_techvj_flood_safe", False) is True


def test_user_upload_worker_reaper_skips_active_upload():
    from core.user_upload_worker import IDLE_TIMEOUT, UserWorkerRegistry

    worker = SimpleNamespace(
        _last_activity=0.0,
        _busy=True,
        _queue=SimpleNamespace(empty=lambda: True),
    )
    now = float(IDLE_TIMEOUT + 1)

    assert UserWorkerRegistry._should_reap(worker, now) is False

    worker._busy = False
    assert UserWorkerRegistry._should_reap(worker, now) is True
