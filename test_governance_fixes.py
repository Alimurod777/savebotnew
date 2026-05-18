import asyncio
from types import SimpleNamespace

from core.copy_utils import get_bot_copy_source_chat_id
from core.permission_guard import permission_guard
from core.priority_queue import PriorityJob, PriorityQueue
from core.rate_limiter import RateLimiter
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
        sent = SimpleNamespace(id=10, chat=SimpleNamespace(id=777), from_user=SimpleNamespace(id=222))

        class FakeBridge:
            def build_send_fn(self, **kwargs):
                return lambda client: None

            async def get_worker(self, record, user_id):
                return object()

            async def enqueue_task(self, worker, send_fn, is_media=True):
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
        assert bot.delete_calls == [(222, [10])]
        assert record.current_tasks == 0

    asyncio.run(run())


def test_session_manager_pool_copy_failure_returns_none_without_delete():
    async def run():
        sent = SimpleNamespace(id=11, chat=SimpleNamespace(id=777), from_user=SimpleNamespace(id=333))

        class FakeBridge:
            def build_send_fn(self, **kwargs):
                return lambda client: None

            async def get_worker(self, record, user_id):
                return object()

            async def enqueue_task(self, worker, send_fn, is_media=True):
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
        assert bot.delete_calls == []
        assert record.current_tasks == 0

    asyncio.run(run())
