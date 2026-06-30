"""
core/topic_extractor.py — Extract all messages from a Telegram forum topic.

Guarantees:
  - Messages yielded in ascending chronological order (oldest first)
  - No duplicates
  - Only messages belonging to the target topic
  - FloodWait handled with exponential backoff

Integration: used by TechVJ/save.py process_topic_posts() when
post_ids list is absent or the full topic is requested.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Tuple

from pyrogram import raw, utils
from pyrogram.types import Message

from core.retry_utils import get_floodwait_seconds, is_floodwait_error
from core.topic_cache import TopicCacheEntry, topic_cache

logger = logging.getLogger(__name__)


@dataclass
class TopicExtractorConfig:
    chat_id: int
    topic_id: int                    # ID of the message that created the topic
    fetch_batch_size: int = 200      # MTProto max is 200
    inter_page_delay: float = 0.3    # seconds between pages
    flood_backoff_cap: float = 120.0 # max extra sleep added on top of FloodWait
    max_scan_pages: int = 1000       # fallback cap when server-side topic filter is unavailable
    stop_after_ids: Optional[Set[int]] = None  # optional anchors that allow early stop
    allow_history_scan_fallback: bool = False  # full chat scan is too flood-prone for production
    raw_peer: Optional[Any] = None    # pre-resolved InputPeer, avoids stale/access-hash re-resolve
    use_cache: bool = True            # persist topic message IDs between requests
    incremental_from_cache: bool = False  # full-topic mode can return only IDs newer than cache


class TopicExtractionError(Exception):
    pass


class TopicExtractor:
    """
    Extracts all messages from a single Telegram forum topic
    and streams them in chronological order.

    Usage:
        extractor = TopicExtractor(acc, TopicExtractorConfig(
            chat_id=-1001234567890,
            topic_id=456,
        ))
        async for batch in extractor.stream(batch_size=50):
            for msg in batch:
                await process(msg)
    """

    def __init__(self, client, config: TopicExtractorConfig) -> None:
        # client is a Pyrogram/Pyrofork Client (user session, not bot)
        self._client = client
        self._cfg = config
        self._thread_filter_supported: Optional[bool] = None
        self._raw_search_supported: Optional[bool] = None
        self._raw_replies_supported: Optional[bool] = None
        self._used_raw_search = False
        self._used_raw_replies = False
        self._last_page_mode: Optional[str] = None
        self._seen_stop_ids: Set[int] = set()
        self._raw_peer = config.raw_peer
        self._cache_entry: Optional[TopicCacheEntry] = None
        if config.use_cache:
            self._cache_entry = topic_cache.get(config.chat_id, config.topic_id)
        self._hit_cache_boundary = False

    @property
    def used_history(self) -> bool:
        return self._last_page_mode == "history" or self._thread_filter_supported is True

    @property
    def used_raw_search(self) -> bool:
        return self._used_raw_search

    @property
    def used_raw_replies(self) -> bool:
        return self._used_raw_replies

    @property
    def used_cache(self) -> bool:
        return bool(self._cache_entry and self._cache_entry.known_message_ids)

    # ── Low-level fetch ────────────────────────────────────────────────────────

    async def _fetch_page(self, offset_id: int = 0, search_offset: int = 0) -> List[Message]:
        """
        Fetch one page of topic messages (newest-first, MTProto default).
        offset_id=0  → fetch from the very newest message downward.
        offset_id=N  → fetch messages older than message ID N.
        Returns [] when no more messages exist.
        Retries forever on FloodWait with exponential backoff.
        """
        attempt = 0
        while True:
            try:
                msgs = await self._fetch_thread_page(offset_id, search_offset)
                logger.debug(
                    "TopicExtractor: page offset_id=%d returned %d msgs",
                    offset_id, len(msgs),
                )
                return msgs

            except Exception as e:
                if not is_floodwait_error(e):
                    logger.error("TopicExtractor: fatal fetch error: %s", e)
                    raise TopicExtractionError(f"fetch failed: {e}") from e
                wait = get_floodwait_seconds(e)
                attempt += 1
                extra = min(attempt * 5, self._cfg.flood_backoff_cap)
                total_wait = wait + extra
                logger.warning(
                    "TopicExtractor: FloodWait %ds (attempt %d) — sleeping %.0fs",
                    wait, attempt, total_wait,
                )
                await asyncio.sleep(total_wait)

    async def _fetch_thread_page(self, offset_id: int, search_offset: int) -> List[Message]:
        if self._raw_search_supported is not False:
            try:
                return await self._fetch_search_page(search_offset)
            except TopicExtractionError:
                raise
            except Exception as e:
                self._raw_search_supported = False
                logger.warning(
                    "TopicExtractor: raw Search(top_msg_id) unavailable for topic=%d: %s",
                    self._cfg.topic_id,
                    e,
                )

        if self._raw_replies_supported is not False:
            try:
                return await self._fetch_replies_page(offset_id)
            except TopicExtractionError:
                raise
            except Exception as e:
                self._raw_replies_supported = False
                logger.warning(
                    "TopicExtractor: raw GetReplies unavailable for topic=%d: %s",
                    self._cfg.topic_id,
                    e,
                )
                if not self._cfg.allow_history_scan_fallback:
                    raise TopicExtractionError(
                        "server-side topic fetch unavailable and chat-history scan disabled"
                    ) from e

        if self._thread_filter_supported is not False and self._cfg.allow_history_scan_fallback:
            kwargs = {
                "limit": self._page_limit(),
                "offset_id": offset_id,
            }
            try:
                msgs: List[Message] = []
                async for msg in self._client.get_chat_history(
                    self._cfg.chat_id,
                    message_thread_id=self._cfg.topic_id,
                    **kwargs,
                ):
                    msgs.append(msg)
                self._thread_filter_supported = True
                self._last_page_mode = "history"
                return msgs
            except TypeError as e:
                if "message_thread_id" not in str(e):
                    raise
                self._thread_filter_supported = False
                logger.info(
                    "TopicExtractor: get_chat_history has no message_thread_id support; "
                    "falling back to raw full-history scan only if enabled"
                )

        if not self._cfg.allow_history_scan_fallback:
            raise TopicExtractionError(
                "server-side topic fetch unavailable and chat-history scan disabled"
            )

        logger.warning(
            "TopicExtractor: using flood-prone chat-history scan fallback for topic=%d",
            self._cfg.topic_id,
        )
        msgs = []
        async for msg in self._client.get_chat_history(
            self._cfg.chat_id,
            limit=self._page_limit(),
            offset_id=offset_id,
        ):
            msgs.append(msg)
        self._last_page_mode = "history"
        return msgs

    def _page_limit(self) -> int:
        return max(1, min(self._cfg.fetch_batch_size, 100))

    def _expected_page_limit(self) -> int:
        return self._page_limit()

    async def _resolve_raw_peer(self):
        if self._raw_peer is None:
            self._raw_peer = await self._client.resolve_peer(self._cfg.chat_id)
        return self._raw_peer

    def _raw_input_channel(self):
        peer = self._raw_peer
        if peer is None:
            return None
        channel_id = getattr(peer, "channel_id", None)
        access_hash = getattr(peer, "access_hash", None)
        if channel_id is None or access_hash is None:
            return None
        return raw.types.InputChannel(channel_id=channel_id, access_hash=access_hash)

    async def _parse_raw_messages(self, result) -> List[Message]:
        self._normalize_raw_messages(result)
        return list(await utils.parse_messages(self._client, result, replies=0))

    async def _fetch_messages_by_ids(self, message_ids: List[int]) -> List[Message]:
        """Fetch specific topic messages in batches, preferring raw GetMessages.

        This is used for cache hydration. It avoids one get_messages request per
        ID and keeps explicit-ID fetches bounded to 100 IDs per request.
        """
        ids = []
        seen = set()
        for msg_id in message_ids:
            if msg_id in seen:
                continue
            seen.add(msg_id)
            ids.append(int(msg_id))

        if not ids:
            return []

        result: List[Message] = []
        try:
            await self._resolve_raw_peer()
            input_channel = self._raw_input_channel()
            if input_channel is not None:
                for i in range(0, len(ids), 100):
                    chunk = ids[i:i + 100]
                    raw_result = await self._client.invoke(
                        raw.functions.channels.GetMessages(
                            channel=input_channel,
                            id=[raw.types.InputMessageID(id=msg_id) for msg_id in chunk],
                        ),
                        sleep_threshold=0,
                    )
                    result.extend(await self._parse_raw_messages(raw_result))
                return result
        except Exception as e:
            if is_floodwait_error(e):
                raise
            logger.debug(
                "TopicExtractor: raw GetMessages hydration fallback for topic=%d: %s",
                self._cfg.topic_id,
                e,
            )

        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            result.extend(await self._fetch_messages_by_ids_high_level(chunk))
        return result

    async def _fetch_messages_by_ids_high_level(self, message_ids: List[int]) -> List[Message]:
        """Fetch specific messages through Pyrogram get_messages in batches."""
        result: List[Message] = []
        for i in range(0, len(message_ids), 100):
            chunk = message_ids[i:i + 100]
            fetched = await self._client.get_messages(self._cfg.chat_id, chunk)
            if not isinstance(fetched, list):
                fetched = [fetched]
            result.extend(
                msg for msg in fetched
                if msg and not getattr(msg, "empty", False)
            )
        return result

    async def _hydrate_cached_ids(self, message_ids: List[int]) -> List[Message]:
        if not message_ids:
            return []
        order = {int(msg_id): idx for idx, msg_id in enumerate(message_ids)}
        messages = [
            msg for msg in await self._fetch_messages_by_ids(message_ids)
            if self._is_topic_message(msg)
        ]
        messages.sort(key=lambda m: order.get(m.id, len(order)))
        return messages

    async def _fetch_discussion_metadata(self) -> List[Message]:
        """Best-effort discussion/thread metadata lookup.

        ``messages.GetDiscussionMessage`` is mainly useful for linked
        discussion threads, but it is cheap and thread-aware. We keep it as a
        metadata/root discovery fallback, never as a history scan.
        """
        try:
            result = await self._client.invoke(
                raw.functions.messages.GetDiscussionMessage(
                    peer=await self._resolve_raw_peer(),
                    msg_id=self._cfg.topic_id,
                ),
                sleep_threshold=0,
            )
            parsed = await self._parse_raw_messages(result)
            return [msg for msg in parsed if self._is_topic_message(msg)]
        except Exception as e:
            if is_floodwait_error(e):
                raise
            logger.debug(
                "TopicExtractor: GetDiscussionMessage unavailable for topic=%d: %s",
                self._cfg.topic_id,
                e,
            )
            return []

    async def _fetch_search_page(self, add_offset: int) -> List[Message]:
        """Fetch forum topic messages using raw messages.Search(top_msg_id).

        For forum topics, ``messages.GetReplies`` can return only the topic
        starter/direct replies on some chats. ``messages.Search`` with
        ``top_msg_id`` is the server-side filter Telegram clients use for
        searching inside a topic/thread, so it avoids a full chat scan.
        """
        result = await self._client.invoke(
            raw.functions.messages.Search(
                peer=await self._resolve_raw_peer(),
                q="",
                filter=raw.types.InputMessagesFilterEmpty(),
                min_date=0,
                max_date=0,
                offset_id=0,
                add_offset=add_offset,
                limit=self._page_limit(),
                max_id=0,
                min_id=0,
                hash=0,
                top_msg_id=self._cfg.topic_id,
            ),
            sleep_threshold=0,
        )

        parsed = await self._parse_raw_messages(result)
        self._raw_search_supported = True
        self._used_raw_search = True
        self._last_page_mode = "search"
        return parsed

    async def _fetch_replies_page(self, offset_id: int) -> List[Message]:
        """Fetch a forum topic page using raw messages.GetReplies.

        This is the important production path for Pyrofork builds where
        get_chat_history(message_thread_id=...) is not implemented. It asks
        Telegram for replies to the topic root, avoiding full chat history scan.
        """
        result = await self._client.invoke(
            raw.functions.messages.GetReplies(
                peer=await self._resolve_raw_peer(),
                msg_id=self._cfg.topic_id,
                offset_id=offset_id,
                offset_date=0,
                add_offset=0,
                limit=self._page_limit(),
                max_id=0,
                min_id=0,
                hash=0,
            ),
            sleep_threshold=0,
        )

        parsed = await self._parse_raw_messages(result)
        self._raw_replies_supported = True
        self._used_raw_replies = True
        self._last_page_mode = "replies"
        return parsed

    @staticmethod
    def _normalize_raw_messages(result) -> None:
        """Patch optional raw fields that Pyrofork parsers expect as lists."""
        for msg in getattr(result, "messages", []) or []:
            if isinstance(msg, raw.types.Message) and getattr(msg, "entities", None) is None:
                msg.entities = []
        for user in getattr(result, "users", []) or []:
            if isinstance(user, raw.types.User):
                if getattr(user, "usernames", None) is None:
                    user.usernames = []
                if getattr(user, "restriction_reason", None) is None:
                    user.restriction_reason = []
        for chat in getattr(result, "chats", []) or []:
            if isinstance(chat, raw.types.Channel):
                if getattr(chat, "usernames", None) is None:
                    chat.usernames = []
                if getattr(chat, "restriction_reason", None) is None:
                    chat.restriction_reason = []

    # ── Topic membership filter ────────────────────────────────────────────────

    def _is_topic_message(self, msg: Message) -> bool:
        """
        Verify the message belongs to our topic.

        Telegram forum mechanics:
          - Topic root message: msg.id == topic_id
          - All replies in topic: reply_to_top_message_id == topic_id
            (Telegram sets top_id to thread root, not immediate parent)
          - First-level direct reply: reply_to_message_id == topic_id
            with no reply_to_top_message_id set

        Pyrofork uses reply_to_top_message_id (MTProto field name).
        """
        if not msg or getattr(msg, "empty", False):
            return False

        if msg.id == self._cfg.topic_id:
            return True

        # Check message_thread_id (modern Pyrogram populates this for forums).
        # Raw parse paths can expose a non-topic sentinel here while still
        # carrying the real topic root in reply_to_top_message_id, so a
        # mismatch is not enough to reject the message.
        thread_id = getattr(msg, "message_thread_id", None)
        if thread_id == self._cfg.topic_id:
            return True

        # Primary: Pyrofork/Pyrogram attribute name
        top_id: Optional[int] = getattr(msg, "reply_to_top_message_id", None)

        # Fallback: nested reply_to object (some Pyrofork builds)
        reply_to = getattr(msg, "reply_to", None)
        if top_id is None:
            if reply_to is not None:
                top_id = getattr(reply_to, "reply_to_top_message_id", None)
                if top_id is None:
                    top_id = getattr(reply_to, "reply_to_top_id", None)

        if top_id is not None:
            return top_id == self._cfg.topic_id

        # Last resort: first-level direct reply to topic root
        reply_id: Optional[int] = getattr(msg, "reply_to_message_id", None)
        if reply_id is None and reply_to is not None:
            reply_id = getattr(reply_to, "reply_to_msg_id", None)
        if reply_id == self._cfg.topic_id:
            return True

        return False

    # ── Full collection ────────────────────────────────────────────────────────

    async def _collect_all(self) -> List[Message]:
        """
        Paginate through the entire topic, deduplicate, and return
        all messages sorted chronologically (oldest first).

        Pagination:
          Page 1: newest 200 msgs  (offset_id=0)
          Page N: 200 msgs older than page N-1's oldest (offset_id=page[-1].id)
          Stop when len(page) < fetch_batch_size (last page reached)

        Order reconstruction:
          pages are collected newest-first.
          reversed(pages) + reversed(each page) = oldest-first, O(n).
          A final sort(date, id) is a safety net for scheduled messages
          or admin-edited timestamps.
        """
        seen: Set[int] = set()
        pages: List[List[Message]] = []
        offset_id = 0
        search_offset = 0
        self._seen_stop_ids = set()
        cached_prefix: List[Message] = []
        use_cache_boundary = False

        if self._cache_entry and self._cache_entry.known_message_ids:
            cached_ids = self._cache_entry.known_message_ids
            if self._cfg.stop_after_ids and self._cfg.stop_after_ids.issubset(set(cached_ids)):
                cached = await self._hydrate_cached_ids(cached_ids)
                positions = {m.id: idx for idx, m in enumerate(cached)}
                if self._cfg.stop_after_ids.issubset(set(positions.keys())):
                    logger.info(
                        "TopicExtractor: served topic=%d anchors from cache (%d ids)",
                        self._cfg.topic_id,
                        len(cached_ids),
                    )
                    return cached

            if self._cache_entry.fully_scanned and self._cache_entry.last_processed_message_id:
                use_cache_boundary = True
                if not self._cfg.incremental_from_cache:
                    cached_prefix = await self._hydrate_cached_ids(cached_ids)
                for msg in cached_prefix:
                    seen.add(msg.id)

        scanned_pages = 0
        while True:
            page = await self._fetch_page(offset_id=offset_id, search_offset=search_offset)
            if (
                not page
                and scanned_pages == 0
                and self._used_raw_search
                and self._raw_replies_supported is not False
            ):
                logger.info(
                    "TopicExtractor: raw Search returned empty first page for topic=%d; trying GetReplies",
                    self._cfg.topic_id,
                )
                self._raw_search_supported = False
                page = await self._fetch_page(offset_id=offset_id, search_offset=search_offset)
            scanned_pages += 1

            if not page:
                break

            clean: List[Message] = []
            for msg in page:
                if (
                    use_cache_boundary
                    and self._cache_entry
                    and self._cache_entry.last_processed_message_id
                    and msg.id == self._cache_entry.last_processed_message_id
                ):
                    self._hit_cache_boundary = True
                    break
                if msg.id in seen:
                    continue
                if not self._is_topic_message(msg):
                    logger.debug(
                        "TopicExtractor: filtered non-topic msg id=%d", msg.id
                    )
                    continue
                seen.add(msg.id)
                clean.append(msg)
                if (
                    self._cfg.stop_after_ids
                    and msg.id in self._cfg.stop_after_ids
                ):
                    self._seen_stop_ids.add(msg.id)

            if clean:
                pages.append(clean)

            if self._hit_cache_boundary:
                logger.info(
                    "TopicExtractor: reached cached boundary topic=%d after %d page(s)",
                    self._cfg.topic_id,
                    scanned_pages,
                )
                break

            if (
                self._cfg.stop_after_ids
                and self._cfg.stop_after_ids.issubset(self._seen_stop_ids)
            ):
                logger.info(
                    "TopicExtractor: all range anchors found for topic=%d after %d page(s)",
                    self._cfg.topic_id,
                    scanned_pages,
                )
                break

            if len(page) < self._expected_page_limit():
                break  # Last page

            if (
                self._thread_filter_supported is False
                and scanned_pages >= self._cfg.max_scan_pages
            ):
                logger.warning(
                    "TopicExtractor: stopped fallback scan at %d pages for topic=%d",
                    scanned_pages,
                    self._cfg.topic_id,
                )
                break

            if self._last_page_mode == "search":
                search_offset += len(page)
            else:
                offset_id = page[-1].id
            await asyncio.sleep(self._cfg.inter_page_delay)

        # Reconstruct chronological order without a full re-sort
        result: List[Message] = list(cached_prefix)
        for page in reversed(pages):
            result.extend(reversed(page))

        skip_root_lookup = self._cfg.incremental_from_cache and use_cache_boundary
        if not skip_root_lookup and self._cfg.topic_id not in seen:
            for msg in await self._fetch_discussion_metadata():
                if msg.id not in seen:
                    seen.add(msg.id)
                    result.append(msg)

        if not skip_root_lookup and self._cfg.topic_id not in seen:
            root = await self._fetch_topic_root()
            if root is not None:
                seen.add(root.id)
                result.append(root)

        # Safety sort: (timestamp, id) handles any edge-case ordering anomalies
        result.sort(key=lambda m: (
            m.date.timestamp() if m.date else 0,
            m.id,
        ))

        logger.info(
            "TopicExtractor: topic=%d collected %d messages",
            self._cfg.topic_id, len(result),
        )
        self._store_cache(result, fully_scanned=not self._cfg.stop_after_ids)
        return result

    def _store_cache(self, messages: List[Message], fully_scanned: bool) -> None:
        if not self._cfg.use_cache or not messages:
            return
        ids = [m.id for m in messages if self._is_topic_message(m)]
        if not ids:
            return
        try:
            existing = self._cache_entry.known_message_ids if self._cache_entry else []
            merged: List[int] = []
            seen = set()
            for msg_id in list(existing) + ids:
                if msg_id in seen:
                    continue
                seen.add(msg_id)
                merged.append(msg_id)
            entry = TopicCacheEntry(
                chat_id=self._cfg.chat_id,
                topic_id=self._cfg.topic_id,
                root_message_id=self._cfg.topic_id,
                known_message_ids=merged,
                last_processed_message_id=merged[-1] if merged else None,
                fully_scanned=fully_scanned,
            )
            topic_cache.put(entry)
            self._cache_entry = entry
        except Exception as e:
            logger.debug(
                "TopicExtractor: cache write skipped topic=%d: %s",
                self._cfg.topic_id,
                e,
            )

    async def _fetch_topic_root(self) -> Optional[Message]:
        """Best-effort include the topic starter without affecting pagination."""
        try:
            roots = await self._fetch_messages_by_ids([self._cfg.topic_id])
            for root in roots:
                if root and not getattr(root, "empty", False) and self._is_topic_message(root):
                    return root
        except Exception as e:
            logger.debug(
                "TopicExtractor: topic root fetch skipped topic=%d: %s",
                self._cfg.topic_id,
                e,
            )
        return None

    # ── Public API ─────────────────────────────────────────────────────────────

    async def stream(self, batch_size: int = 50) -> AsyncGenerator[List[Message], None]:
        """
        Yield messages to the upload pipeline in batches, oldest first.

        Example:
            async for batch in extractor.stream(batch_size=50):
                for msg in batch:
                    await process_single_topic_message(client, acc, user_id, msg, ...)
        """
        all_msgs = await self._collect_all()
        for i in range(0, len(all_msgs), batch_size):
            yield all_msgs[i: i + batch_size]

    async def extract_all(self) -> List[Message]:
        """
        Return all topic messages as a flat list, sorted oldest-first.
        Use stream() for very large topics to avoid keeping everything in RAM.
        """
        return await self._collect_all()

    async def extract_between(self, start_id: int, end_id: int) -> List[Message]:
        """
        Return the chronological topic slice between two anchor message IDs.

        Topic message IDs can be non-monotonic relative to topic order, so this
        intentionally uses the collected oldest-first topic order instead of a
        numeric ID range.

        Fallback: if server-side topic APIs (Search, GetReplies) return
        incomplete results and anchors are missing, fetch only the missing
        anchor IDs first.  A bounded numeric range probe is kept as a final
        small-span fallback.
        """
        all_msgs = await self._collect_all()
        positions = {m.id: idx for idx, m in enumerate(all_msgs)}
        if (
            self._used_raw_search
            and not self._used_raw_replies
            and (start_id not in positions or end_id not in positions)
            and self._raw_replies_supported is not False
        ):
            logger.warning(
                "TopicExtractor: raw Search(top_msg_id) missed range anchors for topic=%d; retrying GetReplies",
                self._cfg.topic_id,
            )
            self._raw_search_supported = False
            self._seen_stop_ids = set()
            all_msgs = await self._collect_all()
            positions = {m.id: idx for idx, m in enumerate(all_msgs)}

        if start_id not in positions or end_id not in positions:
            missing_anchor_ids = [
                mid for mid in (start_id, end_id) if mid not in positions
            ]
            all_msgs = await self._hydrate_missing_anchor_ids(
                missing_anchor_ids,
                all_msgs,
            )
            positions = {m.id: idx for idx, m in enumerate(all_msgs)}

        # ── Direct ID range probe fallback ──────────────────────────────────
        if start_id not in positions or end_id not in positions:
            all_msgs = await self._direct_id_range_fallback(
                start_id, end_id, all_msgs,
            )
            positions = {m.id: idx for idx, m in enumerate(all_msgs)}

        if start_id not in positions or end_id not in positions:
            missing = [str(mid) for mid in (start_id, end_id) if mid not in positions]
            lo, hi = min(start_id, end_id), max(start_id, end_id)
            bounded = [m for m in all_msgs if lo <= m.id <= hi]
            logger.warning(
                "TopicExtractor: anchors not found (%s) for topic=%d; "
                "returning %d collected topic messages within numeric bounds [%d..%d]",
                ", ".join(missing),
                self._cfg.topic_id,
                len(bounded),
                lo,
                hi,
            )
            return bounded

        start_pos = positions[start_id]
        end_pos = positions[end_id]
        if start_pos <= end_pos:
            return all_msgs[start_pos:end_pos + 1]
        return all_msgs[end_pos:start_pos + 1]

    async def _hydrate_missing_anchor_ids(
        self,
        anchor_ids: List[int],
        existing: List[Message],
    ) -> List[Message]:
        """Fetch missing range anchors directly without scanning the full span."""
        if not anchor_ids:
            return existing

        seen = {m.id for m in existing}
        ids: List[int] = []
        for msg_id in anchor_ids:
            msg_id = int(msg_id)
            if msg_id in seen or msg_id in ids:
                continue
            ids.append(msg_id)

        if not ids:
            return existing

        logger.info(
            "TopicExtractor: hydrating missing range anchor(s) topic=%d ids=%s",
            self._cfg.topic_id,
            ids,
        )

        try:
            fetched = await self._fetch_messages_by_ids(ids)
        except Exception as e:
            if is_floodwait_error(e):
                raise
            logger.warning(
                "TopicExtractor: missing anchor hydration failed topic=%d ids=%s: %s",
                self._cfg.topic_id,
                ids,
                e,
            )
            return existing

        recovered: List[Message] = []
        for msg in fetched:
            if not msg or getattr(msg, "empty", False):
                continue
            if msg.id in seen:
                continue
            if not self._is_topic_message(msg):
                logger.warning(
                    "TopicExtractor: hydrated anchor id=%s is not in topic=%d",
                    getattr(msg, "id", None),
                    self._cfg.topic_id,
                )
                continue
            seen.add(msg.id)
            recovered.append(msg)

        unresolved_ids = [
            msg_id for msg_id in ids
            if msg_id not in seen
        ]
        if unresolved_ids:
            try:
                logger.info(
                    "TopicExtractor: retrying missing anchor hydration via get_messages "
                    "topic=%d ids=%s",
                    self._cfg.topic_id,
                    unresolved_ids,
                )
                high_level = await self._fetch_messages_by_ids_high_level(unresolved_ids)
                for msg in high_level:
                    if not msg or getattr(msg, "empty", False):
                        continue
                    if msg.id in seen:
                        continue
                    if not self._is_topic_message(msg):
                        continue
                    seen.add(msg.id)
                    recovered.append(msg)
            except Exception as e:
                if is_floodwait_error(e):
                    raise
                logger.warning(
                    "TopicExtractor: get_messages anchor hydration failed topic=%d ids=%s: %s",
                    self._cfg.topic_id,
                    unresolved_ids,
                    e,
                )

        if not recovered:
            return existing

        logger.info(
            "TopicExtractor: recovered %d missing anchor message(s) for topic=%d",
            len(recovered),
            self._cfg.topic_id,
        )
        merged = list(existing) + recovered
        merged.sort(key=lambda m: (
            m.date.timestamp() if m.date else 0,
            m.id,
        ))
        self._store_cache(merged, fully_scanned=False)
        return merged

    # ── Direct ID range fallback ────────────────────────────────────────────

    _DIRECT_RANGE_MAX_SPAN = 500  # max IDs to probe in one fallback

    async def _direct_id_range_fallback(
        self,
        start_id: int,
        end_id: int,
        existing: List[Message],
    ) -> List[Message]:
        """Fetch all IDs in [min, max] directly and merge with existing results.

        This is the last-resort path when server-side topic filters (Search,
        GetReplies) return incomplete results.  For a typical topic range of
        ≤500 IDs this is 1-5 batch ``channels.GetMessages`` requests — cheap
        and reliable.
        """
        lo = min(start_id, end_id)
        hi = max(start_id, end_id)
        span = hi - lo + 1

        if span > self._DIRECT_RANGE_MAX_SPAN:
            logger.warning(
                "TopicExtractor: direct ID range probe skipped for topic=%d "
                "(span %d exceeds cap %d)",
                self._cfg.topic_id,
                span,
                self._DIRECT_RANGE_MAX_SPAN,
            )
            return existing

        logger.info(
            "TopicExtractor: falling back to direct ID range probe "
            "topic=%d range=[%d..%d] span=%d",
            self._cfg.topic_id,
            lo,
            hi,
            span,
        )

        probe_ids = list(range(lo, hi + 1))
        seen = {m.id for m in existing}
        new_msgs: List[Message] = []

        try:
            fetched = await self._fetch_messages_by_ids(probe_ids)
            for msg in fetched:
                if msg.id in seen:
                    continue
                if not self._is_topic_message(msg):
                    continue
                seen.add(msg.id)
                new_msgs.append(msg)
        except Exception as e:
            if is_floodwait_error(e):
                raise
            logger.warning(
                "TopicExtractor: direct ID range probe failed topic=%d: %s",
                self._cfg.topic_id,
                e,
            )
            return existing

        if new_msgs:
            logger.info(
                "TopicExtractor: direct probe recovered %d messages for topic=%d",
                len(new_msgs),
                self._cfg.topic_id,
            )
            merged = list(existing) + new_msgs
            merged.sort(key=lambda m: (
                m.date.timestamp() if m.date else 0,
                m.id,
            ))
            self._store_cache(merged, fully_scanned=False)
            return merged

        return existing
