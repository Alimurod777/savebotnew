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
from typing import AsyncGenerator, List, Optional, Set

from pyrogram.errors import FloodWait
from pyrogram.types import Message

logger = logging.getLogger(__name__)


@dataclass
class TopicExtractorConfig:
    chat_id: int
    topic_id: int                    # ID of the message that created the topic
    fetch_batch_size: int = 200      # MTProto max is 200
    inter_page_delay: float = 0.3    # seconds between pages
    flood_backoff_cap: float = 120.0 # max extra sleep added on top of FloodWait


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

    # ── Low-level fetch ────────────────────────────────────────────────────────

    async def _fetch_page(self, offset_id: int = 0) -> List[Message]:
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
                msgs: List[Message] = []
                async for msg in self._client.get_chat_history(
                    self._cfg.chat_id,
                    limit=self._cfg.fetch_batch_size,
                    offset_id=offset_id,
                    message_thread_id=self._cfg.topic_id,
                ):
                    msgs.append(msg)
                logger.debug(
                    "TopicExtractor: page offset_id=%d returned %d msgs",
                    offset_id, len(msgs),
                )
                return msgs

            except FloodWait as e:
                wait = getattr(e, "value", getattr(e, "x", 30))
                attempt += 1
                extra = min(attempt * 5, self._cfg.flood_backoff_cap)
                total_wait = wait + extra
                logger.warning(
                    "TopicExtractor: FloodWait %ds (attempt %d) — sleeping %.0fs",
                    wait, attempt, total_wait,
                )
                await asyncio.sleep(total_wait)

            except Exception as e:
                logger.error("TopicExtractor: fatal fetch error: %s", e)
                raise TopicExtractionError(f"fetch failed: {e}") from e

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
        if msg.id == self._cfg.topic_id:
            return True

        # Primary: Pyrofork/Pyrogram attribute name
        top_id: Optional[int] = getattr(msg, "reply_to_top_message_id", None)

        # Fallback: nested reply_to object (some Pyrofork builds)
        if top_id is None:
            reply_to = getattr(msg, "reply_to", None)
            if reply_to is not None:
                top_id = getattr(reply_to, "reply_to_top_message_id", None)

        if top_id is not None:
            return top_id == self._cfg.topic_id

        # Last resort: first-level direct reply to topic root
        reply_id: Optional[int] = getattr(msg, "reply_to_message_id", None)
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

        while True:
            page = await self._fetch_page(offset_id=offset_id)

            if not page:
                break

            clean: List[Message] = []
            for msg in page:
                if msg.id in seen:
                    continue
                if not self._is_topic_message(msg):
                    logger.debug(
                        "TopicExtractor: filtered non-topic msg id=%d", msg.id
                    )
                    continue
                seen.add(msg.id)
                clean.append(msg)

            if clean:
                pages.append(clean)

            if len(page) < self._cfg.fetch_batch_size:
                break  # Last page

            offset_id = page[-1].id
            await asyncio.sleep(self._cfg.inter_page_delay)

        # Reconstruct chronological order without a full re-sort
        result: List[Message] = []
        for page in reversed(pages):
            result.extend(reversed(page))

        # Safety sort: (timestamp, id) handles any edge-case ordering anomalies
        result.sort(key=lambda m: (
            m.date.timestamp() if m.date else 0,
            m.id,
        ))

        logger.info(
            "TopicExtractor: topic=%d collected %d messages",
            self._cfg.topic_id, len(result),
        )
        return result

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
