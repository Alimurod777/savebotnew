"""
core/migration_engine.py - Production-grade Telegram message migration system.

MAIN OBJECTIVE:
    Repost any Telegram message (text, media, caption, formatting, hyperlinks)
    while automatically adapting behaviour based on Premium vs Non-Premium session.

MODULAR ARCHITECTURE (all existing modules, wired together here):
    Session Manager      → core/session_store.py    (sessions/ folder handler)
    Mongo Cache Layer    → core/mongo_cache.py      (RAM-first, write-back)
    Premium Detector     → core/premium_logic.py    (get_me().is_premium + cache)
    UTF-16 Entity Engine → core/entity_splitter.py  + core/entity_rebuilder.py
    Hyperlink-Safe Split → core/repost_router.py    (split_text_respecting_hyperlinks)
    Uploader Engine      → this module              (send via user session only)
    Per-User Flood Ctrl  → core/flood_controller.py (independent queues)

PREMIUM SESSION:
    → me.is_premium == True
    → Do NOT modify message
    → Do NOT split, sanitize entities, or strip emoji
    → copy_message preserves everything (custom emoji, extended limits)
    → 1:1 repost behaviour

NON-PREMIUM SAFE CONVERSION PIPELINE:
    1. Extract raw text/caption (no Markdown rebuild)
    2. Read entities directly from Telegram
    3. REMOVE all CUSTOM_EMOJI entities (text stays unchanged — fallback glyphs)
    4. KEEP bold / italic / underline / links / mentions intact
    5. Recalculate entity offsets using UTF-16 indexing
    6. Split if needed (text → 4096, caption → 1024 UTF-16 units)
    7. Upload using USER SESSION (never Bot API)

UPLOAD METHOD (CRITICAL):
    ALL sending uses user session:  client.send_message / send_photo / etc.
    ❌ Bot API is NEVER used (causes FloodWait collisions and formatting loss).

FLOODWAIT CONTROL:
    Each user has independent queue, retry handler, and rate limiter.
    One user hitting FloodWait does NOT affect others.

FAILURE TOLERANCE:
    Handles: missing media, WEB_PAGE errors, SQLite lock, MongoDB offline,
    Colab filesystem reset.  Never crashes the worker.
"""

import asyncio
import logging
from typing import Optional, List, Tuple

from pyrogram import Client
from pyrogram.types import Message, MessageEntity
from pyrogram.enums import MessageEntityType
from pyrogram.errors import (
    AuthKeyUnregistered, AuthKeyInvalid,
    SessionRevoked, SessionExpired, UserDeactivated,
    FloodWait, RPCError,
)

from core.entity_splitter import utf16_len, TextChunk, CAPTION_LIMIT, MESSAGE_LIMIT
from core.entity_rebuilder import strip_custom_emoji_entities, validate_entities
from core.repost_router import (
    split_text_respecting_hyperlinks,
    _detect_media_type,
    _get_file_id,
    _add_media_attributes,
)
from core.media_classifier import has_downloadable_media
from core.flood_controller import flood_controller
from core.mongo_cache import mongo_cache

logger = logging.getLogger(__name__)

# Fatal errors — session is permanently dead, never retry
_FATAL_ERRORS = (
    AuthKeyUnregistered, AuthKeyInvalid,
    SessionRevoked, SessionExpired, UserDeactivated,
)


class MigrationEngine:
    """
    Production-grade message migration with premium-aware routing.

    Usage:
        from core.migration_engine import migration_engine

        sent = await migration_engine.migrate_message(
            client=user_client,       # Active Pyrogram user session
            original=source_msg,      # Message to repost
            target_chat=chat_id,      # Destination
            user_id=user_id,          # For flood isolation & premium cache
            file_path=downloaded,     # Optional: path to downloaded media
        )
    """

    # ==================== PREMIUM DETECTION ====================

    async def detect_premium(self, client: Client, user_id: int) -> bool:
        """
        Detect if the active session is Telegram Premium.

        Uses in-memory cache (MongoCache) to avoid repeated get_me() calls.
        Cache TTL is 5 minutes — after which a live MTProto check is performed.

        Detection: me = await client.get_me(); me.is_premium
        """
        # Check RAM cache first
        cached = await mongo_cache.get_premium_status(user_id)
        if cached is not None:
            return cached

        # Live check via MTProto
        try:
            me = await client.get_me()
            is_premium = getattr(me, 'is_premium', False) or False
        except Exception as e:
            logger.warning("Premium detection failed for %d: %s", user_id, e)
            is_premium = False

        # Store in RAM cache (NOT written to MongoDB)
        await mongo_cache.set_premium_status(user_id, is_premium)
        return is_premium

    # ==================== MAIN ENTRY POINT ====================

    async def migrate_message(
        self,
        client: Client,
        original: Message,
        target_chat: int,
        user_id: int,
        reply_to_message_id: Optional[int] = None,
        file_path: Optional[str] = None,
        progress=None,
    ) -> List[Message]:
        """
        Repost a message with automatic Premium/Non-Premium routing.

        This is the single entry point for the migration system.
        ALL sends go through per-user FloodController.
        ALL sends use the USER SESSION (never Bot API).

        Args:
            client:               Active Pyrogram user session
            original:             Source message to repost
            target_chat:          Destination chat ID
            user_id:              User's Telegram ID (for premium cache & flood isolation)
            reply_to_message_id:  Optional reply target in destination
            file_path:            Path to downloaded media file (or None)
            progress:             Upload progress callback

        Returns:
            List of sent Message objects (may be >1 for split messages)
        """
        is_premium = await self.detect_premium(client, user_id)

        if is_premium:
            logger.info("User %d: Premium session → direct copy", user_id)
            try:
                result = await self._premium_copy(
                    client, original, target_chat, user_id,
                    reply_to_message_id, file_path, progress,
                )
                if result:
                    return result
                logger.warning("User %d: Premium copy failed, falling back", user_id)
            except _FATAL_ERRORS:
                raise  # Propagate fatal session errors
            except Exception as e:
                logger.warning("User %d: Premium path error: %s, falling back", user_id, e)

        # Non-Premium fallback (or Premium fallback on failure)
        logger.info("User %d: Non-Premium → safe conversion pipeline", user_id)
        return await self._non_premium_pipeline(
            client, original, target_chat, user_id,
            reply_to_message_id, file_path, progress,
        )

    # ==================== PREMIUM PATH (MODE A) ====================

    async def _premium_copy(
        self,
        client: Client,
        original: Message,
        target_chat: int,
        user_id: int,
        reply_to_message_id: Optional[int],
        file_path: Optional[str],
        progress,
    ) -> Optional[List[Message]]:
        """
        Premium path: NO modification, NO splitting, NO entity sanitisation.

        copy_message preserves everything including custom emoji.
        Sends via user session through FloodController.
        """
        # Try copy_message first (preserves everything for Premium)
        copy_kwargs = {
            'chat_id': target_chat,
            'from_chat_id': original.chat.id,
            'message_id': original.id,
        }
        if reply_to_message_id:
            copy_kwargs['reply_to_message_id'] = reply_to_message_id

        msg, err = await flood_controller.execute(
            user_id=user_id,
            send_func=client.copy_message,
            kwargs=copy_kwargs,
        )
        if msg:
            return [msg]

        # copy_message failed — try manual send (still premium, no stripping)
        if file_path and has_downloadable_media(original):
            return await self._send_media(
                client, original, target_chat, user_id,
                reply_to_message_id, file_path, progress,
                strip_emoji=False,
            )

        if original.text:
            text = original.text
            entities = list(original.entities) if original.entities else []
            kw = {
                'chat_id': target_chat,
                'text': text,
                'disable_web_page_preview': True,
            }
            if entities:
                kw['entities'] = entities
            if reply_to_message_id:
                kw['reply_to_message_id'] = reply_to_message_id

            msg, err = await flood_controller.execute(
                user_id=user_id,
                send_func=client.send_message,
                kwargs=kw,
            )
            return [msg] if msg else None

        return None

    # ==================== NON-PREMIUM PIPELINE (MODE C) ====================

    async def _non_premium_pipeline(
        self,
        client: Client,
        original: Message,
        target_chat: int,
        user_id: int,
        reply_to_message_id: Optional[int],
        file_path: Optional[str],
        progress,
    ) -> List[Message]:
        """
        Non-Premium safe conversion pipeline:

        1. Extract raw text/caption (no Markdown rebuild)
        2. Read entities directly from Telegram
        3. REMOVE all CUSTOM_EMOJI entities
        4. KEEP bold / italic / underline / links / mentions
        5. Recalculate entity offsets (UTF-16)
        6. Split if needed (hyperlink-safe)
        7. Upload via USER SESSION through FloodController
        """
        # Step 1-2: Extract content
        text, entities, is_media = self._extract_content(original)

        # Resolve media source
        if is_media and not file_path:
            file_id = _get_file_id(original)
            if not file_id:
                # No downloadable file — treat as text (likely web preview)
                is_media = False
            else:
                file_path = file_id

        # Step 3-5: Strip CUSTOM_EMOJI + validate entities
        if text:
            text, entities = strip_custom_emoji_entities(text, entities)
            entities = validate_entities(text, entities)

        if is_media and file_path:
            return await self._send_media(
                client, original, target_chat, user_id,
                reply_to_message_id, file_path, progress,
                strip_emoji=False,  # Already stripped above
                text_override=text,
                entities_override=entities,
            )
        else:
            return await self._send_text(
                client, target_chat, user_id,
                text, entities, reply_to_message_id,
            )

    # ==================== CONTENT EXTRACTION ====================

    @staticmethod
    def _extract_content(
        msg: Message,
    ) -> Tuple[str, List[MessageEntity], bool]:
        """Extract raw text, entities, and media presence from a message.

        Uses attribute-based media detection (never msg.media enum).
        """
        is_media = has_downloadable_media(msg)

        if msg.text:
            return msg.text, list(msg.entities or []), is_media
        elif msg.caption:
            return msg.caption, list(msg.caption_entities or []), is_media
        else:
            return "", [], is_media

    # ==================== TEXT SENDING ====================

    async def _send_text(
        self,
        client: Client,
        target_chat: int,
        user_id: int,
        text: str,
        entities: List[MessageEntity],
        reply_to_message_id: Optional[int],
    ) -> List[Message]:
        """Send text with non-premium splitting (4096 UTF-16 limit).

        All sends go through FloodController for per-user isolation.
        """
        if not text:
            return []

        # Step 6: Hyperlink-safe UTF-16 splitting
        chunks = split_text_respecting_hyperlinks(text, entities, MESSAGE_LIMIT)
        sent: List[Message] = []
        reply_to = reply_to_message_id

        for i, chunk in enumerate(chunks):
            kw: dict = {
                'chat_id': target_chat,
                'text': chunk.text,
                'disable_web_page_preview': True,
            }
            if chunk.entities:
                kw['entities'] = chunk.entities
            if reply_to:
                kw['reply_to_message_id'] = reply_to

            # Step 7: Upload via USER SESSION
            msg, err = await flood_controller.execute(
                user_id=user_id,
                send_func=client.send_message,
                kwargs=kw,
            )

            if msg:
                sent.append(msg)
                reply_to = msg.id
            elif err:
                logger.warning("User %d: text chunk %d/%d failed: %s",
                               user_id, i + 1, len(chunks), err)

        return sent

    # ==================== MEDIA SENDING ====================

    async def _send_media(
        self,
        client: Client,
        original: Message,
        target_chat: int,
        user_id: int,
        reply_to_message_id: Optional[int],
        file_path: str,
        progress,
        strip_emoji: bool = True,
        text_override: Optional[str] = None,
        entities_override: Optional[List[MessageEntity]] = None,
    ) -> List[Message]:
        """Send media with caption handling and overflow reply chain.

        If caption exceeds 1024 UTF-16 units (non-premium):
          - First chunk sent as media caption
          - Remaining chunks sent as reply chain: "📌 Davomi X/N"

        All sends go through FloodController for per-user isolation.
        """
        sent: List[Message] = []

        # Determine media type
        media_type = _detect_media_type(original)
        if not media_type:
            media_type = 'document'

        # Resolve caption + entities
        if text_override is not None:
            caption = text_override
            entities = entities_override or []
        else:
            caption = original.caption or ""
            entities = list(original.caption_entities or [])
            if strip_emoji and caption:
                caption, entities = strip_custom_emoji_entities(caption, entities)
                entities = validate_entities(caption, entities)

        # Step 6: Split caption (hyperlink-safe)
        caption_chunks: List[TextChunk] = []
        if caption:
            caption_chunks = split_text_respecting_hyperlinks(
                caption, entities, CAPTION_LIMIT,
            )

        # Build media kwargs
        media_kwargs: dict = {
            'chat_id': target_chat,
            media_type: file_path,
        }
        _add_media_attributes(media_kwargs, original, media_type)

        if progress:
            media_kwargs['progress'] = progress
        if reply_to_message_id:
            media_kwargs['reply_to_message_id'] = reply_to_message_id

        # First caption chunk goes with media
        if caption_chunks:
            first = caption_chunks[0]
            media_kwargs['caption'] = first.text
            if first.entities:
                media_kwargs['caption_entities'] = first.entities

        # Step 7: Send media via FloodController
        send_func = getattr(client, f'send_{media_type}', None)
        if not send_func:
            logger.error("User %d: no send method for: %s", user_id, media_type)
            return []

        media_msg, err = await flood_controller.execute(
            user_id=user_id,
            send_func=send_func,
            kwargs=media_kwargs,
        )

        # Fallback: retry without caption entities
        if media_msg is None and 'caption_entities' in media_kwargs:
            del media_kwargs['caption_entities']
            media_msg, err = await flood_controller.execute(
                user_id=user_id,
                send_func=send_func,
                kwargs=media_kwargs,
            )

        if not media_msg:
            logger.error("User %d: media send failed: %s", user_id, err)
            return []

        sent.append(media_msg)

        # Send overflow caption as reply chain: "📌 Davomi X/N"
        if len(caption_chunks) > 1:
            total_parts = len(caption_chunks)
            reply_to = media_msg.id

            for i, chunk in enumerate(caption_chunks[1:], start=2):
                header = f"📌 Davomi {i}/{total_parts}\n\n"
                overflow_text = header + chunk.text

                # Adjust entity offsets for the prepended header
                header_utf16 = utf16_len(header)
                adjusted_entities: List[MessageEntity] = []
                for e in chunk.entities:
                    try:
                        new_e = MessageEntity(
                            type=e.type,
                            offset=e.offset + header_utf16,
                            length=e.length,
                            url=getattr(e, 'url', None),
                            user=getattr(e, 'user', None),
                            language=getattr(e, 'language', None),
                        )
                        if new_e.offset + new_e.length <= utf16_len(overflow_text):
                            adjusted_entities.append(new_e)
                    except Exception:
                        pass

                overflow_kwargs: dict = {
                    'chat_id': target_chat,
                    'text': overflow_text,
                    'reply_to_message_id': reply_to,
                    'disable_web_page_preview': True,
                }
                if adjusted_entities:
                    overflow_kwargs['entities'] = adjusted_entities

                msg, err = await flood_controller.execute(
                    user_id=user_id,
                    send_func=client.send_message,
                    kwargs=overflow_kwargs,
                )
                if msg:
                    sent.append(msg)
                    reply_to = msg.id

        return sent

    # ==================== LIFECYCLE ====================

    async def shutdown(self) -> None:
        """Graceful shutdown: flush MongoDB cache."""
        await mongo_cache.flush_all()
        logger.info("MigrationEngine shutdown complete")


# Module singleton
migration_engine = MigrationEngine()
