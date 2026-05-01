"""
core/entity_rebuilder.py - Remove CUSTOM_EMOJI entities; validate remaining entities.

ROOT CAUSE OF ENTITY_BOUNDS_INVALID:
  Premium-created posts can include CUSTOM_EMOJI MessageEntity overlays that
  point to animated emoji not available to non-premium accounts. When the bot
  tries to forward these entities, Telegram rejects the entire message with
  ENTITY_BOUNDS_INVALID if any entity references an invalid range.

CORRECT APPROACH (implemented here):
  1. Iterate entities. Discard any whose type == CUSTOM_EMOJI.
  2. TEXT IS NEVER MODIFIED. The fallback Unicode glyph Telegram already
     embedded in the text string stays there. Only the entity object is removed.
  3. Because the text is unchanged, ALL other entity offsets remain valid —
     there is nothing to shift. We only rebuild MessageEntity objects to strip
     the custom_emoji_id attribute and to validate bounds.
  4. Final validation: drop any entity whose offset+length exceeds the
     UTF-16 length of the text.

INVARIANT: parse_mode is NEVER used. Entities are sent raw.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType

from core.utf16_utils import utf16_len

logger = logging.getLogger(__name__)


def strip_custom_emoji_entities(
    text: str,
    entities: Optional[List[MessageEntity]],
) -> Tuple[str, List[MessageEntity]]:
    """
    Remove CUSTOM_EMOJI entity overlays and return validated remaining entities.

    TEXT IS NEVER MODIFIED. The Unicode fallback glyphs stay in the text.
    Only the CUSTOM_EMOJI MessageEntity objects are removed from the list.
    All other entities retain their original offsets (no shift needed because
    the text bytes are untouched).

    Args:
        text:     Raw message text (returned unchanged).
        entities: Original MessageEntity list from the source message.

    Returns:
        (text, rebuilt_entities)
        - text is identical to input.
        - rebuilt_entities has no CUSTOM_EMOJI entries; all other entities
          are fresh objects with custom_emoji_id=None, validated against text.
    """
    if not entities:
        return text, []

    text_utf16 = utf16_len(text)
    rebuilt: List[MessageEntity] = []

    for e in entities:
        # Drop custom emoji entities entirely
        if getattr(e, "type", None) == MessageEntityType.CUSTOM_EMOJI:
            continue

        off = getattr(e, "offset", 0)
        lng = getattr(e, "length", 0)

        if off < 0 or lng <= 0:
            continue

        # Clamp to text boundary (defensive — valid source msgs shouldn't need this)
        if off + lng > text_utf16:
            lng = text_utf16 - off
            if lng <= 0:
                logger.debug(
                    "entity out of text bounds: type=%s offset=%d, dropping", e.type, off
                )
                continue

        try:
            new_e = MessageEntity(
                type=e.type,
                offset=off,
                length=lng,
                url=getattr(e, "url", None),
                user=getattr(e, "user", None),
                language=getattr(e, "language", None),
                custom_emoji_id=None,
            )
            rebuilt.append(new_e)
        except Exception as exc:
            logger.debug("Failed to build entity %s: %s", e.type, exc)

    logger.debug(
        "strip_custom_emoji_entities: kept %d/%d entities (text unchanged, len=%d utf16)",
        len(rebuilt),
        len(entities),
        text_utf16,
    )
    return text, rebuilt


def validate_entities(text: str, entities: List[MessageEntity]) -> List[MessageEntity]:
    """
    Final validation gate: drop any entity whose bounds exceed the text.

    Call this as a last step before every send_message / send_photo / etc.
    Uses UTF-16 length — never Python len().
    """
    if not entities:
        return []
    text_utf16 = utf16_len(text)
    valid = []
    for e in entities:
        off = getattr(e, "offset", -1)
        lng = getattr(e, "length", 0)
        if off < 0 or lng <= 0:
            continue
        if off + lng > text_utf16:
            logger.warning(
                "validate_entities: dropping entity type=%s offset=%d length=%d "
                "text_utf16=%d",
                e.type, off, lng, text_utf16,
            )
            continue
        valid.append(e)
    return valid
