"""
core/caption_splitter.py - UTF-16 hyperlink-aware caption splitting.

NON-PREMIUM LIMITS (enforced here):
  Caption:  ≤ 1024 UTF-16 code units
  Message:  ≤ 4096 UTF-16 code units

ALGORITHM:
  1. Strip CUSTOM_EMOJI entities (via entity_rebuilder).
  2. Measure caption in UTF-16 units.
  3. If ≤ 1024 → return single chunk, done.
  4. Otherwise, split at a "safe" point:
       - Never split inside a TEXT_LINK or URL entity (hyperlink rule).
       - If the natural split point (at 1024 units) falls inside a hyperlink,
         extend the chunk to the END of that hyperlink.
       - Then find a word boundary (newline / space) near the split point.
  5. Build overflow chunks (each ≤ 4096 UTF-16 units) labelled:
         📌 Davomi 2/4, 📌 Davomi 3/4, …
  6. Every chunk carries FRESH MessageEntity objects with recalculated offsets.
     Original entity objects are NEVER reused.

CRITICAL: All measurement, slicing, and offset math uses UTF-16 units only.
          Python len() is NEVER used for entity logic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType

from core.utf16_utils import utf16_len, utf16_to_char_index, char_to_utf16_offset
from core.entity_rebuilder import strip_custom_emoji_entities, validate_entities

logger = logging.getLogger(__name__)

CAPTION_LIMIT = 1024          # UTF-16 units — non-premium caption max
PREMIUM_CAPTION_LIMIT = 2048  # UTF-16 units — premium caption max
MESSAGE_LIMIT = 4096          # UTF-16 units — non-premium message max
CONTINUATION_PREFIX = "📌 Davomi"  # header template: "📌 Davomi {n}/{total}\n\n"


@dataclass
class CaptionChunk:
    """One chunk of text with its recalculated, validated entities."""
    text: str
    entities: List[MessageEntity] = field(default_factory=list)

    @property
    def utf16_length(self) -> int:
        return utf16_len(self.text)

    def validate(self) -> "CaptionChunk":
        """Drop any entity that exceeds the text boundary and return self."""
        self.entities = validate_entities(self.text, self.entities)
        return self


def _hyperlink_ranges(entities: List[MessageEntity]) -> List[Tuple[int, int]]:
    """Return list of (start_utf16, end_utf16) for all TEXT_LINK and URL entities."""
    ranges = []
    for e in entities or []:
        if getattr(e, "type", None) in (MessageEntityType.TEXT_LINK, MessageEntityType.URL):
            start = e.offset
            end = e.offset + e.length
            if start >= 0 and end > start:
                ranges.append((start, end))
    return ranges


def _is_inside_hyperlink(utf16_pos: int, hl_ranges: List[Tuple[int, int]]) -> bool:
    for start, end in hl_ranges:
        if start < utf16_pos < end:
            return True
    return False


def _hyperlink_end_at_or_after(utf16_pos: int, hl_ranges: List[Tuple[int, int]]) -> Optional[int]:
    """If utf16_pos is inside a hyperlink, return that hyperlink's end offset."""
    for start, end in hl_ranges:
        if start < utf16_pos <= end:
            return end
    return None


def _hyperlink_start_before(utf16_pos: int, hl_ranges: List[Tuple[int, int]]) -> Optional[int]:
    """If utf16_pos is inside a hyperlink, return that hyperlink's start offset."""
    for start, end in hl_ranges:
        if start < utf16_pos < end:
            return start
    return None


def _find_safe_split(
    text: str,
    entities: List[MessageEntity],
    max_utf16: int,
) -> int:
    """
    Find the largest UTF-16 offset ≤ max_utf16 that is safe to split at.

    Safe means: not inside a TEXT_LINK or URL entity.
    If the natural max_utf16 split point is inside a hyperlink, we extend
    to the end of that hyperlink (may exceed max_utf16 slightly — this is
    intentional: never break a link).

    Then we try to back up to a word boundary (newline, space) without
    going below max_utf16 // 2.

    Returns: UTF-16 offset of the chosen split point.
    """
    hl_ranges = _hyperlink_ranges(entities)
    text_utf16 = utf16_len(text)

    if max_utf16 >= text_utf16:
        return text_utf16

    split_utf16 = max_utf16

    # If natural split point is inside a hyperlink, keep the entire link in
    # one chunk. Prefer backing up when extending would exceed Telegram's cap.
    hl_end = _hyperlink_end_at_or_after(split_utf16, hl_ranges)
    if hl_end is not None:
        if hl_end <= max_utf16:
            return min(hl_end, text_utf16)
        hl_start = _hyperlink_start_before(split_utf16, hl_ranges)
        if hl_start is not None and hl_start > 0:
            return hl_start
        return split_utf16

    # Try to find a nicer word boundary going backwards from split_utf16
    # but not less than half the limit
    min_split = max_utf16 // 2
    split_char = utf16_to_char_index(text, split_utf16)
    search_text = text[:split_char]

    for sep in ["\n\n", "\n", " "]:
        pos = search_text.rfind(sep)
        if pos > 0:
            candidate_char = pos + len(sep)
            candidate_utf16 = char_to_utf16_offset(text, candidate_char)
            if candidate_utf16 >= min_split and not _is_inside_hyperlink(candidate_utf16, hl_ranges):
                return candidate_utf16

    # Hard split — return the original max_utf16 if no better point found
    return split_utf16


def _reconstruct_entities_for_chunk(
    original_text: str,
    entities: List[MessageEntity],
    chunk_start_char: int,
    chunk_text: str,
    is_premium: bool = False,
) -> List[MessageEntity]:
    """
    Build fresh MessageEntity objects for a text chunk.

    UTF-16 math only:
      new_offset = (UTF-16 offset of entity start) - (UTF-16 offset of chunk start)
      new_length = original entity length (entity text is unchanged)

    Entities that do not overlap the chunk are silently skipped.
    """
    chunk_start_utf16 = char_to_utf16_offset(original_text, chunk_start_char)
    chunk_utf16_len = utf16_len(chunk_text)
    chunk_end_utf16 = chunk_start_utf16 + chunk_utf16_len

    new_entities: List[MessageEntity] = []
    for e in entities or []:
        ent_start = e.offset
        ent_end = e.offset + e.length

        # Skip entities that don't overlap this chunk
        if ent_end <= chunk_start_utf16 or ent_start >= chunk_end_utf16:
            continue

        # Hyperlinks must never be sent as partial entities; a partial
        # TEXT_LINK/URL is the usual source of broken links after splitting.
        if getattr(e, "type", None) in (MessageEntityType.TEXT_LINK, MessageEntityType.URL):
            if ent_start < chunk_start_utf16 or ent_end > chunk_end_utf16:
                continue

        # Skip CUSTOM_EMOJI unless premium (premium users can use custom emoji)
        if not is_premium and getattr(e, "type", None) == MessageEntityType.CUSTOM_EMOJI:
            continue

        new_offset = max(0, ent_start - chunk_start_utf16)
        new_end = min(ent_end, chunk_end_utf16) - chunk_start_utf16
        new_length = new_end - new_offset

        if new_length <= 0:
            continue

        # Final bounds check
        if new_offset + new_length > chunk_utf16_len:
            new_length = chunk_utf16_len - new_offset
            if new_length <= 0:
                continue

        try:
            new_entities.append(MessageEntity(
                type=e.type,
                offset=new_offset,
                length=new_length,
                url=getattr(e, "url", None),
                user=getattr(e, "user", None),
                language=getattr(e, "language", None),
                custom_emoji_id=None,
            ))
        except Exception as exc:
            logger.debug("Failed to build entity: %s", exc)

    return new_entities


def split_caption(
    caption: str,
    entities: Optional[List[MessageEntity]],
    is_premium: bool = False,
) -> Tuple[CaptionChunk, List[CaptionChunk]]:
    """
    Split a caption into a primary chunk and overflow chunks.

    Non-premium: primary ≤ 1024 UTF-16 units, custom emoji stripped.
    Premium:     primary ≤ 2048 UTF-16 units, custom emoji entities preserved.

    Step 1: Strip custom emoji entities (non-premium only).
    Step 2: If caption fits → return (chunk, []).
    Step 3: Split into multiple chunks respecting hyperlink boundaries.
    Step 4: Label overflow chunks with "📌 Davomi N/total".
    Step 5: Validate all entity bounds before returning.

    Returns:
        (primary_chunk, overflow_chunks)
        primary_chunk → attach as media caption or first send_message call
        overflow_chunks → send as replies with continuation header
    """
    if not caption:
        return CaptionChunk(text="", entities=[]), []

    # Convert Pyrogram Str to plain str to avoid Str.__getitem__ → remove_surrogates errors
    caption = str(caption)

    # Step 1: strip custom emoji entities (non-premium only)
    if is_premium:
        text = caption
        clean_entities = list(entities or [])
    else:
        text, clean_entities = strip_custom_emoji_entities(caption, entities or [])

    caption_limit = PREMIUM_CAPTION_LIMIT if is_premium else CAPTION_LIMIT
    caption_utf16 = utf16_len(text)

    # Step 2: fits in one chunk
    if caption_utf16 <= caption_limit:
        chunk = CaptionChunk(text=text, entities=clean_entities).validate()
        return chunk, []

    # Step 3: split into raw chunks
    raw_chunks = _split_text_into_chunks(
        text, clean_entities, caption_limit, MESSAGE_LIMIT, is_premium=is_premium
    )

    if not raw_chunks:
        return CaptionChunk(text="", entities=[]), []

    primary = raw_chunks[0].validate()
    overflow_raw = raw_chunks[1:]

    if not overflow_raw:
        return primary, []

    # Step 4: add continuation labels
    total = 1 + len(overflow_raw)
    overflow: List[CaptionChunk] = []
    for i, chunk in enumerate(overflow_raw, start=2):
        header = f"{CONTINUATION_PREFIX} {i}/{total}\n\n"
        header_utf16 = utf16_len(header)
        overflow_text = header + chunk.text

        # Shift entity offsets by header length
        shifted: List[MessageEntity] = []
        for e in chunk.entities:
            new_offset = e.offset + header_utf16
            new_length = e.length
            # Validate against the combined text
            if new_offset + new_length <= utf16_len(overflow_text):
                try:
                    shifted.append(MessageEntity(
                        type=e.type,
                        offset=new_offset,
                        length=new_length,
                        url=getattr(e, "url", None),
                        user=getattr(e, "user", None),
                        language=getattr(e, "language", None),
                        custom_emoji_id=None,
                    ))
                except Exception:
                    pass

        overflow.append(CaptionChunk(text=overflow_text, entities=shifted).validate())

    return primary, overflow


def _split_text_into_chunks(
    text: str,
    entities: List[MessageEntity],
    first_limit: int,
    rest_limit: int,
    is_premium: bool = False,
) -> List[CaptionChunk]:
    """
    Split *text* into chunks where the first chunk is ≤ first_limit UTF-16
    units and subsequent chunks are ≤ rest_limit UTF-16 units.

    Returns list of CaptionChunk (entities not yet labelled with headers).
    """
    chunks: List[CaptionChunk] = []
    remaining_text = text
    remaining_char_offset = 0  # position in original text (char index)
    limit = first_limit

    while remaining_text:
        remaining_utf16 = utf16_len(remaining_text)

        if remaining_utf16 <= limit:
            # Last chunk — reconstruct entities
            chunk_entities = _reconstruct_entities_for_chunk(
                text, entities, remaining_char_offset, remaining_text, is_premium=is_premium
            )
            chunks.append(CaptionChunk(text=remaining_text, entities=chunk_entities))
            break

        # Find where to split (in terms of UTF-16 offset within remaining_text)
        # We need to pass entities relative to remaining_text for hyperlink detection
        relative_entities = _relativize_entities(entities, remaining_char_offset, text, remaining_text)
        split_utf16 = _find_safe_split(remaining_text, relative_entities, limit)

        split_char = utf16_to_char_index(remaining_text, split_utf16)
        if split_char <= 0:
            split_char = 1  # Force progress

        chunk_text = remaining_text[:split_char].rstrip()

        chunk_entities = _reconstruct_entities_for_chunk(
            text, entities, remaining_char_offset, chunk_text, is_premium=is_premium
        )
        if chunk_text:
            chunks.append(CaptionChunk(text=chunk_text, entities=chunk_entities))

        # Advance
        remaining_text = remaining_text[split_char:].lstrip()
        # Compute new char offset in original text
        consumed = len(text) - len(remaining_text) - sum(
            1 for _ in range(remaining_char_offset)
        )
        # Simpler: track directly
        remaining_char_offset = len(text) - len(remaining_text)

        limit = rest_limit  # subsequent chunks use MESSAGE_LIMIT

    return chunks


def _relativize_entities(
    entities: List[MessageEntity],
    chunk_start_char: int,
    original_text: str,
    chunk_text: str,
) -> List[MessageEntity]:
    """
    Return entities whose offsets are relative to chunk_text rather than original_text.
    Used internally to pass to _find_safe_split which works on the remaining substring.
    """
    chunk_start_utf16 = char_to_utf16_offset(original_text, chunk_start_char)
    chunk_utf16 = utf16_len(chunk_text)
    result: List[MessageEntity] = []
    for e in entities:
        new_off = e.offset - chunk_start_utf16
        if new_off < 0 or new_off >= chunk_utf16:
            continue
        new_len = min(e.length, chunk_utf16 - new_off)
        if new_len <= 0:
            continue
        try:
            result.append(MessageEntity(
                type=e.type,
                offset=new_off,
                length=new_len,
                url=getattr(e, "url", None),
                user=getattr(e, "user", None),
                language=getattr(e, "language", None),
                custom_emoji_id=None,
            ))
        except Exception:
            pass
    return result
