"""
core/utf16_utils.py - Canonical UTF-16 measurement helpers.

Telegram's MTProto protocol measures ALL text positions in UTF-16 code units,
NOT Python character indices or UTF-8 bytes. Every entity offset/length MUST
be computed with these functions.

Rules:
  - BMP characters (U+0000..U+FFFF) → 1 UTF-16 code unit
  - Supplementary characters (U+10000..U+10FFFF) → 2 UTF-16 code units (surrogate pair)
  - Lone surrogates from corrupted captions → 1 code unit (surrogatepass)

NEVER use len(text), text.encode('utf-8'), or byte counts for entity math.
"""

from __future__ import annotations


def utf16_len(text: str) -> int:
    """
    Return the number of UTF-16 code units in *text*.

    This is what Telegram calls "length" in every MessageEntity.
    """
    if not text:
        return 0
    try:
        return len(text.encode("utf-16-le", errors="surrogatepass")) // 2
    except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
        # Manual fallback: supplementary chars count as 2 units
        count = 0
        for ch in text:
            count += 2 if ord(ch) > 0xFFFF else 1
        return count


def utf16_to_char_index(text: str, utf16_offset: int) -> int:
    """
    Convert a UTF-16 offset into the equivalent Python string (char) index.

    Used to slice text for chunk extraction.
    Returns len(text) if offset is beyond the string.
    """
    if utf16_offset <= 0:
        return 0
    count = 0
    for i, ch in enumerate(text):
        if count >= utf16_offset:
            return i
        count += 2 if ord(ch) > 0xFFFF else 1
    return len(text)


def char_to_utf16_offset(text: str, char_index: int) -> int:
    """
    Convert a Python string (char) index into a UTF-16 offset.

    Used when we have a character position and need the entity offset.
    """
    if char_index <= 0:
        return 0
    count = 0
    for ch in text[:char_index]:
        count += 2 if ord(ch) > 0xFFFF else 1
    return count


def utf16_slice(text: str, start_utf16: int, end_utf16: int) -> str:
    """
    Extract the substring between two UTF-16 offsets.

    Equivalent to text[start:end] but using UTF-16 indices.
    Uses str() to avoid Pyrogram Str.__getitem__ → remove_surrogates errors.
    """
    start_char = utf16_to_char_index(text, start_utf16)
    end_char = utf16_to_char_index(text, end_utf16)
    # str() ensures we use plain str slicing, not Pyrogram Str.__getitem__
    return str.__getitem__(text, slice(start_char, end_char))
