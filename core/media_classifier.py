"""
core/media_classifier.py - Pyrofork-correct media detection.

PYROFORK DIFFERENCE FROM STANDARD PYROGRAM:
  Pyrofork 2.3.69 does NOT expose MessageMediaType.WEB_PAGE.
  Link-preview messages (web page previews) arrive as TEXT messages
  with no downloadable media object — they must be classified as text,
  not as media requiring a download.

CORRECT CLASSIFICATION RULE:
  A message has downloadable media if and only if one of these attributes
  is truthy on the Message object:
    msg.photo, msg.video, msg.document, msg.audio, msg.voice,
    msg.video_note, msg.animation, msg.sticker

  Everything else (web previews, text-only, service messages, polls) is
  treated as plain text — no download attempt, no retry loop.

DO NOT use:
  - msg.media == MessageMediaType.WEB_PAGE  (not available in Pyrofork)
  - msg.media attribute for classification  (unreliable in Pyrofork)
  - isinstance checks against media objects (fragile across versions)
"""

from __future__ import annotations

from typing import Optional

from pyrogram.types import Message


# Ordered by likelihood for early-exit performance
_MEDIA_ATTRS = (
    "photo",
    "video",
    "document",
    "audio",
    "voice",
    "video_note",
    "animation",
    "sticker",
)

# Map attribute name → send method name (matches client.send_<name>)
_ATTR_TO_SEND = {
    "photo": "photo",
    "video": "video",
    "document": "document",
    "audio": "audio",
    "voice": "voice",
    "video_note": "video_note",
    "animation": "animation",
    "sticker": "sticker",
}


def has_downloadable_media(msg: Message) -> bool:
    """
    Return True iff *msg* contains a file that can be downloaded.

    Web-page previews, text-only messages, polls, and service messages
    all return False. Only real file attachments return True.

    Never checks msg.media or MessageMediaType.
    """
    for attr in _MEDIA_ATTRS:
        if getattr(msg, attr, None):
            return True
    return False


def get_media_send_method(msg: Message) -> Optional[str]:
    """
    Return the Pyrogram send method name for the media in *msg*.

    Returns one of: "photo", "video", "document", "audio", "voice",
                    "video_note", "animation", "sticker"
    Returns None if the message has no downloadable media.

    Usage:
        method = get_media_send_method(msg)
        if method:
            await client.send_photo(...)  # or send_video, etc.
    """
    for attr in _MEDIA_ATTRS:
        if getattr(msg, attr, None):
            return _ATTR_TO_SEND[attr]
    return None


def classify_message(msg: Message) -> str:
    """
    Classify a message into one of:
      "photo", "video", "document", "audio", "voice",
      "video_note", "animation", "sticker", "poll", "text"

    "text" covers: text-only, web previews, service messages, empty.

    This is the single authoritative classifier for the repost pipeline.
    """
    for attr in _MEDIA_ATTRS:
        if getattr(msg, attr, None):
            return attr  # e.g. "photo", "video", ...

    # Poll — renderable but not downloadable
    if getattr(msg, "poll", None):
        return "poll"

    return "text"


def is_text_message(msg: Message) -> bool:
    """
    Return True if the message should be handled as plain text.

    Includes web-page previews (no download needed).
    """
    return not has_downloadable_media(msg) and not getattr(msg, "poll", None)
