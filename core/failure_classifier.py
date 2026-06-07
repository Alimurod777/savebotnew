"""
Failure classification layer for post-delivery diagnostics.

Single source of truth that decides, for every download/upload failure,
which audience (if any) should be notified. The goal is to stop flooding the
OWNER chat with normal Telegram operational conditions (deleted posts, message
gaps, FloodWait, old invalid links) and reserve owner incident reports for
genuine system failures that require investigation.

Notification matrix
-------------------
    EXPECTED_TELEGRAM_STATE  -> owner: NO   user: no    stats: yes
    USER_INPUT_ERROR         -> owner: NO   user: yes   stats: yes
    SYSTEM_FAILURE           -> owner: YES* user: yes   stats: yes

    * SYSTEM_FAILURE only reaches the owner when the post is actually
      reportable (see ``is_reportable_to_owner``): the message was fetched,
      internal processing had started, or a real system exception was raised.
      A message that never existed produces no owner incident report.

This module is pure (no I/O, no Telegram calls) so it is trivially testable and
safe to import from anywhere in the pipeline.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional


class FailureCategory(str, Enum):
    """How a post failure should be treated for notification routing."""

    EXPECTED_TELEGRAM_STATE = "expected_telegram_state"
    USER_INPUT_ERROR = "user_input_error"
    SYSTEM_FAILURE = "system_failure"


# ---------------------------------------------------------------------------
# Keyword tables. Matching is done against the UPPERCASED concatenation of
# stage + reason + error type/text, mirroring the existing logic in
# save._user_failure_text so classification and the user-facing message agree.
# ---------------------------------------------------------------------------

# Genuine system failures — these are the only conditions that page the owner.
# Strong signal of a bug or infrastructure fault in OUR pipeline, not a normal
# Telegram state. Checked FIRST so an upload/relay/routing fault wins even if
# the error text also happens to mention a transient word.
_CRITICAL_SYSTEM_TOKENS = (
    "PEER_ID_INVALID",
    "PEERIDINVALID",
    "INVALID_PEER",
    "CHANNEL_INVALID",
    "CHANNELINVALID",
    "CORRUPT",
    "CONTAMINATION",
    "OWNERSHIP",
    "MISMATCH",
    "ROUTING",
    "RELAY",
    "QUEUE",
    "WORKER",
    "POOL",
    "RECOVERY",
    "VALIDATION",
    "VALIDATE",
    "ZERO BYTE",
    "ZERO-BYTE",
    "0 B",
    "EMPTY FILE",
    "DOWNLOADER",
    "DOWNLOAD CORRUPTION",
    "UPLOAD CORRUPTION",
)

_PROCESSING_FAILURE_TOKENS = (
    "UPLOAD",
    "SEND_MEDIA",
    "SEND_LOCATION",
    "SEND_VENUE",
    "SEND_POLL",
    "SEND_TEXT",
    "SEND_QUIZBOT",
    "ALBUM",
    "EXTRACT",
    "RETURNED FALSE",
    "RETURNED STATUS",
    "DOWNLOAD_AND_SEND_MEDIA",
)

# User-supplied input problems — notify the user, never the owner.
_USER_INPUT_TOKENS = (
    "MALFORMED",
    "INVALID URL",
    "INVALID LINK",
    "INVALID_URL",
    "BAD URL",
    "UNSUPPORTED",
    "WRONG TOPIC",
    "PARSE",
    "USERNAME_INVALID",
    "USERNAME_NOT_OCCUPIED",
    "INVITE_HASH_INVALID",
    "INVITE_HASH_EXPIRED",
)

# Normal Telegram operational conditions — never page the owner. These are the
# dominant source of report spam in production (deleted posts + FloodWait).
_EXPECTED_STATE_TOKENS = (
    # deleted / missing / gap
    "DELETED",
    "INACCESSIBLE",
    "EMPTY",
    "MSG_ID_INVALID",
    "MSGIDINVALID",
    "MESSAGE_ID_INVALID",
    "MESSAGEIDINVALID",
    "MESSAGE_NOT_FOUND",
    "MESSAGENOTFOUND",
    "NOT FOUND",
    "POST_MISSING",
    "MISSING",
    "REMOVED",
    "NON_EXISTING",
    "NON-EXISTING",
    "NOT_AVAILABLE",
    "NOT AVAILABLE",
    "UNAVAILABLE",
    "MESSAGE GAP",
    "MESSAGE_GAP",
    "EMPTY_MESSAGE_GAP",
    # transient throttling / timeouts
    "FLOODWAIT",
    "FLOOD_WAIT",
    "FLOOD",
    "SLOWMODE",
    "TIMEOUT",
    "TIMED OUT",
    "CONNECTION TIMEOUT",
    # access / protection conditions controlled by the source, not by us
    "RESTRICT",
    "PROTECTED",
    "FORBIDDEN",
    "CHANNEL_PRIVATE",
    "CHANNELPRIVATE",
    "USER_BANNED",
    "USER_BANNED_IN_CHANNEL",
    "CHAT_ADMIN_REQUIRED",
    "CHATADMINREQUIRED",
)

# Stages that mean internal processing had already started (used by the
# reportability gate and as a system-failure hint).
_PROCESSING_STAGE_TOKENS = (
    "ALBUM",
    "SEND_MEDIA",
    "UPLOAD",
    "RELAY",
    "EXTRACT",
    "PROCESS",
    "COPY",
)


def _signal(stage: Any, reason: Any, error: Any = None) -> str:
    """Build the uppercased text signal used for keyword matching."""
    parts = [str(stage or ""), str(reason or "")]
    if error is not None:
        parts.append(type(error).__name__)
        parts.append(str(error))
    return " ".join(parts).upper()


def _is_real_exception(error: Any) -> bool:
    """True when ``error`` is an actual raised exception object."""
    return isinstance(error, BaseException)


def classify_failure(stage: Any, reason: Any, error: Any = None) -> FailureCategory:
    """
    Classify a post failure into a :class:`FailureCategory`.

    Precedence (first match wins):
        1. SYSTEM_FAILURE keywords  — explicit pipeline faults
        2. USER_INPUT_ERROR keywords
        3. EXPECTED_TELEGRAM_STATE keywords — normal Telegram conditions
        4. fallback: a real exception with no other signal is treated as an
           unexpected SYSTEM_FAILURE; anything else defaults to the benign
           EXPECTED_TELEGRAM_STATE so the owner is never paged on noise.
    """
    signal = _signal(stage, reason, error)

    if any(token in signal for token in _CRITICAL_SYSTEM_TOKENS):
        return FailureCategory.SYSTEM_FAILURE

    if any(token in signal for token in _USER_INPUT_TOKENS):
        return FailureCategory.USER_INPUT_ERROR

    if any(token in signal for token in _EXPECTED_STATE_TOKENS):
        return FailureCategory.EXPECTED_TELEGRAM_STATE

    if any(token in signal for token in _PROCESSING_FAILURE_TOKENS):
        return FailureCategory.SYSTEM_FAILURE

    # No explicit keyword. An unhandled exception is a real ("unexpected")
    # system failure and worth an owner report; a quiet failure with no
    # exception is treated as a benign operational condition.
    if _is_real_exception(error):
        return FailureCategory.SYSTEM_FAILURE

    return FailureCategory.EXPECTED_TELEGRAM_STATE


def stage_indicates_processing(stage: Any) -> bool:
    """True when the stage name shows internal processing had started."""
    s = str(stage or "").upper()
    return any(token in s for token in _PROCESSING_STAGE_TOKENS)


def is_reportable_to_owner(
    category: FailureCategory,
    *,
    message_fetched: bool = False,
    processing_started: bool = False,
    system_exception: bool = False,
) -> bool:
    """
    Final gate before paging the owner.

    Only SYSTEM_FAILURE is ever reportable, and even then only when the post is
    actually actionable: the message was successfully fetched, internal
    processing had started, or a real system exception was raised. A message
    that never existed (no fetch, no processing, no exception) yields no owner
    incident report.
    """
    if category != FailureCategory.SYSTEM_FAILURE:
        return False
    return bool(message_fetched or processing_started or system_exception)


def should_notify_user(category: FailureCategory) -> bool:
    """Expected Telegram absence is counted silently; other failures inform user."""
    return category != FailureCategory.EXPECTED_TELEGRAM_STATE


def describe(category: FailureCategory) -> str:
    """Human-readable label for logs / SQLite details."""
    return {
        FailureCategory.EXPECTED_TELEGRAM_STATE: "expected Telegram state",
        FailureCategory.USER_INPUT_ERROR: "user input error",
        FailureCategory.SYSTEM_FAILURE: "system failure",
    }.get(category, str(category))
