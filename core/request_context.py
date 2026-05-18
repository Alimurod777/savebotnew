"""
core/request_context.py — Request-scoped structured logging via contextvars.

Every async request (save(), process_*) creates a RequestContext that
propagates automatically through all awaited calls. Log records are
enriched with request metadata so concurrent requests from different
users never intermingle in log output.

Usage:
    from core.request_context import set_request_context, get_request_context, RequestContext

    # At request entry point (save handler):
    ctx = RequestContext(request_id="abc123", user_id=42, ...)
    set_request_context(ctx)

    # Anywhere downstream — automatic via logging filter:
    logger.info("downloading file")
    # Output: 2026-05-18 ... [req=abc123 user=42 ...] downloading file

Architecture:
    - contextvars.ContextVar is async-safe and zero-overhead
    - RequestContextFilter is installed on root logger once at startup
    - Each request creates+sets its own context; child coroutines inherit it
    - No locks, no shared mutable state, no contention
"""

from __future__ import annotations

import uuid
import logging
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Optional

# ── Request context dataclass ────────────────────────────────────────────────

@dataclass
class RequestContext:
    """Immutable context for a single user request."""

    request_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    requester_user_id: Optional[int] = None
    uploader_session_id: Optional[str] = None       # first 16 chars of session string
    premium_session_id: Optional[str] = None         # pool session prefix if used
    sender_mode: Optional[str] = None                # "bot" | "user_session" | "pool_session"
    topic_id: Optional[int] = None
    message_thread_id: Optional[int] = None
    media_group_id: Optional[str] = None
    source_message_id: Optional[int] = None
    target_chat_id: Optional[int] = None
    routing_mode: Optional[str] = None               # "private" | "public" | "topic" | "thread"
    worker_id: Optional[str] = None                  # "pool_3" | "user_42" etc.
    source_chat_id: Optional[int] = None

    def to_log_dict(self) -> dict:
        """Return only non-None fields for compact log output."""
        d = {}
        if self.request_id:
            d["req"] = self.request_id
        if self.requester_user_id is not None:
            d["user"] = self.requester_user_id
        if self.sender_mode:
            d["sender"] = self.sender_mode
        if self.routing_mode:
            d["route"] = self.routing_mode
        if self.target_chat_id is not None:
            d["target"] = self.target_chat_id
        if self.source_chat_id is not None:
            d["src_chat"] = self.source_chat_id
        if self.source_message_id is not None:
            d["src_msg"] = self.source_message_id
        if self.topic_id is not None:
            d["topic"] = self.topic_id
        if self.message_thread_id is not None:
            d["thread"] = self.message_thread_id
        if self.media_group_id is not None:
            d["album"] = self.media_group_id
        if self.worker_id:
            d["worker"] = self.worker_id
        if self.uploader_session_id:
            d["sess"] = self.uploader_session_id
        if self.premium_session_id:
            d["prem_sess"] = self.premium_session_id
        return d

    def format_prefix(self) -> str:
        """Compact log prefix string."""
        parts = [f"{k}={v}" for k, v in self.to_log_dict().items()]
        return f"[{' '.join(parts)}]" if parts else ""

    def with_updates(self, **kwargs) -> "RequestContext":
        """Return a shallow copy with specified fields updated."""
        import dataclasses
        return dataclasses.replace(self, **kwargs)


# ── ContextVar ────────────────────────────────────────────────────────────────

_request_ctx: ContextVar[Optional[RequestContext]] = ContextVar(
    "request_ctx", default=None
)


def set_request_context(ctx: RequestContext) -> None:
    """Set the request context for the current async task."""
    _request_ctx.set(ctx)


def get_request_context() -> Optional[RequestContext]:
    """Get the current request context (None if outside a request)."""
    return _request_ctx.get()


def clear_request_context() -> None:
    """Clear the request context (called at request end)."""
    _request_ctx.set(None)


# ── Logging filter ────────────────────────────────────────────────────────────

class RequestContextFilter(logging.Filter):
    """
    Logging filter that injects request context fields into every log record.

    Install once on the root logger:
        logging.getLogger().addFilter(RequestContextFilter())

    Then the formatter can use %(request_prefix)s to include context.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _request_ctx.get()
        if ctx is not None:
            record.request_prefix = ctx.format_prefix() + " "
            # Also set individual fields for structured log systems
            for k, v in ctx.to_log_dict().items():
                setattr(record, f"ctx_{k}", v)
        else:
            record.request_prefix = ""
        return True


def install_request_logging(fmt: Optional[str] = None) -> None:
    """
    Install the request context filter on the root logger and
    update the formatter to include request context.

    Call once at bot startup (main.py).
    """
    root = logging.getLogger()

    # Add filter (idempotent — check if already installed)
    for f in root.filters:
        if isinstance(f, RequestContextFilter):
            return
    root.addFilter(RequestContextFilter())

    # Update all handler formatters to include %(request_prefix)s
    if fmt is None:
        fmt = "%(asctime)s - %(name)s - %(levelname)s - %(request_prefix)s%(message)s"
    formatter = logging.Formatter(fmt)
    for handler in root.handlers:
        handler.setFormatter(formatter)
