"""
core/structured_log.py - Structured logging for observability.

Every major operation logs: operation_id, message_id, topic_id,
bytes_processed, retry_count. Errors NEVER fail silently.

Usage:
    from core.structured_log import op_log, OperationLogger

    # Quick structured log
    op_log("download", message_id=123, bytes_processed=1024)

    # Context manager for operation tracking
    with OperationLogger("upload", user_id=42, message_id=5) as ol:
        ol.update(bytes_processed=512)
        ol.retry()
"""

import uuid
import time
import logging
from typing import Optional, Any, Dict
from contextlib import contextmanager

logger = logging.getLogger("core.ops")


def op_log(
    operation: str,
    *,
    operation_id: Optional[str] = None,
    message_id: Optional[int] = None,
    topic_id: Optional[int] = None,
    user_id: Optional[int] = None,
    bytes_processed: int = 0,
    retry_count: int = 0,
    level: int = logging.INFO,
    error: Optional[str] = None,
    **extra: Any
) -> None:
    """
    Emit a structured log entry for a major operation.
    
    All fields are optional except `operation`.
    Errors are logged at ERROR level automatically.
    """
    fields: Dict[str, Any] = {"op": operation}
    
    if operation_id:
        fields["op_id"] = operation_id
    if message_id is not None:
        fields["msg_id"] = message_id
    if topic_id is not None:
        fields["topic_id"] = topic_id
    if user_id is not None:
        fields["user_id"] = user_id
    if bytes_processed:
        fields["bytes"] = bytes_processed
    if retry_count:
        fields["retries"] = retry_count
    if error:
        fields["error"] = error
        level = logging.ERROR
    
    fields.update(extra)
    
    parts = [f"{k}={v}" for k, v in fields.items()]
    logger.log(level, " | ".join(parts))


class OperationLogger:
    """
    Context manager for tracking an operation's lifecycle.
    
    Automatically generates operation_id and logs start/end/error.
    """
    
    def __init__(
        self,
        operation: str,
        *,
        message_id: Optional[int] = None,
        topic_id: Optional[int] = None,
        user_id: Optional[int] = None,
        **extra: Any
    ):
        self.operation = operation
        self.operation_id = uuid.uuid4().hex[:12]
        self.message_id = message_id
        self.topic_id = topic_id
        self.user_id = user_id
        self.extra = extra
        self.bytes_processed = 0
        self.retry_count = 0
        self._start_time = 0.0
    
    def __enter__(self):
        self._start_time = time.monotonic()
        op_log(
            self.operation,
            operation_id=self.operation_id,
            message_id=self.message_id,
            topic_id=self.topic_id,
            user_id=self.user_id,
            status="started",
            **self.extra
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.monotonic() - self._start_time
        if exc_type is not None:
            # Errors NEVER fail silently
            op_log(
                self.operation,
                operation_id=self.operation_id,
                message_id=self.message_id,
                topic_id=self.topic_id,
                user_id=self.user_id,
                bytes_processed=self.bytes_processed,
                retry_count=self.retry_count,
                error=f"{exc_type.__name__}: {exc_val}",
                elapsed_s=round(elapsed, 2),
                **self.extra
            )
        else:
            op_log(
                self.operation,
                operation_id=self.operation_id,
                message_id=self.message_id,
                topic_id=self.topic_id,
                user_id=self.user_id,
                bytes_processed=self.bytes_processed,
                retry_count=self.retry_count,
                status="completed",
                elapsed_s=round(elapsed, 2),
                **self.extra
            )
        return False  # Don't suppress exceptions
    
    def update(self, bytes_processed: int = 0, **extra: Any) -> None:
        """Update operation metrics."""
        self.bytes_processed = bytes_processed
        self.extra.update(extra)
    
    def retry(self) -> None:
        """Increment retry counter."""
        self.retry_count += 1
