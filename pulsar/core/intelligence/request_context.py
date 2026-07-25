"""
Layer 5: Request ID Threading - End-to-end request tracing via contextvars.

Automatically propagates request_id through all components without manual passing.
Enables quick debugging: grep request_id through logs instead of manual correlation.

Architecture:
  Agent.think() sets request_id in context
  ├─ All logs automatically include request_id
  ├─ Tool calls inherit request_id
  ├─ LLM calls include request_id
  └─ Nested calls propagate automatically

Benefits:
  - Before: Manual ID passing through 20+ function signatures
  - After: Automatic via contextvars, zero changes to function signatures
  - Debugging: 30 minutes of log correlation → 2 minutes with grep
"""

import contextvars
import uuid
import logging
from typing import Optional, Generator
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# ContextVar for request ID (thread-safe, async-safe)
_request_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "request_id", default=None
)

# ContextVar for conversation ID (for filtering logs by conversation)
_conversation_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "conversation_id", default=None
)

# ContextVar for stage (scout, explorer, etc.)
_stage: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "stage", default=None
)


def generate_request_id() -> str:
    """Generate a unique request ID."""
    return str(uuid.uuid4())


def set_request_id(request_id: str) -> None:
    """
    Set the request ID for the current context.

    Args:
        request_id: Unique request identifier
    """
    _request_id.set(request_id)
    logger.debug(f"Request context initialized: {request_id[:8]}...")


def get_request_id() -> Optional[str]:
    """Get the current request ID, or None if not set."""
    return _request_id.get()


def set_conversation_id(conversation_id: str) -> None:
    """Set the conversation ID for the current context."""
    _conversation_id.set(conversation_id)


def get_conversation_id() -> Optional[str]:
    """Get the current conversation ID, or None if not set."""
    return _conversation_id.get()


def set_stage(stage: str) -> None:
    """Set the current journey stage."""
    _stage.set(stage)


def get_stage() -> Optional[str]:
    """Get the current journey stage, or None if not set."""
    return _stage.get()


def clear_request_context() -> None:
    """Clear all request context variables."""
    _request_id.set(None)
    _conversation_id.set(None)
    _stage.set(None)


@contextmanager
def request_context(
    request_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    stage: Optional[str] = None,
) -> Generator:
    """
    Context manager for setting request context.

    Usage:
        with request_context(
            request_id="abc-123",
            conversation_id="conv-456",
            stage="scout"
        ):
            # All code here has context set
            agent.think("What is the distribution?")

    Args:
        request_id: Request ID (generated if not provided)
        conversation_id: Conversation ID
        stage: Journey stage

    Yields:
        The request_id that was set
    """
    if request_id is None:
        request_id = generate_request_id()

    # Save previous values
    prev_request_id = _request_id.get()
    prev_conversation_id = _conversation_id.get()
    prev_stage = _stage.get()

    try:
        # Set new values
        set_request_id(request_id)
        if conversation_id:
            set_conversation_id(conversation_id)
        if stage:
            set_stage(stage)

        yield request_id

    finally:
        # Restore previous values
        _request_id.set(prev_request_id)
        _conversation_id.set(prev_conversation_id)
        _stage.set(prev_stage)


class RequestContextFilter(logging.Filter):
    """
    Logging filter that adds request context to all log records.

    This is automatically added to all handlers by logging_config.
    Enables filtering logs by request_id, conversation_id, or stage.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Add request context to log record."""
        record.request_id = get_request_id() or "-"
        record.conversation_id = get_conversation_id() or "-"
        record.stage = get_stage() or "-"
        return True


def get_context_summary() -> dict:
    """Get a summary of the current request context."""
    return {
        "request_id": get_request_id(),
        "conversation_id": get_conversation_id(),
        "stage": get_stage(),
    }
