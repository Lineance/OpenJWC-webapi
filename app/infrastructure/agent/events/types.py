"""Event types for streaming agent execution."""

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

EventType = Literal[
    "thought",
    "tool_call",
    "tool_result",
    "warning",
    "message",
    "done",
    "error",
]


class AgentEvent(BaseModel):
    type: EventType
    step: int = 0
    call_id: str | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: dict[str, Any] = Field(default_factory=dict)
