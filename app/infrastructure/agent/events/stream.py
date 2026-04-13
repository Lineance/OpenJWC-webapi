"""SSE serializer for agent events."""

import json

from .types import AgentEvent


def to_sse(event: AgentEvent) -> str:
    """Serialize an AgentEvent to SSE text format."""
    data = event.model_dump(mode="json")
    return f"event: {event.type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
