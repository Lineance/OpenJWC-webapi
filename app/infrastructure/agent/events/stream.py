import json

from .types import AgentEvent

def to_sse(event: AgentEvent) -> str:
    data = event.model_dump(mode="json")
    return f"event: {event.type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
