from app.infrastructure.agent.events.stream import to_sse
from app.infrastructure.agent.events.types import AgentEvent


def test_to_sse_uses_standard_data_prefix() -> None:
    event = AgentEvent(type="message", payload={"content": "ok"})
    serialized = to_sse(event)

    assert serialized.startswith("event: message\n")
    assert "\ndata: " in serialized
    assert "\nndata: " not in serialized
    assert serialized.endswith("\n\n")
