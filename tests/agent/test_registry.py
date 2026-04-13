import pytest
from app.infrastructure.agent.tools.registry import ToolRegistry


class DummyTool:
    name = "dummy"
    description = "dummy tool"

    async def run(self, **kwargs):
        return type("Result", (), {"ok": True, "content": {"echo": kwargs}, "error": None})()


@pytest.mark.asyncio
async def test_registry_executes_registered_tool() -> None:
    registry = ToolRegistry()
    registry.register(DummyTool())

    result = await registry.execute("dummy", {"a": 1})
    assert result.ok is True
    assert result.content["echo"]["a"] == 1


@pytest.mark.asyncio
async def test_registry_handles_unknown_tool() -> None:
    registry = ToolRegistry()
    result = await registry.execute("missing", {})
    assert result.ok is False
    assert "unknown tool" in (result.error or "")
