"""In-process registry for agent tools."""

from typing import Any

from .protocol import Tool, ToolResult


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        self._tools[tool.name] = tool

    def list_tools(self) -> list[str]:
        return sorted(self._tools.keys())

    async def execute(self, name: str, params: dict[str, Any]) -> ToolResult:
        tool = self._tools.get(name)
        if tool is None:
            return ToolResult(ok=False, content={}, error=f"unknown tool: {name}")

        try:
            return await tool.run(**params)
        except Exception as exc:  # noqa: BLE001
            return ToolResult(ok=False, content={}, error=str(exc))
