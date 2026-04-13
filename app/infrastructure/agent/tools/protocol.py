"""Protocol definitions for agent tools."""

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class ToolResult:
    ok: bool
    content: dict[str, Any]
    error: str | None = None


class Tool(Protocol):
    name: str
    description: str

    async def run(self, **kwargs: Any) -> ToolResult: ...
