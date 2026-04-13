"""Structured query tool with SQLGuard protection."""

from typing import Any

from app.infrastructure.storage.lancedb.guard import SQLGuard
from app.infrastructure.storage.lancedb.repository import ArticleRepository

from .protocol import ToolResult


class SQLTool:
    name = "sql_service"
    description = "Run safe structured filters against article data."

    def __init__(self, repo: ArticleRepository, guard: SQLGuard) -> None:
        self._repo = repo
        self._guard = guard

    async def run(self, **kwargs: Any) -> ToolResult:
        conditions = kwargs.get("conditions") or {}
        limit = int(kwargs.get("limit", 10))

        if not isinstance(conditions, dict):
            return ToolResult(ok=False, content={}, error="conditions must be an object")

        try:
            where = self._guard.build_safe_where(conditions) if conditions else None
        except ValueError as exc:
            return ToolResult(ok=False, content={}, error=str(exc))

        try:
            if where:
                rows = self._repo.table.search().where(where).limit(limit).to_list()
            else:
                rows = self._repo.find_all(limit=limit)
        except Exception as exc:  # noqa: BLE001
            return ToolResult(ok=False, content={}, error=str(exc))

        results = [
            {
                "id": row.get("news_id"),
                "title": row.get("title"),
                "url": row.get("url"),
                "source": row.get("source_site"),
                "published_date": str(row.get("publish_date", ""))[:10],
            }
            for row in rows
        ]

        return ToolResult(ok=True, content={"results": results, "total": len(results)})
