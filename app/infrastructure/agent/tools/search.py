"""Search tool wrapping RetrievalEngine."""

from typing import Any

from backend.retrieval.engine import RetrievalEngine

from .protocol import ToolResult


class SearchTool:
    name = "search_keyword"
    description = (
        "Search relevant campus articles by query and filters, returning summary and content text."
    )

    def __init__(
        self,
        engine: RetrievalEngine,
        default_limit: int = 5,
        summary_chars: int = 220,
        content_chars: int = 4000,
    ) -> None:
        self._engine = engine
        self._default_limit = default_limit
        self._summary_chars = summary_chars
        self._content_chars = content_chars

    def _truncate(self, text: Any, limit: int) -> str:
        value = str(text or "").strip()
        if len(value) <= limit:
            return value
        return value[:limit].rstrip() + "…"

    async def run(self, **kwargs: Any) -> ToolResult:
        query = str(kwargs.get("query", "") or kwargs.get("keyword", "")).strip()
        if not query:
            return ToolResult(ok=False, content={}, error="query is required")

        limit = int(kwargs.get("limit", self._default_limit))
        source = kwargs.get("source") or kwargs.get("category")
        tags = kwargs.get("tags")
        if tags and isinstance(tags, str):
            tags = [item.strip() for item in tags.split(",") if item.strip()]

        result = self._engine.search(
            query=query,
            search_type="hybrid",
            limit=limit,
            source_site=source,
            tags=tags,
            start_date=kwargs.get("start_date"),
            end_date=kwargs.get("end_date"),
        )

        items = [
            {
                "id": row.get("news_id"),
                "title": row.get("title"),
                "url": row.get("url"),
                "summary": self._truncate(row.get("content_text", ""), self._summary_chars),
                "content_text": self._truncate(row.get("content_text", ""), self._content_chars),
                "source": row.get("source_site"),
                "published_date": str(row.get("publish_date", ""))[:10],
                "score": row.get("_score", 0),
                "content_truncated": len(str(row.get("content_text", ""))) > self._content_chars,
            }
            for row in result.get("results", [])
        ]

        time_window = None
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        if start_date or end_date:
            time_window = {
                "start_date": str(start_date or ""),
                "end_date": str(end_date or ""),
            }

        return ToolResult(
            ok=True,
            content={
                "query": query,
                "results": items,
                "total": len(items),
                "applied_time_window": time_window,
            },
        )
