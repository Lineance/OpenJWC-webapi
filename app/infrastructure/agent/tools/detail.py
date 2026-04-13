"""Article detail tool backed by ArticleRepository."""

import json
from typing import Any

from app.infrastructure.storage.lancedb.repository import ArticleRepository

from .protocol import ToolResult


class DetailTool:
    name = "get_article_detail"
    description = "Fetch a single article by news_id with title, content, tags and attachments."

    def __init__(self, repo: ArticleRepository, content_chars: int = 12000) -> None:
        self._repo = repo
        self._content_chars = content_chars

    def _truncate(self, text: Any) -> str:
        value = str(text or "").strip()
        if len(value) <= self._content_chars:
            return value
        return value[: self._content_chars].rstrip() + "…"

    def _parse_metadata(self, value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    async def run(self, **kwargs: Any) -> ToolResult:
        news_id = str(kwargs.get("news_id", "")).strip()
        if not news_id:
            return ToolResult(ok=False, content={}, error="news_id is required")

        record = self._repo.get(news_id)
        if not record:
            return ToolResult(ok=False, content={}, error=f"article not found: {news_id}")

        content_markdown = record.get("content_markdown") or ""
        content_text = record.get("content_text") or ""

        content_markdown_text = self._truncate(content_markdown)
        content_text_value = self._truncate(content_text)

        payload = {
            "news_id": record.get("news_id", news_id),
            "title": record.get("title"),
            "publish_date": str(record.get("publish_date", ""))[:10],
            "url": record.get("url"),
            "source_site": record.get("source_site"),
            "author": record.get("author"),
            "tags": record.get("tags") or [],
            "attachments": record.get("attachments") or [],
            "metadata": self._parse_metadata(record.get("metadata")),
            "content_markdown": content_markdown_text,
            "content_text": content_text_value,
            "content_truncated": len(str(content_markdown)) > self._content_chars
            or len(str(content_text)) > self._content_chars,
        }

        return ToolResult(ok=True, content=payload)
