from __future__ import annotations

from app.infrastructure.agent.core.agent import (
    AgentConfig,
    AgentEvent,
    Any,
    AsyncIterator,
    ConversationBuffer,
    DecisionClient,
    Protocol,
    ToolRegistry,
    asyncio,
    datetime,
    inspect,
    json,
    re,
    timedelta,
)

class ObservationMixin:
    """封装 ReActAgent 的单一职责方法。"""

    @staticmethod
    def _truncate_text(value: Any, limit: int = 240) -> str:
        text = str(value or "").strip()
        if len(text) <= limit:
            return text
        return text[:limit].rstrip() + "…"

    @classmethod
    def _compact_rows(cls, rows: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
        compact: list[dict[str, Any]] = []
        for row in rows[:limit]:
            item: dict[str, Any] = {}
            for key in ("id", "title", "url", "source", "published_date", "score"):
                value = row.get(key)
                if value not in (None, ""):
                    item[key] = value
            summary = row.get("summary") or row.get("content_text")
            if summary:
                item["summary"] = cls._truncate_text(summary, 220)
            content_text = row.get("content_text")
            if content_text:
                item["content_preview"] = cls._truncate_text(content_text, 360)
            compact.append(item)
        return compact

    @classmethod
    def _observation_text(cls, tool_name: str, tool_result: dict[str, Any]) -> str:
        total = tool_result.get("total")
        results = tool_result.get("results")
        if isinstance(results, list):
            payload: dict[str, Any] = {"tool": tool_name}
            if isinstance(total, int):
                payload["total"] = total
            compact_rows = cls._compact_rows([row for row in results if isinstance(row, dict)])
            if compact_rows:
                payload["results"] = compact_rows
            if isinstance(tool_result.get("query"), str) and tool_result.get("query"):
                payload["query"] = tool_result["query"]
            if len(results) > len(compact_rows):
                payload["more_results"] = len(results) - len(compact_rows)
            return json.dumps(payload, ensure_ascii=False)

        if tool_name == "get_article_detail":
            payload = {"tool": tool_name}
            for key in (
                "news_id",
                "title",
                "publish_date",
                "url",
                "source_site",
                "author",
                "tags",
                "attachments",
                "content_truncated",
            ):
                value = tool_result.get(key)
                if value not in (None, "", []):
                    payload[key] = value
            for key in ("content_markdown", "content_text"):
                value = tool_result.get(key)
                if value:
                    payload[key] = cls._truncate_text(value, 900)
            return json.dumps(payload, ensure_ascii=False)

        if tool_name == "web_url_fetch":
            payload = {"tool": tool_name}
            for key in ("url", "status", "snippet", "content_text"):
                value = tool_result.get(key)
                if value:
                    payload[key] = cls._truncate_text(
                        value, 500 if key in {"snippet", "content_text"} else 240
                    )
            return json.dumps(payload, ensure_ascii=False)

        if tool_result:
            payload = {"tool": tool_name}
            for key, value in tool_result.items():
                if value not in (None, ""):
                    payload[key] = value
            return json.dumps(payload, ensure_ascii=False)

        return f"{tool_name} completed"
