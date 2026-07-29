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

class AnswerCompositionMixin:
    """封装 ReActAgent 的单一职责方法。"""

    @staticmethod
    def _extract_sources(observations: list[dict[str, Any]]) -> list[str]:
        sources: list[str] = []
        seen: set[str] = set()
        for observation in observations:
            result = observation.get("result", {})
            if not isinstance(result, dict):
                continue

            rows = result.get("results")
            if not isinstance(rows, list):
                url = str(result.get("url", "")).strip()
                if url and url not in seen:
                    seen.add(url)
                    sources.append(url)
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                url = str(row.get("url", "")).strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                sources.append(url)
        return sources

    def _compose_answer(self, query: str, tool_name: str, tool_content: dict[str, Any]) -> str:
        results = tool_content.get("results")
        if isinstance(results, list) and results:
            aggregate_query = any(
                token in query for token in ["聚合", "汇总", "列表", "表格", "近期", "最近"]
            )
            if aggregate_query and len(results) >= 2:
                lines = [f"根据你的问题“{query}”，我整理了以下结果："]
                lines.append("| 序号 | 标题 | 来源 | 日期 | 链接 |")
                lines.append("|---|---|---|---|---|")
                for idx, item in enumerate(results[:8], start=1):
                    title = str(item.get("title", "(无标题)")).replace("|", " ").strip()
                    source = str(item.get("source", "未知来源")).replace("|", " ").strip()
                    date_str = str(item.get("published_date", "")).strip() or "未知日期"
                    url = str(item.get("url", "")).strip()
                    lines.append(f"| {idx} | {title} | {source} | {date_str} | {url} |")

                applied_window = tool_content.get("applied_time_window")
                if isinstance(applied_window, dict):
                    start = str(applied_window.get("start_date", "")).strip()
                    end = str(applied_window.get("end_date", "")).strip()
                    if start or end:
                        lines.append(f"\n已按时间窗口过滤：{start} ~ {end}。")
                lines.append("如需，我可以继续按标签、来源或更严格时间范围进一步筛选。")
                return "\n".join(lines)

            lines = [f"根据你的问题\"{query}\"，我找到以下相关信息："]
            for idx, item in enumerate(results[:5], start=1):
                title = item.get("title", "(无标题)")
                source = item.get("source", "未知来源")
                url = item.get("url", "")
                summary = item.get("summary") or item.get("content_text") or ""
                lines.append(f"{idx}. {title} [{source}] {url}")
                if summary:
                    lines.append(f"   {self._truncate_text(summary, 140)}")
            lines.append("如需，我可以继续按时间范围或标签进一步筛选。")
            return "\n".join(lines)

        if tool_name == "web_url_fetch":
            snippet = tool_content.get("snippet") or tool_content.get("content_text")
            if snippet:
                return (
                    f"我已抓取到网页内容片段，可用于事实核查：\n{self._truncate_text(snippet, 500)}"
                )

        if tool_name == "get_article_detail":
            lines = []
            title = tool_content.get("title") or "(无标题)"
            url = tool_content.get("url") or ""
            publish_date = tool_content.get("publish_date") or "未知日期"
            source_site = tool_content.get("source_site") or "未知来源"
            lines.append(f"我已读取文章详情：{title} [{source_site}] {publish_date}")
            if url:
                lines.append(f"来源：{url}")
            body = tool_content.get("content_markdown") or tool_content.get("content_text") or ""
            if body:
                lines.append(self._truncate_text(body, 500))
            return "\n".join(lines)

        return "我已完成查询，但没有找到可用结果。你可以换个关键词或增加筛选条件。"

    async def _build_final_answer(
        self,
        *,
        query: str,
        session_id: str,
        observations: list[dict[str, Any]],
        fallback_tool_name: str,
        fallback_tool_content: dict[str, Any],
    ) -> str:
        if self._decision_client is not None:
            generator = getattr(self._decision_client, "generate_final_answer", None)
            if callable(generator):
                result = generator(
                    query=query,
                    history=self._memory.read(session_id),
                    observations=observations,
                )
                llm_answer = await result if inspect.isawaitable(result) else None
                if llm_answer:
                    return llm_answer

        return self._compose_answer(query, fallback_tool_name, fallback_tool_content)
