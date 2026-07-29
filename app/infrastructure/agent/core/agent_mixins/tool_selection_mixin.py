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

class ToolSelectionMixin:
    """封装 ReActAgent 的单一职责方法。"""

    @staticmethod
    def _derive_followup_query(original_query: str, detail: dict[str, Any]) -> str | None:
        if not detail:
            return None

        q = original_query.lower()
        title = str(detail.get("title", "")).strip()
        attachments = detail.get("attachments")
        content_truncated = bool(detail.get("content_truncated"))
        url = str(detail.get("url", "")).strip()
        publish_date = str(detail.get("publish_date", "")).strip()

        if (
            any(token in q for token in ["附件", "下载", "文件"])
            and isinstance(attachments, list)
            and attachments
        ):
            return f"{title} 附件 下载 链接"

        if (
            any(token in q for token in ["全文", "原文", "细节", "完整内容"])
            and content_truncated
            and url
        ):
            return url

        if (
            any(token in q for token in ["日期", "时间", "什么时候", "deadline"])
            and not publish_date
        ):
            return f"{title} 发布时间"

        return None

    async def _pick_tool(
        self, session_id: str, query: str
    ) -> tuple[str, dict[str, Any], str, dict[str, str] | None]:
        available_tools = self._tools.list_tools()
        if self._decision_client is not None:
            action = await self._decision_client.decide_action(
                query=query,
                history=self._memory.read(session_id),
                available_tools=available_tools,
            )
            if action:
                tool_name = str(action.get("tool", "")).strip()
                params = action.get("input", {})
                if isinstance(params, dict) and (
                    tool_name in available_tools or tool_name == "finish"
                ):
                    if tool_name in available_tools:
                        params, _ = self._apply_recent_time_window(
                            tool_name=tool_name,
                            tool_params=params,
                            query=query,
                        )
                    return tool_name, params, "llm", None

            tool_name, params, route = self._pick_tool_fallback(query)
            return tool_name, params, "heuristic", route

        tool_name, params, route = self._pick_tool_fallback(query)
        return tool_name, params, "heuristic", route
