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

class IntentAnalysisMixin:
    """封装 ReActAgent 的单一职责方法。"""

    @staticmethod
    def _detect_news_id(query: str) -> str | None:
        pattern = re.compile(r"\b\d{6,8}[_-][A-Za-z0-9_-]+\b")
        match = pattern.search(query)
        return match.group(0) if match else None

    def _classify_intent(self, query: str) -> dict[str, str]:
        q = query.strip()
        lower = q.lower()

        has_url = re.search(r"https?://[^\s]+", q) is not None
        has_link_cue = any(token in lower for token in ["链接", "网址", "网页", "url", "http"])
        has_verify_cue = any(token in lower for token in ["核验", "真伪"])

        if has_url or (has_link_cue and has_verify_cue):
            return {
                "intent": "link_verification",
                "reason": "query contains URL or link-check cues",
            }

        if any(token in lower for token in ["统计", "数量", "多少", "count", "筛选", "占比"]):
            return {"intent": "statistics", "reason": "query asks for counts or structured filters"}

        if self._detect_news_id(q) or any(
            token in lower for token in ["详情", "全文", "原文", "附件"]
        ):
            return {
                "intent": "detail",
                "reason": "query asks for full text/detail or has a news_id",
            }

        return {"intent": "fuzzy_qa", "reason": "default fallback for general campus QA"}

    @staticmethod
    def _infer_recent_time_window(query: str, now: datetime | None = None) -> dict[str, str] | None:
        text = query.strip().lower()
        now_dt = now or datetime.now()
        today = now_dt.date()

        explicit = re.search(r"近\s*(\d{1,3})\s*(天|日|周|星期|个月|月)", text)
        if explicit:
            value = max(1, int(explicit.group(1)))
            unit = explicit.group(2)
            if unit in {"天", "日"}:
                delta = timedelta(days=value)
                label = f"近{value}天"
            elif unit in {"周", "星期"}:
                delta = timedelta(days=value * 7)
                label = f"近{value}周"
            else:

                delta = timedelta(days=value * 30)
                label = f"近{value}月"

            start_date = today - delta + timedelta(days=1)
            return {
                "label": label,
                "start_date": f"{start_date.isoformat()}T00:00:00",
                "end_date": f"{today.isoformat()}T23:59:59",
            }

        if "本周" in query:
            start_date = today - timedelta(days=today.weekday())
            return {
                "label": "本周",
                "start_date": f"{start_date.isoformat()}T00:00:00",
                "end_date": f"{today.isoformat()}T23:59:59",
            }

        if "本月" in query:
            start_date = today.replace(day=1)
            return {
                "label": "本月",
                "start_date": f"{start_date.isoformat()}T00:00:00",
                "end_date": f"{today.isoformat()}T23:59:59",
            }

        if any(token in text for token in ["近期", "最近", "最新", "recent"]):
            start_date = today - timedelta(days=29)
            return {
                "label": "近期(近30天)",
                "start_date": f"{start_date.isoformat()}T00:00:00",
                "end_date": f"{today.isoformat()}T23:59:59",
            }

        return None

    @classmethod
    def _apply_recent_time_window(
        cls,
        *,
        tool_name: str,
        tool_params: dict[str, Any],
        query: str,
    ) -> tuple[dict[str, Any], dict[str, str] | None]:
        if tool_name != "search_keyword":
            return tool_params, None

        if tool_params.get("start_date") or tool_params.get("end_date"):
            return tool_params, None

        window = cls._infer_recent_time_window(query)
        if not window:
            return tool_params, None

        patched = dict(tool_params)
        patched["start_date"] = window["start_date"]
        patched["end_date"] = window["end_date"]
        return patched, window

    def _pick_tool_fallback(self, query: str) -> tuple[str, dict[str, Any], dict[str, str]]:
        intent_info = (
            self._classify_intent(query)
            if self._config.enable_intent_routing
            else {
                "intent": "fuzzy_qa",
                "reason": "intent routing disabled",
            }
        )
        intent = intent_info["intent"]

        url_match = re.search(r"https?://[^\s]+", query)
        news_id = self._detect_news_id(query)

        if intent == "link_verification" and url_match:
            return "web_url_fetch", {"url": url_match.group(0)}, intent_info

        if intent == "detail" and news_id:
            return "get_article_detail", {"news_id": news_id}, intent_info

        if intent == "statistics":
            conditions: dict[str, Any] = {}
            if "教务" in query:
                conditions["source_site"] = "jwc"
            return (
                "sql_service",
                {"conditions": conditions, "limit": self._config.default_stats_limit},
                intent_info,
            )

        search_params = {"query": query, "limit": self._config.default_search_limit}
        search_params, recent_window = self._apply_recent_time_window(
            tool_name="search_keyword",
            tool_params=search_params,
            query=query,
        )
        if recent_window:
            intent_info = {**intent_info, "time_window": recent_window.get("label", "近期")}

        return ("search_keyword", search_params, intent_info)
