"""ReAct-style agent loop with tool orchestration."""

import asyncio
import inspect
import json
import re
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any, Protocol

from app.infrastructure.agent.config import AgentConfig
from app.infrastructure.agent.events.types import AgentEvent
from app.infrastructure.agent.memory.buffer import ConversationBuffer
from app.infrastructure.agent.tools.registry import ToolRegistry


class DecisionClient(Protocol):
    async def decide_action(
        self,
        *,
        query: str,
        history: list[dict[str, Any]],
        available_tools: list[str],
    ) -> dict[str, Any] | None: ...


class ReActAgent:
    def __init__(
        self,
        *,
        tool_registry: ToolRegistry,
        memory: ConversationBuffer,
        config: AgentConfig,
        decision_client: DecisionClient | None = None,
    ) -> None:
        self._tools = tool_registry
        self._memory = memory
        self._config = config
        self._decision_client = decision_client

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

        # Only treat as link verification when there is an actual URL,
        # or verification terms appear together with explicit link cues.
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

        # 动态窗口：近X天/周/月
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
                # 月按 30 天近似
                delta = timedelta(days=value * 30)
                label = f"近{value}月"

            start_date = today - delta + timedelta(days=1)
            return {
                "label": label,
                "start_date": f"{start_date.isoformat()}T00:00:00",
                "end_date": f"{today.isoformat()}T23:59:59",
            }

        # 本周：周一到今天
        if "本周" in query:
            start_date = today - timedelta(days=today.weekday())
            return {
                "label": "本周",
                "start_date": f"{start_date.isoformat()}T00:00:00",
                "end_date": f"{today.isoformat()}T23:59:59",
            }

        # 本月：月初到今天
        if "本月" in query:
            start_date = today.replace(day=1)
            return {
                "label": "本月",
                "start_date": f"{start_date.isoformat()}T00:00:00",
                "end_date": f"{today.isoformat()}T23:59:59",
            }

        # 近期/最近/最新：默认近 30 天
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

    async def run_stream(
        self,
        *,
        query: str,
        session_id: str,
        history: list[dict[str, Any]] | None = None,
    ) -> AsyncIterator[AgentEvent]:
        step = 1
        history = history or []
        original_query = query
        active_query = query
        last_success_tool = ""
        last_success_content: dict[str, Any] = {}
        observations: list[dict[str, Any]] = []
        auto_followup_used = False

        for item in history[-self._config.history_window :]:
            role = str(item.get("role", "user"))
            content = str(item.get("content", ""))
            self._memory.append(session_id, role=role, content=content)

        self._memory.append(session_id, role="user", content=original_query)

        while step <= self._config.max_steps:
            yield AgentEvent(
                type="thought",
                step=step,
                payload={
                    "message": f"正在进行第 {step} 轮分析与决策",
                    "available_tools": self._tools.list_tools(),
                },
            )

            tool_name, tool_params, planner, route_info = await self._pick_tool(
                session_id, active_query
            )
            call_id = f"step-{step}-{tool_name}"
            yield AgentEvent(
                type="tool_call",
                step=step,
                call_id=call_id,
                payload={"tool": tool_name, "input": tool_params, "planner": planner},
            )

            if planner == "heuristic":
                route_message = ""
                if route_info:
                    route_message = (
                        f"；回退意图={route_info.get('intent', 'unknown')}，"
                        f"原因={route_info.get('reason', 'n/a')}"
                    )
                yield AgentEvent(
                    type="warning",
                    step=step,
                    call_id=call_id,
                    payload={
                        "message": f"LLM 规划不可用，已回退到规则策略{route_message}",
                        "recoverable": True,
                        "route": route_info or {},
                    },
                )

            if tool_name == "finish":
                answer = str(tool_params.get("answer", "")).strip()
                if not answer:
                    if last_success_tool:
                        observations.append(
                            {
                                "step": step,
                                "tool": "finish",
                                "input": tool_params,
                                "result": {},
                            }
                        )
                        answer = await self._build_final_answer(
                            query=original_query,
                            session_id=session_id,
                            observations=observations,
                            fallback_tool_name=last_success_tool,
                            fallback_tool_content=last_success_content,
                        )
                    else:
                        answer = "我已完成分析，但缺少可输出的最终结论。"

                self._memory.append(session_id, role="assistant", content=answer)
                yield AgentEvent(type="message", step=step, payload={"content": answer})
                yield AgentEvent(
                    type="done",
                    step=step,
                    payload={"reason": "completed", "sources": self._extract_sources(observations)},
                )
                return

            try:
                tool_result = await asyncio.wait_for(
                    self._tools.execute(tool_name, tool_params),
                    timeout=self._config.tool_timeout_seconds,
                )
            except TimeoutError:
                yield AgentEvent(
                    type="error",
                    step=step,
                    call_id=call_id,
                    payload={"message": "工具调用超时", "tool": tool_name},
                )
                yield AgentEvent(
                    type="done",
                    step=step,
                    payload={
                        "reason": "tool_timeout",
                        "sources": self._extract_sources(observations),
                    },
                )
                return

            if not tool_result.ok:
                yield AgentEvent(
                    type="tool_result",
                    step=step,
                    call_id=call_id,
                    payload={"tool": tool_name, "ok": False, "error": tool_result.error},
                )
                yield AgentEvent(
                    type="warning",
                    step=step,
                    call_id=call_id,
                    payload={
                        "message": "工具执行失败，已进入降级回答路径",
                        "recoverable": True,
                    },
                )
                fallback = "工具执行失败，我会基于现有信息继续回答。请尝试更具体的问题。"
                self._memory.append(session_id, role="assistant", content=fallback)
                yield AgentEvent(type="message", step=step, payload={"content": fallback})
                yield AgentEvent(
                    type="done",
                    step=step,
                    payload={
                        "reason": "tool_error",
                        "sources": self._extract_sources(observations),
                    },
                )
                return

            yield AgentEvent(
                type="tool_result",
                step=step,
                call_id=call_id,
                payload={"tool": tool_name, "ok": True, "result": tool_result.content},
            )

            last_success_tool = tool_name
            last_success_content = tool_result.content
            observations.append(
                {
                    "step": step,
                    "tool": tool_name,
                    "input": tool_params,
                    "result": tool_result.content,
                }
            )
            self._memory.append(
                session_id,
                role="tool",
                content=self._truncate_text(
                    self._observation_text(tool_name, tool_result.content),
                    self._config.observation_memory_char_budget,
                ),
            )

            if (
                tool_name == "get_article_detail"
                and not auto_followup_used
                and step < self._config.max_steps
            ):
                followup_query = self._derive_followup_query(active_query, tool_result.content)
                if followup_query:
                    auto_followup_used = True
                    active_query = followup_query
                    self._memory.append(
                        session_id,
                        role="user",
                        content=f"[auto_followup] {followup_query}",
                    )
                    yield AgentEvent(
                        type="warning",
                        step=step,
                        call_id=call_id,
                        payload={
                            "message": "已触发详情二跳推理模板，将继续补充检索证据",
                            "recoverable": True,
                            "followup_query": followup_query,
                        },
                    )

            step += 1

        if last_success_tool:
            answer = await self._build_final_answer(
                query=original_query,
                session_id=session_id,
                observations=observations,
                fallback_tool_name=last_success_tool,
                fallback_tool_content=last_success_content,
            )
        else:
            answer = "达到最大推理步数，仍未生成最终答案。请缩小问题范围后重试。"

        self._memory.append(session_id, role="assistant", content=answer)
        yield AgentEvent(type="message", step=self._config.max_steps, payload={"content": answer})
        yield AgentEvent(
            type="done",
            step=self._config.max_steps,
            payload={"reason": "max_steps", "sources": self._extract_sources(observations)},
        )
