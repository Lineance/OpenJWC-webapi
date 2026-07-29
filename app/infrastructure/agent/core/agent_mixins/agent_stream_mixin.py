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

class AgentStreamMixin:
    """封装 ReActAgent 的单一职责方法。"""

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
