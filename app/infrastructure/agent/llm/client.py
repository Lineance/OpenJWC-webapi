"""LLM client used to decide tool actions for the agent."""

import json
import logging
import os
from datetime import datetime
from typing import Any

from litellm import acompletion

from app.infrastructure.agent.core.parser import parse_action_detailed

logger = logging.getLogger(__name__)


class LLMDecisionClient:
    def __init__(
        self,
        *,
        model: str,
        temperature: float,
        max_tokens: int,
        timeout_seconds: float,
        planner_history_char_budget: int = 6000,
        final_history_char_budget: int = 9000,
        final_observations_char_budget: int = 12000,
        planner_retry_parse_once: bool = True,
        planner_retry_max_tokens: int = 256,
    ) -> None:
        self._model = model
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._timeout_seconds = timeout_seconds
        self._planner_history_char_budget = planner_history_char_budget
        self._final_history_char_budget = final_history_char_budget
        self._final_observations_char_budget = final_observations_char_budget
        self._planner_retry_parse_once = planner_retry_parse_once
        self._planner_retry_max_tokens = planner_retry_max_tokens

    @staticmethod
    def _trim_history_by_budget(
        history: list[dict[str, Any]], budget_chars: int
    ) -> list[dict[str, Any]]:
        if budget_chars <= 0:
            return history[-6:]

        selected: list[dict[str, Any]] = []
        used = 0
        for item in reversed(history):
            role = str(item.get("role", ""))
            content = str(item.get("content", ""))
            cost = len(role) + len(content)
            if selected and used + cost > budget_chars:
                break
            selected.append({"role": role, "content": content})
            used += cost
        selected.reverse()
        return selected

    @staticmethod
    def _trim_observations_by_budget(
        observations: list[dict[str, Any]], budget_chars: int
    ) -> list[dict[str, Any]]:
        if budget_chars <= 0:
            return observations[-6:]

        selected: list[dict[str, Any]] = []
        used = 0
        for item in reversed(observations):
            cost = len(json.dumps(item, ensure_ascii=False))
            if selected and used + cost > budget_chars:
                break
            selected.append(item)
            used += cost
        selected.reverse()
        return selected

    async def _retry_parse_action_once(
        self,
        *,
        raw_output: str,
        parse_error: str,
        available_tools: list[str],
    ) -> dict[str, Any] | None:
        correction_prompt = (
            "Your previous output could not be parsed as a valid action JSON. "
            "Return JSON only and strictly follow this schema: "
            '{"tool":"tool_name","input":{...}}. '
            "If enough evidence exists, return finish with answer. "
            f"Available tools: {available_tools}. "
            f"Parse error: {parse_error}. "
            f"Previous output: {raw_output}"
        )
        try:
            response = await acompletion(
                model=self._model,
                messages=[{"role": "user", "content": correction_prompt}],
                temperature=0,
                max_tokens=min(self._planner_retry_max_tokens, self._max_tokens),
                timeout=self._timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Planner correction retry failed: %s", exc)
            return None

        choices = getattr(response, "choices", None)
        if not choices:
            return None

        message = getattr(choices[0], "message", None)
        content = getattr(message, "content", "") if message is not None else ""
        if not content:
            return None

        parsed, _ = parse_action_detailed(content, available_tools=available_tools)
        return parsed

    @staticmethod
    def default_model() -> str:
        return os.getenv("OPENJWC_AGENT_MODEL", "openai/gpt-4o-mini")

    async def decide_action(
        self,
        *,
        query: str,
        history: list[dict[str, Any]],
        available_tools: list[str],
    ) -> dict[str, Any] | None:
        if not available_tools:
            return None

        system_prompt = (
            "You are a planning assistant for a campus QA agent. "
            "Choose the next action and produce strict JSON only. "
            'Output format: {"tool":"tool_name","input":{...}}. '
            'When you already have enough evidence, output {"tool":"finish","input":{"answer":"..."}}. '
            "Interpret recency words such as 最近/近期/本周/本月 relative to current_date in payload. "
            "Do not include markdown fences or extra text."
        )

        now = datetime.now()

        user_payload = {
            "query": query,
            "current_date": now.date().isoformat(),
            "current_datetime": now.isoformat(timespec="seconds"),
            "timezone": "Asia/Shanghai",
            "history": self._trim_history_by_budget(
                history, self._planner_history_char_budget
            ),
            "available_tools": available_tools,
            "rules": [
                "Prefer search_keyword for general campus Q&A",
                "Treat search results as evidence, not just counts; inspect title, summary, and content_text when present",
                "Use get_article_detail with news_id when search results are not enough or when the user asks for full content",
                "For recent queries, always attach start_date/end_date to search_keyword input",
                "Use sql_service for explicit filtering/counting/statistics",
                "Use web_url_fetch only when user provides a concrete URL",
                "After search, fetch detail only when the summary is insufficient or the user explicitly asks for source text",
                "Use finish when no further tool call is needed",
            ],
        }

        try:
            response = await acompletion(
                model=self._model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": json.dumps(user_payload, ensure_ascii=False),
                    },
                ],
                temperature=self._temperature,
                max_tokens=self._max_tokens,
                timeout=self._timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "LLM planning failed, fallback to heuristic planner: %s", exc
            )
            return None

        choices = getattr(response, "choices", None)
        if not choices:
            return None

        first_choice = choices[0]
        message = getattr(first_choice, "message", None)
        content = getattr(message, "content", "") if message is not None else ""
        if not content:
            return None

        parsed, parse_error = parse_action_detailed(
            content, available_tools=available_tools
        )
        if not parsed and self._planner_retry_parse_once:
            parsed = await self._retry_parse_action_once(
                raw_output=content,
                parse_error=parse_error or "unknown parse error",
                available_tools=available_tools,
            )
        if not parsed:
            if parse_error:
                logger.warning("Planner output parse failed: %s", parse_error)
            return None

        tool_name = parsed.get("tool")
        if tool_name != "finish" and tool_name not in available_tools:
            return None

        tool_input = parsed.get("input", {})
        if not isinstance(tool_input, dict):
            tool_input = {}

        return {"tool": tool_name, "input": tool_input}

    async def generate_final_answer(
        self,
        *,
        query: str,
        history: list[dict[str, Any]],
        observations: list[dict[str, Any]],
    ) -> str | None:
        """Generate the final user-facing answer from multi-step observations.

        This method calls the underlying LLM with the given ``query``, a truncated
        interaction ``history``, and the collected ``observations`` from previous
        tool calls. The model is instructed to base its answer only on the
        provided observations and to avoid fabricating unsupported details.

        When the available observations do not provide enough grounded
        information to confidently answer the query (that is, when there is
        *insufficient evidence*), the LLM is asked to explicitly state its
        uncertainty and suggest an appropriate follow-up query instead of
        pretending to have a definitive answer. Such uncertainty-aware answers
        are still returned as a normal ``str``.

        Returns:
            str | None: The final natural-language answer from the LLM, or
            ``None`` if the LLM call fails, times out, returns no choices, or
            produces only empty/whitespace content.
        """
        system_prompt = (
            "You are a campus assistant. Generate the final answer based only on observations. "
            "If evidence is insufficient, state uncertainty clearly and suggest a next query. "
            "When the user asks for aggregation/listing, you may use a Markdown table for clarity if helpful. "
            "When observations include an applied time window, mention it briefly. "
            "Answer in Chinese and keep it concise."
        )

        now = datetime.now()

        payload = {
            "query": query,
            "current_date": now.date().isoformat(),
            "current_datetime": now.isoformat(timespec="seconds"),
            "timezone": "Asia/Shanghai",
            "history": self._trim_history_by_budget(
                history, self._final_history_char_budget
            ),
            "observations": self._trim_observations_by_budget(
                observations, self._final_observations_char_budget
            ),
        }

        try:
            response = await acompletion(
                model=self._model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": json.dumps(payload, ensure_ascii=False),
                    },
                ],
                # Use a low temperature for final answers to keep them deterministic and
                # closely grounded in the given observations, avoiding creative deviations.
                temperature=min(self._temperature, 0.3),
                max_tokens=self._max_tokens,
                timeout=self._timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "LLM final answer generation failed, fallback to template: %s", exc
            )
            return None

        choices = getattr(response, "choices", None)
        if not choices:
            return None

        first_choice = choices[0]
        message = getattr(first_choice, "message", None)
        content = getattr(message, "content", "") if message is not None else ""
        answer = str(content).strip()
        return answer or None
