"""Smoke tests for ReActAgent with LLM integration.

Run with:
  pytest backend/tests/agent/test_smoke.py -v
  pytest backend/tests/agent/test_smoke.py -v -k llm  # only LLM tests
  pytest backend/tests/agent/test_smoke.py -v -k heuristic  # only heuristic tests
"""

import os
import sys
from pathlib import Path
from typing import Any

import pytest

# Ensure backend is in path
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.infrastructure.agent.config import AgentConfig
from app.infrastructure.agent.core.agent import ReActAgent
from app.infrastructure.agent.llm.client import LLMDecisionClient
from app.infrastructure.agent.memory.buffer import ConversationBuffer
from app.infrastructure.agent.tools.protocol import ToolResult
from app.infrastructure.agent.tools.registry import ToolRegistry


class SmokeSearchTool:
    """A minimal tool that simulates retrieval results for smoke testing."""

    name = "search_keyword"
    description = "Return deterministic mock results for smoke tests"

    async def run(self, **kwargs: Any) -> ToolResult:
        query = str(kwargs.get("query", "")).strip() or "(empty query)"
        limit = int(kwargs.get("limit", 3))
        results = [
            {
                "title": f"smoke result #{idx + 1} for {query}",
                "url": f"https://example.com/smoke/{idx + 1}",
                "source": "smoke",
            }
            for idx in range(max(1, min(limit, 5)))
        ]
        return ToolResult(ok=True, content={"results": results, "total": len(results)})


def _load_env() -> None:
    """Load environment variables once per process."""
    from dotenv import load_dotenv

    backend_dir = Path(__file__).resolve().parents[3]
    load_dotenv(backend_dir / "backend" / "agent" / ".env", override=False)
    load_dotenv(backend_dir / "backend" / ".env", override=False)


def _has_api_key() -> bool:
    """Check if any LLM API key is available."""
    import os

    _load_env()
    return bool(os.getenv("DEEPSEEK_API_KEY") or os.getenv("LITELLM_API_KEY"))


def create_agent(mode: str = "llm") -> ReActAgent:
    """Create a ReActAgent for smoke testing."""
    _load_env()

    config = AgentConfig(
        max_steps=3,
        history_window=5,
        tool_timeout_seconds=5.0,
        llm_timeout_seconds=20.0,
        temperature=0.2,
        max_tokens=512,
        llm_model=os.getenv("SEU_WUHUB_AGENT_MODEL", LLMDecisionClient.default_model()),
    )

    registry = ToolRegistry()
    registry.register(SmokeSearchTool())

    decision_client = None
    if mode == "llm":
        decision_client = LLMDecisionClient(
            model=config.llm_model,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            timeout_seconds=config.llm_timeout_seconds,
        )

    agent = ReActAgent(
        tool_registry=registry,
        memory=ConversationBuffer(window_size=config.history_window),
        config=config,
        decision_client=decision_client,
    )
    return agent


@pytest.mark.asyncio
async def test_heuristic_mode_basic():
    """Test agent in heuristic mode (no LLM)."""
    agent = create_agent(mode="heuristic")

    saw_tool_call = False
    saw_done = False

    async for event in agent.run_stream(
        query="请帮我总结本周教学通知", session_id="test-heuristic", history=[]
    ):
        if event.type == "tool_call":
            saw_tool_call = True
        if event.type == "done":
            saw_done = True

    assert saw_tool_call, "Should emit tool_call event"
    assert saw_done, "Should emit done event"


@pytest.mark.asyncio
async def test_llm_mode_basic():
    """Test agent in LLM mode with real API call.

    Requires DEEPSEEK_API_KEY or other LLM API key in .env file.
    Skip this test if no API key is available.
    """
    if not _has_api_key():
        pytest.skip("No LLM API key available (DEEPSEEK_API_KEY or LITELLM_API_KEY)")

    agent = create_agent(mode="llm")

    saw_tool_call = False
    saw_done = False
    saw_llm_planner = False

    async for event in agent.run_stream(
        query="请帮我总结本周教学通知", session_id="test-llm", history=[]
    ):
        planner = event.payload.get("planner") if isinstance(event.payload, dict) else None
        if event.type == "tool_call":
            saw_tool_call = True
            if planner == "llm":
                saw_llm_planner = True
        if event.type == "done":
            saw_done = True

    assert saw_tool_call, "Should emit tool_call event"
    assert saw_done, "Should emit done event"
    assert saw_llm_planner, "Should use LLM planner"


@pytest.mark.asyncio
async def test_heuristic_mode_with_limit():
    """Test heuristic mode with custom max_steps."""
    agent = create_agent(mode="heuristic")
    agent._config.max_steps = 1

    events = []
    async for event in agent.run_stream(
        query="hello", session_id="test-limit", history=[]
    ):
        events.append(event)

    # With max_steps=1, should still produce done event
    event_types = [e.type for e in events]
    assert "done" in event_types, "Should produce done event even with limit=1"
