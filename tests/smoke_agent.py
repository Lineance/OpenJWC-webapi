"""Smoke test script for agent event loop and planning behavior.

Usage examples:
  python tests/smoke_agent.py --mode heuristic
  python tests/smoke_agent.py --mode llm --require-llm
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a smoke test for ReActAgent")
    parser.add_argument("--query", default="请帮我总结本周教学通知")
    parser.add_argument("--session-id", default="smoke-session")
    parser.add_argument("--mode", choices=["llm", "heuristic"], default="llm")
    parser.add_argument("--require-llm", action="store_true")
    parser.add_argument("--max-steps", type=int, default=3)
    parser.add_argument("--history-window", type=int, default=5)
    parser.add_argument("--tool-timeout", type=float, default=5.0)
    parser.add_argument("--llm-timeout", type=float, default=20.0)
    parser.add_argument("--temperature", type=float, default=0.2)
    parser.add_argument("--max-tokens", type=int, default=512)
    return parser.parse_args()


def load_env_files() -> None:
    load_dotenv(ROOT / "app" / "infrastructure" / "agent" / ".env", override=False)
    load_dotenv(ROOT / ".env", override=False)


def build_decision_client(args: argparse.Namespace) -> LLMDecisionClient | None:
    if args.mode == "heuristic":
        return None

    model = os.getenv("OPENJWC_AGENT_MODEL", LLMDecisionClient.default_model())
    return LLMDecisionClient(
        model=model,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        timeout_seconds=args.llm_timeout,
    )


async def run_smoke(args: argparse.Namespace) -> int:
    load_env_files()

    config = AgentConfig(
        max_steps=args.max_steps,
        history_window=args.history_window,
        tool_timeout_seconds=args.tool_timeout,
        llm_timeout_seconds=args.llm_timeout,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        llm_model=os.getenv("OPENJWC_AGENT_MODEL", LLMDecisionClient.default_model()),
    )

    registry = ToolRegistry()
    registry.register(SmokeSearchTool())

    decision_client = build_decision_client(args)
    agent = ReActAgent(
        tool_registry=registry,
        memory=ConversationBuffer(window_size=config.history_window),
        config=config,
        decision_client=decision_client,
    )

    print("== Agent Smoke Test ==")
    print(f"mode={args.mode}, model={config.llm_model}, session_id={args.session_id}")
    print(f"query={args.query}")

    saw_tool_call = False
    saw_done = False
    saw_llm_planner = False

    async for event in agent.run_stream(
        query=args.query, session_id=args.session_id, history=[]
    ):
        planner = (
            event.payload.get("planner") if isinstance(event.payload, dict) else None
        )
        if event.type == "tool_call":
            saw_tool_call = True
            if planner == "llm":
                saw_llm_planner = True

        if event.type == "done":
            saw_done = True

        print(f"[{event.type}] step={event.step} payload={event.payload}")

    failures: list[str] = []
    if not saw_tool_call:
        failures.append("missing tool_call event")
    if not saw_done:
        failures.append("missing done event")
    if args.require_llm and not saw_llm_planner:
        failures.append("LLM planner was not used (planner != llm)")

    if failures:
        print("\nSMOKE FAILED")
        for item in failures:
            print(f"- {item}")
        return 1

    print("\nSMOKE PASSED")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())


