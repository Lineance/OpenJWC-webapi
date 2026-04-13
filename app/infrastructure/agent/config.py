"""Agent runtime configuration."""

import os

from pydantic import BaseModel, Field


def _default_llm_model() -> str:
    return os.getenv("OPENJWC_AGENT_MODEL", "openai/gpt-4o-mini")


class AgentConfig(BaseModel):
    llm_model: str = Field(default_factory=_default_llm_model)
    llm_timeout_seconds: float = Field(
        default_factory=lambda: float(os.getenv("OPENJWC_LLM_TIMEOUT_SECONDS", "45")),
        gt=0,
    )
    max_steps: int = Field(default=6, ge=1, le=10)
    history_window: int = Field(default=8, ge=1, le=20)
    tool_timeout_seconds: float = Field(default=8.0, gt=0)
    fetch_timeout_seconds: float = Field(default=10.0, gt=0)
    fetch_retry_count: int = Field(default=1, ge=0, le=3)
    temperature: float = Field(default=0.2, ge=0.0, le=1.0)
    max_tokens: int = Field(default=1024, ge=128, le=4096)
    allowed_fetch_domains: list[str] = Field(default_factory=lambda: ["seu.edu.cn"])
    enable_intent_routing: bool = Field(default=True)
    default_search_limit: int = Field(default=5, ge=1, le=20)
    default_stats_limit: int = Field(default=10, ge=1, le=50)
    observation_memory_char_budget: int = Field(default=2400, ge=400, le=12000)
    planner_history_char_budget: int = Field(default=6000, ge=1000, le=20000)
    final_history_char_budget: int = Field(default=9000, ge=1000, le=30000)
    final_observations_char_budget: int = Field(default=12000, ge=1000, le=40000)
    planner_retry_parse_once: bool = Field(default=True)
    planner_retry_max_tokens: int = Field(default=256, ge=64, le=1024)
