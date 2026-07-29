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

from .agent_mixins.intent_analysis_mixin import IntentAnalysisMixin
from .agent_mixins.tool_selection_mixin import ToolSelectionMixin
from .agent_mixins.observation_mixin import ObservationMixin
from .agent_mixins.answer_composition_mixin import AnswerCompositionMixin
from .agent_mixins.agent_stream_mixin import AgentStreamMixin

class ReActAgent(IntentAnalysisMixin, ToolSelectionMixin, ObservationMixin, AnswerCompositionMixin, AgentStreamMixin):
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
