"""Thin chat orchestration adapter for preserving API contract."""

from collections.abc import AsyncIterator

from app.infrastructure.agent.chat import ai_service as agent_chat_service
from app.models.schemas import ChatRequest


async def get_chat_text(request: ChatRequest, use_rag: bool = True) -> str:
    response = await agent_chat_service.get_ai_response(request, use_rag=use_rag)
    return str(response.choices[0].message.content or "")


async def get_chat_stream(
    request: ChatRequest, use_rag: bool = True
) -> AsyncIterator[str]:
    response = await agent_chat_service.get_ai_response(request, use_rag=use_rag)
    async for chunk in agent_chat_service.generate_stream(response):
        yield chunk


def reinitialize_client() -> None:
    agent_chat_service.ai_service.reinitialize_client()
