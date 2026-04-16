import asyncio
import logging

import httpx
import openai
from fastapi import HTTPException
from openai import AsyncOpenAI
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.core.config import DATA_DIR
from app.infrastructure.agent.chat.prompt_engine import PromptEngine
from app.infrastructure.retrieval.engine import RetrievalEngine
from app.infrastructure.storage.lancedb.repository import get_article_repository
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.models.schemas import ChatRequest
from app.utils.logging_manager import setup_logger

logger = setup_logger("ai_service_logs")

http_client = httpx.AsyncClient(proxy=None, timeout=60.0)
retrieval_engine = RetrievalEngine(
    db_path=str(DATA_DIR / "lancedb"), table_name="articles"
)


def _get_article_repo():
    return get_article_repository()


def _search_context(query: str, top_k: int) -> str:
    payload = retrieval_engine.semantic_search(
        query=query,
        field="content",
        similarity_threshold=0.0,
        limit=top_k,
    )
    results = payload.get("results", [])
    if not results:
        return "未检索到相关资讯。"

    lines = []
    for index, item in enumerate(results, start=1):
        preview = str(item.get("content_text", ""))
        if len(preview) > 180:
            preview = preview[:180] + "..."
        lines.append(
            "\n".join(
                [
                    f"{index}. 标题：{item.get('title', '')}",
                    f"日期：{item.get('publish_date', '')}",
                    f"链接：{item.get('url', '')}",
                    f"正文：{preview}",
                ]
            )
        )
    return "\n\n".join(lines)


class AIService:
    def __init__(self):
        self.client = AsyncOpenAI(
            api_key=db.get_system_setting("deepseek_api_key"),
            base_url="https://api.deepseek.com",
            http_client=http_client,
        )

    def reinitialize_client(self):
        """重新实例化deepseek client，用于感知系统设置中apikey的变化"""
        logger.info("正在重新初始化DeepSeek客户端")
        self.client = AsyncOpenAI(
            api_key=db.get_system_setting("deepseek_api_key"),
            base_url="https://api.deepseek.com",
            http_client=http_client,
        )
        logger.info("DeepSeek客户端重新初始化完成")


ai_service = AIService()


@retry(
    retry=retry_if_exception_type(
        (
            openai.APIConnectionError,
            openai.APITimeoutError,
            openai.InternalServerError,
            openai.RateLimitError,
        )
    ),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(4),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def call_llm_with_retry(messages: list, stream: bool):
    """封装调用 LLM 的底层逻辑，并附加重试机制"""
    return await ai_service.client.chat.completions.create(
        model="deepseek-chat", messages=messages, stream=stream
    )


async def get_ai_response(request: ChatRequest, use_rag=False):
    context = ""
    if request.notice_ids:
        context += """
\n用户指定了以下资讯，请你对这些资讯的信息更加注意。
请你格外注意站在用户的视角看待资讯，用户只能看到他们自己选中的资讯。
所以对于"这篇"或"那篇"这样的代词实际应为用户自身指定的资讯。
对于此类情形，你不必向用户求证，优先默认其为用户最新选中的资讯：
"""
        for notice_id in request.notice_ids:
            target_notice = _get_article_repo().get_notice_content(notice_id)
            if target_notice:
                logger.info(f"检测到用户指定资讯：{target_notice['title']}")
                context += f"\n资讯标题：{target_notice['title']}"
                context += f"\n资讯正文：{target_notice['content_text']}"
                context += f"\n资讯日期：{target_notice['date']}"
    if use_rag:
        if context != "":
            try:
                logger.info("尝试从向量数据库检索相关资讯...")
                context += "\n以下是通过知识库获取的和用户需求可能相关的资讯。请你更多关注用户指定的资讯。\n"
                context += await asyncio.to_thread(
                    _search_context, request.user_query, 3
                )
            except Exception as e:
                logger.error(f"向量数据库检索失败: {e}")
        else:
            try:
                logger.info("尝试从向量数据库检索相关资讯...")
                context += "\n以下是通过知识库获取的和用户需求相关的部分资讯。请注意提示用户你可能并没有获取所有必需的资讯。\n"
                context += await asyncio.to_thread(
                    _search_context, request.user_query, 10
                )
            except Exception as e:
                logger.error(f"向量数据库检索失败: {e}")

    # 获取组装好的 Prompt
    messages = PromptEngine.build_chat_prompt(
        request.history, request.user_query, context
    )

    if db.get_system_setting("prompt_debug") != "0":
        logger.debug(messages)

    # 调用 OpenAI/DeepSeek API
    logger.info("调用Deepseek API...")
    try:
        return await call_llm_with_retry(messages, request.stream)
    except openai.APIConnectionError as e:
        logger.error(f"DeepSeek 连接失败: {e}")
        raise HTTPException(status_code=503, detail="AI 服务暂时不可用 (网络连接失败)")
    except openai.RateLimitError:
        logger.error("DeepSeek 服务请求过于频繁")
        raise HTTPException(status_code=429, detail="AI 服务请求过于频繁")
    except Exception as e:
        logger.error(f"DeepSeek 连接失败：{e}")
        raise HTTPException(status_code=500, detail=f"内部错误: {str(e)}")


async def generate_stream(response):
    async for chunk in response:
        content = chunk.choices[0].delta.content
        if content is not None:
            yield f"data: {content}\n\n"
    yield "data: [DONE]\n\n"
