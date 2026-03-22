from app.services.prompt_engine import PromptEngine
from app.services.vector_db_service import vector_db
from openai import AsyncOpenAI
import openai
from fastapi import HTTPException
from app.utils.logging_manager import setup_logger
import logging
from app.models.schemas import ChatRequest
from app.services.sql_db_service import db
import asyncio
import httpx

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = setup_logger("ai_service_logs")

http_client = httpx.AsyncClient(proxy=None, timeout=60.0)

client = AsyncOpenAI(
    api_key=db.get_system_setting("deepseek_api_key"),
    base_url="https://api.deepseek.com",
    http_client=http_client,
)


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
    return await client.chat.completions.create(
        model="deepseek-chat", messages=messages, stream=stream
    )


async def get_ai_response(request: ChatRequest, use_rag=False):
    context = ""
    if request.notice_ids:
        context += "\n用户指定了以下资讯，请你对这些资讯的信息更加注意。请你格外注意站在用户的视角看待资讯，用户只能看到他们自己选中的资讯。所以用户问题中的一些代词可能特指他们自己选中的资讯："
        for notice_id in request.notice_ids:
            target_notice = db.get_notice_content(notice_id)
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
                    vector_db.search, request.user_query, 3
                )
            except Exception as e:
                logger.error(f"向量数据库检索失败: {e}")
        else:
            try:
                logger.info("尝试从向量数据库检索相关资讯...")
                context += "\n以下是通过知识库获取的和用户需求相关的部分资讯。请注意提示用户你可能并没有获取所有必需的资讯。\n"
                context += await asyncio.to_thread(
                    vector_db.search, request.user_query, 10
                )
            except Exception as e:
                logger.error(f"向量数据库检索失败: {e}")

    # 获取组装好的 Prompt
    messages = PromptEngine.build_chat_prompt(
        request.history, request.user_query, context
    )

    if bool(db.get_system_setting("prompt_debug")):
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
