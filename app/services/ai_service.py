from app.services.prompt_engine import PromptEngine
from app.services.vector_db_service import vector_db  # 假设你有个向量数据库服务
from openai import AsyncOpenAI
import os

client = AsyncOpenAI(
    api_key=os.environ.get("DEEPSEEK_API_KEY"), base_url="https://api.deepseek.com"
)


async def get_ai_response(request, use_rag=False):
    context = None
    if use_rag:
        # 在这里调用向量数据库检索相关资讯
        context = await vector_db.search(request.user_query)

    # 获取组装好的 Prompt
    messages = PromptEngine.build_chat_prompt(
        request.history, request.user_query, context
    )

    # 调用 OpenAI/DeepSeek API
    return await client.chat.completions.create(
        model="deepseek-chat", messages=messages, stream=request.stream
    )


# 定义一个异步生成器
async def generate_stream(response):
    # 当 stream=True 时，response 是一个异步生成器
    async for chunk in response:
        content = chunk.choices[0].delta.content
        if content is not None:
            yield f"data: {content}\n\n"
    yield "data: [DONE]\n\n"
