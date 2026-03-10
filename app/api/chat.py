from fastapi import APIRouter
from app.models.schemas import ChatRequest

router = APIRouter()


@router.post("/chat")
async def chat_with_notice(request: ChatRequest):
    return {
        "status": "success",
        "reply": "根据通知，截止日期为本周五下午5点。",  # 假设这是LLM的回答
    }
