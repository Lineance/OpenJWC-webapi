from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

import app.application.chat.ai_service as ai_service
from app.api.dependencies import verify_api_key
from app.api.logging_route import LoggingRoute
from app.models.schemas import ChatRequest
from app.utils.logging_manager import setup_logger

logger = setup_logger("chat_api_logs")

router = APIRouter(prefix="/chat", route_class=LoggingRoute)


@router.post("")
async def chat_with_notice(
    request: ChatRequest, valid_token: str = Depends(verify_api_key)
):
    logger.info(f"接受到LLM聊天请求: {valid_token[:8]}...")
    if request.stream:
        logger.info("尝试流式输出")
        return StreamingResponse(
            ai_service.get_chat_stream(request, use_rag=True),
            media_type="text/event-stream",
        )

    else:
        logger.info("尝试非流式输出")
        full_reply = await ai_service.get_chat_text(request, use_rag=True)
        return {"status": "success", "reply": full_reply}
