from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from app.models.schemas import ChatRequest
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_api_key
import app.services.ai_service as ai_service
from app.api.logging_route import LoggingRoute

logger = setup_logger("chat_api_logs")

router = APIRouter(route_class=LoggingRoute)


@router.post("/chat")
async def chat_with_notice(
    request: ChatRequest, valid_token: str = Depends(verify_api_key)
):
    logger.info(f"接受到LLM聊天请求: {valid_token[:8]}...")
    response = await ai_service.get_ai_response(request, use_rag=True)
    if request.stream:
        logger.info("尝试流式输出")
        return StreamingResponse(
            ai_service.generate_stream(response), media_type="text/event-stream"
        )

    else:
        logger.info("尝试非流式输出")
        full_reply = response.choices[0].message.content
        return {"status": "success", "reply": full_reply}
