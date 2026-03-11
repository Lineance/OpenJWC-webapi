from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from app.models.schemas import ChatRequest
from app.services.ai_service import client
import app.services.ai_service as ai_service


router = APIRouter()


@router.post("/chat")
async def chat_with_notice(request: ChatRequest):
    response = ai_service.get_ai_response(request, use_rag=True)
    # 3. 分支处理：流式输出
    if request.stream:
        return StreamingResponse(
            ai_service.generate_stream(response), media_type="text/event-stream"
        )

    else:
        full_reply = response.choices[0].message.content
        return {"status": "success", "reply": full_reply}
