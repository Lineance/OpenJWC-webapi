from asyncio import to_thread

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.api.dependencies import verify_api_key
from app.api.logging_route import LoggingRoute
from app.application.submission.submission_service import (
    get_my_submissions,
    submit_submission,
)
from app.domain.submission import SubmissionContent, SubmissionDraft
from app.models.schemas import ResponseModel, SubmissionRequest
from app.utils.logging_manager import setup_logger

logger = setup_logger("submission_api_logs")

router = APIRouter(prefix="/submissions", route_class=LoggingRoute)


# TODO: 测试该模块功能


@router.post("")
async def client_submission(
    request: SubmissionRequest, valid_token: str = Depends(verify_api_key)
):
    draft = SubmissionDraft(
        label=request.label,
        title=request.title,
        date=request.date,
        detail_url=request.detail_url,
        is_page=request.is_page,
        content=SubmissionContent(
            text=request.content.text,
            attachment_urls=request.content.attachment_urls,
        ),
    )
    success, message = await to_thread(submit_submission, draft, valid_token)
    if success:
        return ResponseModel(msg=message, data={})
    else:
        return JSONResponse(
            status_code=422,
            content={
                "msg": message,
                "data": {},
            },
        )


@router.get("/my")
async def process_query(valid_token: str = Depends(verify_api_key)):
    notices = await to_thread(get_my_submissions, valid_token)
    return ResponseModel(
        msg="提交成功",
        data={"total": len(notices), "notices": notices},
    )
