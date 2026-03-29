from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from app.models.schemas import SubmissionRequest, ResponseModel
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_api_key
from app.api.logging_route import LoggingRoute
from app.services.sql_db_service import db
from asyncio import to_thread

logger = setup_logger("submission_api_logs")

router = APIRouter(prefix="/submissions", route_class=LoggingRoute)


# TODO: 测试该模块功能


@router.post("")
async def client_submission(
    request: SubmissionRequest, valid_token: str = Depends(verify_api_key)
):
    success = await to_thread(db.create_submission, request, valid_token)
    if success:
        return ResponseModel(msg="提交成功", data={})
    else:
        return JSONResponse(
            status_code=422,
            content={
                "msg": f"正文文字量超过上限:{db.get_system_setting('submission_max_length')}",
                "data": {},
            },
        )


@router.get("/my")
async def process_query(valid_token: str = Depends(verify_api_key)):
    notices = await to_thread(db.get_submission_by_apikey, valid_token)
    return ResponseModel(
        msg="提交成功",
        data={"total": len(notices), "notices": notices},
    )
