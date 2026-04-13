from asyncio import to_thread
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute
from app.application.submission.submission_service import (
    audit_and_import_submission,
    get_submission_detail,
    get_submissions_for_admin,
)
from app.models.schemas import ResponseModel, UpdateStatusRequest
from app.utils.logging_manager import setup_logger

logger = setup_logger("admin_submission_logs")

router = APIRouter(prefix="/submissions", route_class=LoggingRoute)


@router.get("", response_model=ResponseModel)
async def get_pending_submissions(
    status: Annotated[str | None, Query(description="可选的指定状态")] = None,
    page: int = Query(1, ge=1, description="返回的页码"),
    size: int = Query(20, ge=1, description="每页返回的数量，最大不超过50条"),
    admin_info: dict = Depends(verify_admin_token),
):
    """
    获取待审核的资讯列表。
    管理员特供。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    total, notices = await to_thread(get_submissions_for_admin, page, size, status)
    return ResponseModel(
        msg="获取成功",
        data={
            "total": total,
            "notices": notices,
        },
    )


@router.get("/{id}", response_model=ResponseModel)
async def get_submission_content(
    id: str = Path(description="目标提交的id"),
    admin_info: dict = Depends(verify_admin_token),
):
    """
    获取一个待审核提交的详细信息。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    return ResponseModel(
        msg="获取成功", data=await to_thread(get_submission_detail, id)
    )


# TODO:
@router.post("/{id}/review", response_model=ResponseModel)
async def update_submission_status(
    request: UpdateStatusRequest,
    id: str = Path(description="目标提交的id"),
    admin_info: dict = Depends(verify_admin_token),
):
    """
    对一个待审核提交进行审核。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    success = await to_thread(
        audit_and_import_submission,
        id,
        request.action,
        request.review,
    )
    if not success:
        return ResponseModel(msg="修改失败", data={})
    return ResponseModel(msg="修改成功", data={})
