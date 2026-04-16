from asyncio import to_thread
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute
from app.application.user_registration.user_registration_service import (
    audit_user_registration,
    get_pending_registrations_for_admin,
    get_registration_detail,
)
from app.models.schemas import ResponseModel, UpdateStatusRequest
from app.utils.logging_manager import setup_logger

logger = setup_logger("admin_user_registration_logs")

router = APIRouter(prefix="/user-registrations", route_class=LoggingRoute)


@router.get("", response_model=ResponseModel)
async def get_user_registrations(
    status: Annotated[str | None, Query(description="可选的指定状态")] = None,
    page: int = Query(1, ge=1, description="返回的页码"),
    size: int = Query(20, ge=1, description="每页返回的数量，最大不超过50条"),
    admin_info: dict = Depends(verify_admin_token),
):
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    total, users = await to_thread(
        get_pending_registrations_for_admin, page, size, status
    )
    return ResponseModel(
        msg="获取成功",
        data={
            "total": total,
            "users": users,
        },
    )


@router.get("/{id}", response_model=ResponseModel)
async def get_user_registration_detail(
    id: str = Path(description="目标用户ID"),
    admin_info: dict = Depends(verify_admin_token),
):
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    return ResponseModel(
        msg="获取成功", data=await to_thread(get_registration_detail, id)
    )


@router.post("/{id}/review", response_model=ResponseModel)
async def review_user_registration(
    request: UpdateStatusRequest,
    id: str = Path(description="目标用户ID"),
    admin_info: dict = Depends(verify_admin_token),
):
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    success = await to_thread(
        audit_user_registration,
        id,
        request.action,
        request.review,
    )
    if not success:
        return ResponseModel(msg="审核失败", data={})
    return ResponseModel(msg="审核成功", data={})
