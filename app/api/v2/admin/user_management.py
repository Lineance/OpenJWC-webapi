from asyncio import to_thread
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query
from pydantic import BaseModel

from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute
from app.models.schemas import ResponseModel
from app.utils.logging_manager import setup_logger

logger = setup_logger("admin_user_management_logs")

router = APIRouter(prefix="/users", route_class=LoggingRoute)


class SetUserActiveRequest(BaseModel):
    is_active: bool


@router.get("", response_model=ResponseModel)
async def get_users(
    is_active: Annotated[bool | None, Query(description="按账号状态筛选")] = None,
    page: int = Query(1, ge=1, description="返回的页码"),
    size: int = Query(20, ge=1, description="每页返回的数量，最大不超过50条"),
    admin_info: dict = Depends(verify_admin_token),
):
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    offset = size * (page - 1)
    from app.infrastructure.storage.sqlite.sql_db_service import db
    total, users = await to_thread(
        db.list_users_for_admin, offset, size, is_active
    )
    return ResponseModel(
        msg="获取成功",
        data={
            "total": total,
            "users": users,
        },
    )


@router.post("/{id}/status", response_model=ResponseModel)
async def set_user_active_status(
    request: SetUserActiveRequest,
    id: str = Path(description="目标用户ID"),
    admin_info: dict = Depends(verify_admin_token),
):
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    from app.infrastructure.storage.sqlite.sql_db_service import db
    success = await to_thread(
        db.set_user_active_status, int(id), request.is_active
    )
    if not success:
        return ResponseModel(msg="修改失败", data={})
    return ResponseModel(msg="修改成功", data={})


@router.delete("/{id}", response_model=ResponseModel)
async def delete_user(
    id: str = Path(description="目标用户ID"),
    admin_info: dict = Depends(verify_admin_token),
):
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    from app.infrastructure.storage.sqlite.sql_db_service import db
    success = await to_thread(db.delete_user, int(id))
    if not success:
        return ResponseModel(msg="删除失败", data={})
    return ResponseModel(msg="删除成功", data={})
