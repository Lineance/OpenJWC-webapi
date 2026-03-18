from typing import Any, Dict
from fastapi import APIRouter, Depends
from app.models.schemas import ResponseModel
from app.services.sql_db_service import db
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute
from app.core.security import verify_password

logger = setup_logger("modify_password_logs")

router = APIRouter(route_class=LoggingRoute)


@router.put("/password", response_model=ResponseModel)
async def toggle_apikey(
    settings: Dict[str, Any],
    admin_info: dict = Depends(verify_admin_token),
):
    """
    修改管理员密码。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    admin = db.get_admin_user(admin_info["username"])
    if admin:
        if verify_password(settings["old_password"], admin["hashed_password"]):
            db.modify_password(admin_info["username"], settings["new_password"])
            return ResponseModel(msg="修改成功", data={})
        else:
            return ResponseModel(msg="旧密码错误", data={})
    else:
        return ResponseModel(msg="用户不存在", data={})
