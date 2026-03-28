from fastapi import APIRouter, Depends
from app.core.security import verify_password
from app.services.sql_db_service import db
from app.models.schemas import ResponseModel, UpdateSettingRequest
from app.utils.logging_manager import setup_logger
from app.api.logging_route import LoggingRoute
from typing import Dict, Any, List
from app.api.dependencies import verify_admin_token
from app.core.config import ALLOWED_SETTINGS

router = APIRouter(prefix="/settings", route_class=LoggingRoute)

logger = setup_logger("settings_logs")


@router.put("/password", response_model=ResponseModel)
async def update_password(
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


@router.get("", response_model=ResponseModel)
async def get_system_settings(
    admin_info: dict = Depends(verify_admin_token),
):
    """
    获取系统设置信息。
    """
    db._sync_settings()
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    return ResponseModel(msg="请求成功", data=db.get_all_settings())


@router.put("/reset", response_model=ResponseModel)
async def reset_settings(
    settings: List[str],
    admin_info: dict = Depends(verify_admin_token),
):
    """
    重置系统设置。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    sanitized_data = [k for k in settings if k in ALLOWED_SETTINGS.keys()]
    if len(sanitized_data) == 0:
        db.reset_all_settings()
        return ResponseModel(msg="重置全部设置。", data={})
    for key in sanitized_data:
        db.reset_system_setting(key)
    return ResponseModel(msg="修改成功", data={})


@router.put("", response_model=ResponseModel)
async def toggle_apikey(
    settings: UpdateSettingRequest,
    admin_info: dict = Depends(verify_admin_token),
):
    """
    修改系统设置。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    msg = "修改成功"
    for setting in settings.settings:
        if setting.key in ALLOWED_SETTINGS.keys():
            msg += f"，{setting.key}被修改为{setting.value}"
            db.update_system_setting(setting.key, setting.value)
    return ResponseModel(msg=msg, data={})
