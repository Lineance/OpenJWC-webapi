from fastapi import APIRouter, Depends
from app.models.schemas import ResponseModel
from app.services.sql_db_service import db
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute

logger = setup_logger("get_settings_logs")

router = APIRouter(route_class=LoggingRoute)


@router.get("/settings", response_model=ResponseModel)
async def get_latest_notices(
    admin_info: dict = Depends(verify_admin_token),
):
    """
    获取系统设置信息。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    return ResponseModel(msg="请求成功", data=db.get_all_settings())
