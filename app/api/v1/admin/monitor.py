from fastapi import APIRouter, Depends
from app.models.schemas import ResponseModel
from app.services.sql_db_service import db
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute
from app.utils.sysinfo_monitor import get_server_status

logger = setup_logger("monitor_logs")

router = APIRouter(prefix="/monitor", route_class=LoggingRoute)


@router.get("/stats", response_model=ResponseModel)
async def get_stats(admin_info: dict = Depends(verify_admin_token)):
    """
    获取服务系统信息
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    return ResponseModel(
        msg="请求成功",
        data={
            "total_api_calls": db.get_total_api_calls(),
            "active_keys_count": db.get_active_keys_counts(),
            "total_notices": db.get_total_notices(),
        },
    )


@router.get("/sysinfo", response_model=ResponseModel)
async def get_sysinfo(admin_info: dict = Depends(verify_admin_token)):
    """
    获取服务系统信息
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    return get_server_status()
