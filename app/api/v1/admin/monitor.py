from fastapi import APIRouter, Depends
from app.models.schemas import ResponseModel
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute
from app.utils.sysinfo_monitor import get_server_status
from asyncio import to_thread
import traceback

logger = setup_logger("monitor_logs")

router = APIRouter(prefix="/monitor", route_class=LoggingRoute)


@router.get("/stats", response_model=ResponseModel)
async def get_stats(admin_info: dict = Depends(verify_admin_token)):
    """
    获取服务系统信息
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    try:
        total_api_calls = await to_thread(db.get_total_api_calls)
        active_keys_count = await to_thread(db.get_active_keys_counts)
        total_notices = await to_thread(db.get_total_notices)

        return ResponseModel(
            msg="请求成功",
            data={
                "total_api_calls": total_api_calls,
                "active_keys_count": active_keys_count,
                "total_notices": total_notices,
            },
        )
    except Exception as e:
        logger.error(f"获取统计数据失败: {traceback.format_exc()}")
        return ResponseModel(
            msg="获取统计数据失败",
            data={
                "total_api_calls": 0,
                "active_keys_count": 0,
                "total_notices": 0,
            },
        )


@router.get("/sysinfo", response_model=ResponseModel)
async def get_sysinfo(admin_info: dict = Depends(verify_admin_token)):
    """
    获取服务系统信息
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    try:
        return get_server_status()
    except Exception as e:
        logger.error(f"获取系统信息失败: {traceback.format_exc()}")
        return ResponseModel(data=None, msg=f"获取系统信息失败: {str(e)}")

