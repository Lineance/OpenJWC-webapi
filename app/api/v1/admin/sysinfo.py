from fastapi import APIRouter, Depends
from app.models.schemas import ResponseModel
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_admin_token
from app.utils.sysinfo_monitor import get_server_status
from app.api.logging_route import LoggingRoute

logger = setup_logger("sysinfo_logs")

router = APIRouter(route_class=LoggingRoute)


@router.get("/sysinfo", response_model=ResponseModel)
async def get_latest_notices(admin_info: dict = Depends(verify_admin_token)):
    """
    获取服务系统信息
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    return get_server_status()
