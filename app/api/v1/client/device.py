from fastapi import APIRouter, Depends
from typing import Tuple
from app.models.schemas import ResponseModel
from app.services.sql_db_service import db
from app.api.dependencies import verify_api_key_and_device
from app.utils.logging_manager import setup_logger
from app.api.logging_route import LoggingRoute

logger = setup_logger("device_api_logs")

router = APIRouter(route_class=LoggingRoute)


@router.get("/device", response_model=ResponseModel)
async def get_latest_notices(
    valid_token: Tuple[str, str] = Depends(verify_api_key_and_device),
):
    """
    获取当前apikey能绑定的最大设备数以及目前绑定的设备。
    """
    apikey, device_id = valid_token
    return db.get_device_info(key_string=apikey, device_id=device_id)
