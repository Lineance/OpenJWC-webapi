from fastapi import APIRouter, Depends, HTTPException
from typing import Tuple
from app.models.schemas import ResponseModel
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.api.dependencies import verify_api_key_and_device
from app.utils.logging_manager import setup_logger
from app.api.logging_route import LoggingRoute

logger = setup_logger("device_api_logs")

router = APIRouter(route_class=LoggingRoute, prefix="/device")


@router.get("", response_model=ResponseModel)
async def get_devices(
    valid_token: Tuple[str, str] = Depends(verify_api_key_and_device),
):
    """
    获取当前apikey能绑定的最大设备数以及目前绑定的设备。
    """
    apikey, device_id = valid_token
    return db.get_device_info(key_string=apikey, device_id=device_id)


@router.post("/unbind")
async def unbind_device(valid_token_and_device=Depends(verify_api_key_and_device)):
    """解绑设备"""
    success = db.unbind_device(valid_token_and_device[0], valid_token_and_device[1])
    logger.info(success)
    if not success:
        raise HTTPException(status_code=404, detail="绑定关系不存在或Key无效")
    return {"detail": "解绑成功，名额已释放。"}

