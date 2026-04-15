from fastapi import APIRouter, Depends, Header, HTTPException, status
from app.models.v2_schemas import V2Response, V2DetailResponse, UnbindRequest
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.api.dependencies import verify_client_token
from app.utils.logging_manager import setup_logger
from app.api.logging_route import LoggingRoute

logger = setup_logger("v2_device_logs")

router = APIRouter(prefix="/device", route_class=LoggingRoute)


@router.get("", response_model=V2Response)
async def get_device_list(
    current_user: dict = Depends(verify_client_token),
):
    """获取当前账号登录的设备列表"""
    devices = db.get_user_devices(user_id=current_user["user_id"])
    return V2Response(
        msg="请求成功",
        data={"devices": devices},
    )


@router.post("/unbind", response_model=V2DetailResponse)
async def unbind_device(
    body: UnbindRequest,
    current_user: dict = Depends(verify_client_token),
):
    """解绑设备（目标设备ID从请求体传入，与鉴权用的 X-Device-ID 分离）"""
    success = db.unbind_user_device(
        user_id=current_user["user_id"],
        device_uuid=body.device_uuid,
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="设备不存在或未绑定",
        )
    logger.info(f"用户[{current_user['username']}]解绑设备[{body.device_uuid[:8]}...]成功")
    return V2DetailResponse(detail="解绑成功")
