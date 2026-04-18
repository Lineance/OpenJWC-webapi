from fastapi import APIRouter, Depends
from app.models.schemas import ResponseModel
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_client_token
from app.api.logging_route import LoggingRoute

logger = setup_logger("register_api_logs")

router = APIRouter(prefix="/register", route_class=LoggingRoute)


@router.post("", response_model=ResponseModel)
async def register_device(auth: dict = Depends(verify_client_token)):
    return ResponseModel(msg="设备注册成功", data={})
