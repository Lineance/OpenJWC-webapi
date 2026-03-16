from fastapi import APIRouter, Depends
from app.models.schemas import ResponseModel, CreateApiKeyRequest
from app.services.sql_db_service import db
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute

logger = setup_logger("create_apikey_logs")

router = APIRouter(route_class=LoggingRoute)


@router.post("/apikeys", response_model=ResponseModel)
async def get_latest_notices(
    request: CreateApiKeyRequest,
    admin_info: dict = Depends(verify_admin_token),
):
    """
    创建新的apikey。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    try:
        db.create_api_key(request.owner_name, request.max_devices)
        return ResponseModel(msg="Apikey创建成功。", data={})
    except Exception:
        return ResponseModel(msg="Apikey创建失败：未知错误。", data={})
