from fastapi import APIRouter, Depends, Path, Query
from app.models.schemas import ResponseModel, CreateApiKeyRequest, ToggleApiKeyRequest
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute
from typing import Annotated


logger = setup_logger("apikey_logs")

router = APIRouter(prefix="/apikeys", route_class=LoggingRoute)


@router.post("", response_model=ResponseModel)
async def create_api_key(
    request: CreateApiKeyRequest,
    admin_info: dict = Depends(verify_admin_token),
):
    """
    创建新的apikey。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    try:
        new_key = db.create_api_key(request.owner_name, request.max_devices)
        return ResponseModel(msg="Apikey创建成功。", data={"new_key": new_key})
    except Exception:
        return ResponseModel(msg="Apikey创建失败：未知错误。", data={"new_key": ""})


@router.delete("/{key_id}", response_model=ResponseModel)
async def delete_apikey(
    key_id: int = Path(description="目标apikey"),
    admin_info: dict = Depends(verify_admin_token),
):
    """
    删除apikey。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")

    try:
        db.delete_api_key(key_id)
        logger.info("API key deleted successfully.")
        return ResponseModel(msg="APIkey删除成功。", data={})
    except Exception as e:
        logger.error(f"Error deleting API key: {e}")
        return ResponseModel(msg="APIkey删除失败。", data={})


@router.get("", response_model=ResponseModel)
async def get_apikeys(
    page: int = Query(1, ge=1, description="请求的页码，从1开始"),
    size: int = Query(20, ge=1, le=50, description="每页返回的数量，最大不超过50条"),
    keyword: Annotated[str | None, Query(description="指定的apikey用户")] = None,
    admin_info: dict = Depends(verify_admin_token),
):
    """
    获取所有的apikey信息
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    return ResponseModel(
        msg="请求成功", data=db.get_target_api_keys(page, size, keyword)
    )


@router.put("/{key_id}/status", response_model=ResponseModel)
async def toggle_apikey(
    request: ToggleApiKeyRequest,
    key_id: int = Path(description="目标apikey"),
    admin_info: dict = Depends(verify_admin_token),
):
    """
    启停apikey。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    try:
        db.toggle_key_status(key_id=key_id, is_active=request.is_active)
        logger.info(f"{key_id}的apikey启用状态已调整为{request.is_active}")
        return ResponseModel(
            msg=f"{key_id}的启用状态调整为{request.is_active}", data={}
        )
    except Exception as e:
        logger.error(f"启停apikey时遇到未知错误: {e}")
        return ResponseModel(msg="启停apikey时遇到未知错误", data={})

