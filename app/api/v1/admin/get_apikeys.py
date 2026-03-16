from fastapi import APIRouter, Depends, Query
from typing import Annotated
from app.models.schemas import ResponseModel
from app.services.sql_db_service import db
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute

logger = setup_logger("get_apikeys_logs")

router = APIRouter(route_class=LoggingRoute)


@router.get("/apikeys", response_model=ResponseModel)
async def get_latest_notices(
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
