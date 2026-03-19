from fastapi import APIRouter, Query, Depends
from app.models.schemas import ResponseModel
from app.services.sql_db_service import db
from app.utils.logging_manager import setup_logger
from app.api.logging_route import LoggingRoute
from typing import Annotated
from app.api.dependencies import verify_api_key

logger = setup_logger("notice_api_logs")

router = APIRouter(prefix="/notices", route_class=LoggingRoute)


@router.get("", response_model=ResponseModel)
async def get_latest_notices(
    label: Annotated[str | None, Query(description="可选的指定标签")] = None,
    page: int = Query(1, ge=1, description="返回的页码"),
    size: int = Query(20, ge=1, le=50, description="每页返回的数量，最大不超过50条"),
    valid_token: str = Depends(verify_api_key),
):
    """
    获取教务处最新资讯列表（支持分页）
    """
    offset = size * (page - 1)
    limit = size
    total, notices = db.get_notices_for_app(label=label, offset=offset, limit=limit)
    return ResponseModel(
        msg="获取成功",
        data={
            "total_returned": total,
            "total_label": db.get_total_labels(),
            "notices": notices,
        },
    )


# TODO: 获取所有标签的接口。
@router.get("/labels", response_model=ResponseModel)
async def get_notices_labels(
    valid_token: str = Depends(verify_api_key),
):
    """
    获取所有标签。
    """
    return ResponseModel(msg="获取成功", data={"labels": db.get_labels()})
