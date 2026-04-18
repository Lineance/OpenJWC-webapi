from asyncio import to_thread
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import verify_client_token
from app.api.logging_route import LoggingRoute
from app.infrastructure.storage.sqlite.notice_repository import get_notice_repository
from app.models.schemas import ResponseModel
from app.utils.logging_manager import setup_logger

logger = setup_logger("client_notice_logs")

router = APIRouter(prefix="/notices", route_class=LoggingRoute)


@router.get("", response_model=ResponseModel)
async def get_latest_notices(
    label: Annotated[str | None, Query(description="可选的指定标签")] = None,
    page: int = Query(1, ge=1, description="返回的页码"),
    size: int = Query(20, ge=1, le=50, description="每页返回的数量，最大不超过50条"),
    auth: dict = Depends(verify_client_token),
):
    """
    获取教务处最新资讯列表（支持分页）
    """
    notice_repo = get_notice_repository()
    offset = size * (page - 1)
    limit = size
    total, notices = await to_thread(notice_repo.list_for_notices, limit, offset, label)
    return ResponseModel(
        msg="获取成功",
        data={
            "total_returned": total,
            "total_label": await to_thread(notice_repo.get_notice_total_labels),
            "notices": notices,
        },
    )


# TODO: 获取所有标签的接口。
@router.get("/labels", response_model=ResponseModel)
async def get_notices_labels(
    auth: dict = Depends(verify_client_token),
):
    """
    获取所有标签。
    """
    notice_repo = get_notice_repository()
    return ResponseModel(
        msg="获取成功", data={"labels": await to_thread(notice_repo.get_notice_labels)}
    )
