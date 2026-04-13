from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from app.api.dependencies import verify_admin_token
from app.api.logging_route import LoggingRoute
from app.infrastructure.storage.lancedb.connection import get_connection
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.models.schemas import ResponseModel
from app.utils.logging_manager import setup_logger

logger = setup_logger("admin_notice_logs")

router = APIRouter(prefix="/notices", route_class=LoggingRoute)


@router.get("", response_model=ResponseModel)
async def get_latest_notices(
    label: Annotated[str | None, Query(description="可选的指定标签")] = None,
    page: int = Query(1, ge=1, description="返回的页码"),
    size: int = Query(20, ge=1, le=50, description="每页返回的数量，最大不超过50条"),
    admin_info: dict = Depends(verify_admin_token),
):
    """
    获取教务处最新资讯列表（支持分页）
    管理员特供。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
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
    admin_info: dict = Depends(verify_admin_token),
):
    """
    获取所有标签。
    管理员特供。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    return ResponseModel(msg="获取成功", data={"labels": db.get_labels()})


@router.delete("/{notice_id}", response_model=ResponseModel)
async def delete_notice(
    notice_id: str = Path(description="目标资讯的id"),
    admin_info: dict = Depends(verify_admin_token),
):
    """
    删除某个已入库资讯。
    """
    logger.info(f"Request ID: {admin_info['x_request_id']}")
    logger.info(f"Client Version: {admin_info['x_client_version']}")
    try:
        db.delete_notice_by_id(notice_id=notice_id)
        get_connection().rebuild_article_order()
        logger.info("Notice deleted successfully.")
        return ResponseModel(msg="入库资讯删除成功。", data={})
    except Exception as e:
        logger.error(f"Error deleting notice: {e}")
        return ResponseModel(msg="入库资讯删除失败。", data={})
