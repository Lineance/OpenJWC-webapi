from fastapi import APIRouter, Query
from app.models.schemas import NoticeListResponse
from app.services.sql_db_service import db  # 导入我们在上一节写的单例数据库服务

router = APIRouter()


@router.get("/notices", response_model=NoticeListResponse)
async def get_latest_notices(
    page: int = Query(1, ge=1, description="请求的页码，从1开始"),
    size: int = Query(20, ge=1, le=50, description="每页返回的数量，最大不超过50条"),
):
    """
    获取教务处最新资讯列表（支持分页）
    """
    # 1. 计算数据库查询的偏移量 (Offset)
    # 比如：第1页 -> offset 0; 第2页 -> offset 20 (假设size=20)
    offset = (page - 1) * size

    # 2. 调用 DB 服务获取数据
    notices_data = db.get_notices_for_app(limit=size, offset=offset)

    # 3. 按 Pydantic 规范拼装返回结果
    return NoticeListResponse(
        status="success",
        page=page,
        size=size,
        total_returned=len(notices_data),
        data=notices_data,
    )
