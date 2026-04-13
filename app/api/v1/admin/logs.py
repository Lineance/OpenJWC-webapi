from fastapi import APIRouter, Depends, Query
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.models.schemas import ResponseModel
from app.utils import logging_manager
from app.utils.logging_manager import setup_logger
from app.api.logging_route import LoggingRoute
from typing import Dict, Any, List, Annotated
from app.api.dependencies import verify_admin_token
from app.utils.logging_manager import parse_logs

router = APIRouter(prefix="/logs", route_class=LoggingRoute)

logger = setup_logger("logs_logs")


@router.get("/", response_model=ResponseModel)
async def get_logs(
    level: Annotated[str | None, Query(description="可选，日志等级")] = None,
    size: int = Query(20, description="返回日志一页的条目数"),
    page: int = Query(1, description="返回日志的页码"),
    module: Annotated[str | None, Query(description="可选，来源模块")] = None,
    keyword: Annotated[str | None, Query(description="可选，模糊搜索")] = None,
    admin_info: dict = Depends(verify_admin_token),
):
    """
    获取日志。
    """
    filtered_logs = parse_logs(level=level, module=module, keyword=keyword)
    total = len(filtered_logs)
    logs = filtered_logs[(page - 1) * size : page * size]
    return ResponseModel(
        msg="获取日志成功",
        data={"total": total, "logs": logs},
    )


@router.get("/modules", response_model=ResponseModel)
async def get_logs_modules(
    admin_info: dict = Depends(verify_admin_token),
):
    """
    获取日志来源模块。
    """
    logs = parse_logs()
    modules = list(set([log["module"] for log in logs]))
    modules.sort()
    return ResponseModel(msg="获取日志来源模块成功", data={"modules": modules})

