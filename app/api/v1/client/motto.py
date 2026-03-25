from datetime import date
from fastapi import APIRouter, Depends
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_api_key
from app.models.schemas import ResponseModel
from app.api.logging_route import LoggingRoute
from app.services.sql_db_service import db

logger = setup_logger("motto_api_logs")

router = APIRouter(prefix="/motto", route_class=LoggingRoute)


@router.get("", response_model=ResponseModel)
async def get_motto(valid_token: str = Depends(verify_api_key)):
    today_str = date.today().strftime("%Y-%m-%d")
    success, data = db.get_today_motto(today_str)
    if (not success) and db.insert_motto_from_hitokoto(today_str):
        success, data = db.get_today_motto(today_str)
    if success:
        return ResponseModel(
            msg="每日一言获取成功",
            data={"text": data["motto_content"], "author": data["motto_author"]},
        )
    return ResponseModel(
        msg="每日一言获取失败",
        data={
            "text": "后端出现未知问题导致未成功请求到每日一言，就像你人生中的许多其他事情一样。",
            "author": "Moonhalf",
        },
    )
