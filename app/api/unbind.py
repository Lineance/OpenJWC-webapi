from fastapi import APIRouter, Depends, HTTPException
from app.utils.logging_manager import setup_logger
from app.api.dependencies import verify_api_key_and_device
from app.services.sql_db_service import db

logger = setup_logger("unbind_api_logs")

router = APIRouter()


@router.post("/client/device/unbind")
async def chat_with_notice(valid_token_and_device=Depends(verify_api_key_and_device)):
    success = db.unbind_device(valid_token_and_device[0], valid_token_and_device[1])
    if not success:
        raise HTTPException(status_code=404, detail="绑定关系不存在或Key无效")
    return {"detail": "解绑成功，名额已释放。"}
