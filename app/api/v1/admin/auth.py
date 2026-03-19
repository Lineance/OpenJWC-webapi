from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm
from app.core.security import verify_password, create_access_token
from app.services.sql_db_service import db
from app.models.schemas import ResponseModel
from app.utils.logging_manager import setup_logger
from app.core.config import ACCESS_TOKEN_EXPIRE_MINUTES
from app.api.logging_route import LoggingRoute

router = APIRouter(prefix="/auth", route_class=LoggingRoute)

logger = setup_logger("admin_auth_logs")


@router.post("/login", response_model=ResponseModel)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    面板管理员登录接口
    注意：为了兼容 OAuth2 标准，前端发来的请求体必须是 form-data，而不是 JSON。
    包含两个字段：username 和 password。
    """
    user = db.get_admin_user(form_data.username)
    if not user:
        logger.warning(f"尝试登录的用户不存在: {form_data.username}")
        return ResponseModel(msg="用户不存在", data=None)

    if not verify_password(form_data.password, user["hashed_password"]):
        logger.warning(f"用户 {form_data.username} 密码错误")
        return ResponseModel(msg="密码错误", data=None)

    access_token = create_access_token(data={"sub": user["user_name"]})

    return ResponseModel(
        msg="登录成功",
        data={"token": access_token, "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60},
    )
