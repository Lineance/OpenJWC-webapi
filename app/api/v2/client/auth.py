from fastapi import APIRouter, Header, HTTPException, status
from app.models.v2_schemas import RegisterRequest, LoginRequest, V2Response
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.core.security import create_client_token
from app.utils.logging_manager import setup_logger
from app.api.logging_route import LoggingRoute

logger = setup_logger("v2_auth_logs")

router = APIRouter(prefix="/auth", route_class=LoggingRoute)


@router.post("/register", response_model=V2Response)
async def register(
    body: RegisterRequest,
    x_device_id: str = Header(default=None),
):
    """注册账号"""
    try:
        db.create_user(
            username=body.username,
            email=body.email,
            password_hash=body.password_hash,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )

    logger.info(f"用户注册成功: {body.username}")
    return V2Response(msg="注册成功", data={})


@router.post("/login", response_model=V2Response)
async def login(
    body: LoginRequest,
    x_device_id: str = Header(default=None),
):
    """登录接口"""
    user = db.authenticate_user(
        account=body.account,
        password_hash=body.password_hash,
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="账号或密码错误",
        )

    # 生成长期 JWT Token
    token = create_client_token(data={"sub": user["username"], "user_id": user["id"]})

    # 记录设备登录
    if x_device_id:
        db.upsert_user_device(
            user_id=user["id"],
            device_uuid=x_device_id,
            device_name=body.device_name,
        )

    logger.info(f"用户登录成功: {user['username']}")
    return V2Response(
        msg="登录成功",
        data={
            "token": token,
            "username": user["username"],
            "email": user["email"],
        },
    )
