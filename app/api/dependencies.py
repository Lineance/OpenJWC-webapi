from typing import Tuple
from fastapi import Depends, HTTPException, Header, status
from fastapi.security import (
    HTTPBearer,
    HTTPAuthorizationCredentials,
    OAuth2PasswordBearer,
)
from app.services.sql_db_service import db
from app.utils.logging_manager import setup_logger
import jwt
from app.core.security import SECRET_KEY, ALGORITHM

logger = setup_logger("auth_logs")

# HTTPBearer 是 FastAPI 内置的工具，专门用来解析请求头里的 Authorization: Bearer <token>
security = HTTPBearer()


async def verify_api_key(
    # 自动提取 Authorization Header 中的 Token
    credentials: HTTPAuthorizationCredentials = Depends(security),
    # 强制要求请求头中必须带上 X-Device-ID
    x_device_id: str = Header(..., description="移动端设备的唯一标识 UUID"),
) -> str:
    """
    核心鉴权依赖：
    如果通过，返回提取到的 token 字符串；
    如果失败，直接在这里抛出 HTTP 异常，请求会被立刻拦截。
    """
    token = credentials.credentials
    is_valid, error_msg = db.validate_and_use_key(token, x_device_id)

    if not is_valid:
        logger.warning(
            f"鉴权拦截 - Token: {token[:8]}... 设备: {x_device_id} 原因: {error_msg}"
        )
        # 抛出 401 或 403 错误，前端会收到这个状态码
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=error_msg,
        )

    logger.debug(f"鉴权通过 - Token: {token[:8]}... 设备: {x_device_id}")
    return token


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/admin/login")


def get_current_admin(token: str = Depends(oauth2_scheme)):
    """
    管理员接口专属拦截器。
    任何想保护的接口，只要加上 Depends(get_current_admin) 即可。
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="认证失败或Token已过期，请重新登录",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # 尝试解密 JWT
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        # admin_user = db.get_user(username)
        # if not admin_user: raise credentials_exception
        return username  # 返回解析出的管理员用户名
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="登录已过期")
    except jwt.InvalidTokenError:
        raise credentials_exception


async def verify_api_key_and_device(
    # 自动提取 Authorization Header 中的 Token
    credentials: HTTPAuthorizationCredentials = Depends(security),
    # 强制要求请求头中必须带上 X-Device-ID
    x_device_id: str = Header(..., description="移动端设备的唯一标识 UUID"),
) -> Tuple[str, str]:
    """检查apikey和device是否存在绑定关系"""
    token = credentials.credentials
    is_valid, error_msg = db.validate_key_and_device(token, x_device_id)

    if not is_valid:
        logger.warning(
            f"鉴权拦截 - Token: {token[:8]}... 设备: {x_device_id} 原因: {error_msg}"
        )
        # 抛出 401 或 403 错误，前端会收到这个状态码
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=error_msg,
        )

    logger.debug(f"鉴权通过 - Token: {token[:8]}... 设备: {x_device_id}")
    return token, x_device_id
