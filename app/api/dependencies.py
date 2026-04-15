from typing import Tuple
from fastapi import Depends, HTTPException, Header, status
from fastapi.security import (
    HTTPBearer,
    HTTPAuthorizationCredentials,
    OAuth2PasswordBearer,
)
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.utils.logging_manager import setup_logger
import jwt
from app.core.security import SECRET_KEY, ALGORITHM

logger = setup_logger("auth_logs")

security = HTTPBearer(auto_error=False)


# INFO: 用来对客户端进行鉴权的依赖，假如用户apikey异常则此处会直接拦截。
# 假如apikey存在且设备数未达上限，但是用户设备没有绑定，则自动对其进行绑定。
async def verify_api_key(
    # 自动提取 Authorization Header 中的 Token
    credentials: HTTPAuthorizationCredentials = Depends(security),
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


async def optional_verify_api_key(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    x_device_id: str = Header(..., description="移动端设备的唯一标识 UUID"),
) -> str:
    """
    可选鉴权，如果系统设置了true则鉴权，否则不鉴权。
    """
    if credentials:
        token = credentials.credentials
        is_valid, error_msg = db.validate_and_use_key(token, x_device_id)
    else:
        token = ""
        is_valid = False
        error_msg = "未接受到有效token"

    if not is_valid and db.get_system_setting("notices_auth") != "0":
        logger.warning(
            f"鉴权拦截 - Token: {token[:8]}... 设备: {x_device_id} 原因: {error_msg}"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=error_msg,
        )

    logger.debug(f"鉴权通过 - Token: {token[:8]}... 设备: {x_device_id}")
    return token


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/admin/auth/login")


def verify_admin_token(
    token: str = Depends(oauth2_scheme),
    x_client_version: str = Header(default="1.0.0"),
    x_request_id: str = Header(default=None),
):
    """
    管理员接口的全局鉴权依赖。
    如果 Token 错误或过期，直接抛出 401 错误。
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={"msg": "无效的凭据或Token已过期", "data": None},
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        # 尝试解码 Token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        # 将解析出的信息传递给下游的接口函数
        return {
            "username": username,
            "x_client_version": x_client_version,
            "x_request_id": x_request_id,
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail={"msg": "Token 已过期，请重新登录"})
    except jwt.PyJWTError:
        raise credentials_exception


# INFO: 用来鉴权设备操作的专用依赖，只检查apikey和设备之间的关系是否存在。不进行自动绑定。
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


# ==================== v2 客户端账密鉴权 ====================

async def verify_client_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    x_device_id: str = Header(default=None),
) -> dict:
    """
    v2 客户端接口的全局鉴权依赖。
    三重验证：
    1. JWT 签名和过期验证
    2. 设备ID一致性检查（Token中的 device_uuid vs 请求头 X-Device-ID）
    3. 数据库验证（用户存在性 + 设备-Token绑定关系）
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="未提供认证信息",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    try:
        # ====== 第一重：JWT 签名和过期验证 ======
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        user_id: int = payload.get("user_id")
        token_device_uuid: str = payload.get("device_uuid")
        if username is None or user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="无效的Token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # ====== 第二重：设备ID一致性检查 ======
        if token_device_uuid and x_device_id and token_device_uuid != x_device_id:
            logger.warning(
                f"v2鉴权拦截 - Token设备[{token_device_uuid[:8]}...]与请求设备[{x_device_id[:8]}...]不匹配"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="设备ID不匹配，请重新登录",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # ====== 第三重：数据库验证 ======
        # 3.1 用户存在性验证
        user = db.get_user_by_id(user_id)
        if not user:
            logger.warning(f"v2鉴权拦截 - user_id={user_id}在数据库中不存在")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="用户不存在",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # 3.2 设备-Token绑定关系验证
        if token_device_uuid:
            if not db.check_device_token_binding(user_id, token_device_uuid, token):
                logger.warning(
                    f"v2鉴权拦截 - 用户[{user_id}]设备[{token_device_uuid[:8]}...]未授权或Token已失效"
                )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="设备未授权或Token已失效，请重新登录",
                    headers={"WWW-Authenticate": "Bearer"},
                )

        return {"username": user["username"], "user_id": user["id"], "device_uuid": token_device_uuid}
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token已过期，请重新登录",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效的Token",
            headers={"WWW-Authenticate": "Bearer"},
        )

