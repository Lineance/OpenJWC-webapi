# app/core/security.py
import os
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
from passlib.context import CryptContext

from app.core.config import ACCESS_TOKEN_EXPIRE_MINUTES

SECRET_KEY = os.getenv(
    "OPENJWC_SECRET_KEY",
    os.getenv("SECRET_KEY", "your-super-secret-key-change-this-in-production"),
)
ALGORITHM = os.getenv("OPENJWC_JWT_ALGORITHM", "HS256")

# 密码加密工具
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """核对明文密码和数据库里加密后的密码是否一致"""
    password_byte_enc = plain_password.encode("utf-8")
    hashed_password_byte_enc = hashed_password.encode("utf-8")
    return bcrypt.checkpw(password_byte_enc, hashed_password_byte_enc)


def get_password_hash(password: str) -> str:
    """将明文密码变成哈希值（用于初始化管理员账号）"""
    pwd_bytes = password.encode("utf-8")
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(pwd_bytes, salt)
    return hashed_password.decode("utf-8")


def create_access_token(data: dict) -> str:
    """生成 JWT 认证token"""
    to_encode = data.copy()
    # 设置过期时间
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    # 用 SECRET_KEY 进行签名加密
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
