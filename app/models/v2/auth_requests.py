from __future__ import annotations

from app.models.base import SemanticModel
from app.models.v2_schemas import (
    BaseModel,
    List,
    Optional,
)

class RegisterRequest(SemanticModel):
    """注册请求"""
    username: str
    password_hash: str
    email: str

class LoginRequest(SemanticModel):
    """登录请求"""
    account: str
    password_hash: str
    device_name: str
