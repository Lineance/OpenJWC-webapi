from __future__ import annotations

from app.models.base import SemanticModel
from app.models.v2_schemas import (
    BaseModel,
    List,
    LoginRequest,
    Optional,
    RegisterRequest,
    UnbindRequest,
)

class LoginData(SemanticModel):
    """登录成功返回的数据"""
    token: str
    username: str
    email: str
