from __future__ import annotations

from app.models.base import SemanticModel
from app.models.v2_schemas import (
    BaseModel,
    List,
    LoginRequest,
    Optional,
    RegisterRequest,
)

class UnbindRequest(SemanticModel):
    """解绑设备请求"""
    device_uuid: str
