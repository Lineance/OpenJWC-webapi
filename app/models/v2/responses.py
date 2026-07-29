from __future__ import annotations

from app.models.base import SemanticModel
from app.models.v2_schemas import (
    BaseModel,
    DeviceItem,
    DeviceListData,
    List,
    LoginData,
    LoginRequest,
    Optional,
    RegisterRequest,
    UnbindRequest,
)

class V2Response(SemanticModel):
    """v2 通用响应模型"""
    msg: str
    data: Optional[dict] = None

class V2DetailResponse(SemanticModel):
    """v2 detail 响应模型（用于解绑等简单操作）"""
    detail: str
