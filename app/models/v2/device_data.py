from __future__ import annotations

from app.models.base import SemanticModel
from app.models.v2_schemas import (
    BaseModel,
    List,
    LoginData,
    LoginRequest,
    Optional,
    RegisterRequest,
    UnbindRequest,
)

class DeviceItem(SemanticModel):
    """设备列表中的单个设备"""
    device_uuid: str
    device_name: str
    last_login: str

class DeviceListData(SemanticModel):
    """设备列表返回数据"""
    devices: List[DeviceItem]
