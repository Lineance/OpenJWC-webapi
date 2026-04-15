from pydantic import BaseModel
from typing import List, Optional


# ==================== 请求模型 ====================

class RegisterRequest(BaseModel):
    """注册请求"""
    username: str
    password_hash: str  # 客户端计算好的 SHA256
    email: str


class LoginRequest(BaseModel):
    """登录请求"""
    account: str  # 用户名或邮箱
    password_hash: str  # 客户端计算好的 SHA256
    device_name: str


class UnbindRequest(BaseModel):
    """解绑设备请求"""
    device_uuid: str  # 要解绑的目标设备ID


# ==================== 响应模型 ====================

class LoginData(BaseModel):
    """登录成功返回的数据"""
    token: str
    username: str
    email: str


class DeviceItem(BaseModel):
    """设备列表中的单个设备"""
    device_uuid: str
    device_name: str
    last_login: str


class DeviceListData(BaseModel):
    """设备列表返回数据"""
    devices: List[DeviceItem]


class V2Response(BaseModel):
    """v2 通用响应模型"""
    msg: str
    data: Optional[dict] = None


class V2DetailResponse(BaseModel):
    """v2 detail 响应模型（用于解绑等简单操作）"""
    detail: str
