from __future__ import annotations

from app.models.base import SemanticModel
from app.models.schemas import (
    BaseModel,
    ChatRequest,
    CreateApiKeyRequest,
    Generic,
    List,
    Message,
    NoticeItem,
    NoticeListResponse,
    Optional,
    T,
    ToggleApiKeyRequest,
    TypeVar,
)

class ResponseModel(SemanticModel, Generic[T]):
    """控制面板通用响应模型"""

    msg: str
    data: Optional[T]
