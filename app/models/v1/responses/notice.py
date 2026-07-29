from __future__ import annotations

from app.models.base import SemanticModel
from app.models.schemas import (
    BaseModel,
    ChatRequest,
    CreateApiKeyRequest,
    Generic,
    List,
    Message,
    Optional,
    T,
    ToggleApiKeyRequest,
    TypeVar,
)

class NoticeItem(SemanticModel):
    id: str
    label: Optional[str] = None
    title: str
    date: str
    detail_url: str
    is_page: bool

class NoticeListResponse(SemanticModel):
    status: str
    page: int
    size: int
    total_returned: int
    data: List[NoticeItem]
