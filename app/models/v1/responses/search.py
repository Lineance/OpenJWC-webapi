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
    ResponseModel,
    SemanticSearchRequest,
    SubmissionContent,
    SubmissionRequest,
    T,
    ToggleApiKeyRequest,
    TypeVar,
    UpdateSettingModel,
    UpdateSettingRequest,
    UpdateStatusRequest,
)

class SemanticSearchResult(SemanticModel):
    id: str
    label: Optional[str] = None
    title: str
    date: str
    detail_url: str
    is_page: bool
    similarity_score: float
    distance: float
