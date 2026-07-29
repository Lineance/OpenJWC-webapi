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
    SubmissionContent,
    SubmissionRequest,
    T,
    ToggleApiKeyRequest,
    TypeVar,
    UpdateSettingModel,
    UpdateSettingRequest,
    UpdateStatusRequest,
)

class SemanticSearchRequest(SemanticModel):
    query: str
    top_k: int = 5
    min_similarity: float = None
