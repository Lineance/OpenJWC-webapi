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
)

class UpdateStatusRequest(SemanticModel):
    action: str
    review: str
