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
    UpdateStatusRequest,
)

class UpdateSettingModel(SemanticModel):
    key: str
    value: str

class UpdateSettingRequest(SemanticModel):
    settings: List[UpdateSettingModel]
