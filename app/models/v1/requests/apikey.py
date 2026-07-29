from __future__ import annotations

from app.models.base import SemanticModel
from app.models.schemas import (
    BaseModel,
    ChatRequest,
    Generic,
    List,
    Message,
    Optional,
    T,
    TypeVar,
)

class CreateApiKeyRequest(SemanticModel):
    owner_name: str
    max_devices: int

class ToggleApiKeyRequest(SemanticModel):
    is_active: bool
