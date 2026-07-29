from __future__ import annotations

from app.models.base import SemanticModel
from app.models.schemas import (
    BaseModel,
    Generic,
    List,
    Optional,
    T,
    TypeVar,
)

class Message(SemanticModel):
    role: str
    content: str

class ChatRequest(SemanticModel):
    notice_ids: Optional[List[str]] = None
    user_query: str
    stream: bool = False
    history: List[Message] = []
