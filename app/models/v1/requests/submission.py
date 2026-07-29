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
    T,
    ToggleApiKeyRequest,
    TypeVar,
)

class SubmissionContent(SemanticModel):
    """附带的链接"""

    attachment_urls: List[str]
    """正文"""
    text: str

class SubmissionRequest(SemanticModel):
    content: SubmissionContent
    """资讯发布日期"""
    date: str
    """详细url"""
    detail_url: Optional[str] = None
    is_page: bool
    """资讯标签"""
    label: str
    """资讯标题"""
    title: str
