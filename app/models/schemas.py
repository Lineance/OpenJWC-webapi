from typing import List, Optional, Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T")


class Message(BaseModel):
    role: str  # 比如 "user" 或 "assistant"
    content: str  # 具体的对话内容


# 定义聊天请求格式的结构 (Schema)
class ChatRequest(BaseModel):
    notice_id: str
    user_query: str
    stream: bool = False
    history: List[Message] = []


# 定义创建apikey请求格式的结构
class CreateApiKeyRequest(BaseModel):
    owner_name: str
    max_devices: int


# 定义启停apikey请求格式的结构
class ToggleApiKeyRequest(BaseModel):
    is_active: bool


# 定义单条资讯的结构
class NoticeItem(BaseModel):
    id: str
    label: Optional[str] = None
    title: str
    date: str
    detail_url: str
    is_page: bool


# 定义列表接口的整体返回格式
class NoticeListResponse(BaseModel):
    status: str
    page: int
    size: int
    total_returned: int  # 本次实际返回的条数
    data: List[NoticeItem]


class ResponseModel(BaseModel, Generic[T]):
    """
    控制面板通用响应模型
    """

    msg: str
    data: Optional[T]


class SubmissionContent(BaseModel):
    """附带的链接"""

    attachment_urls: List[str]
    """正文"""
    text: str


class SubmissionRequest(BaseModel):
    content: SubmissionContent
    """资讯发布日期"""
    date: str
    """详细url"""
    detail_url: str
    is_page: bool
    """资讯标签"""
    label: str
    """资讯标题"""
    title: str
