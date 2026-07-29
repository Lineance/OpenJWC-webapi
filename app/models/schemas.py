from typing import List, Optional, Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T")
from app.models.v1.requests.chat import (
    Message,
    ChatRequest,
)
from app.models.v1.requests.apikey import (
    CreateApiKeyRequest,
    ToggleApiKeyRequest,
)
from app.models.v1.responses.notice import (
    NoticeItem,
    NoticeListResponse,
)
from app.models.v1.responses.common import (
    ResponseModel,
)
from app.models.v1.requests.submission import (
    SubmissionContent,
    SubmissionRequest,
)
from app.models.v1.requests.review import (
    UpdateStatusRequest,
)
from app.models.v1.requests.settings import (
    UpdateSettingModel,
    UpdateSettingRequest,
)
from app.models.v1.requests.search import (
    SemanticSearchRequest,
)
from app.models.v1.responses.search import (
    SemanticSearchResult,
)
