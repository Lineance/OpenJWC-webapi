"""Data Validators - 数据验证模块"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

ALLOWED_SCHEMES = frozenset(["http", "https"])
ALLOWED_DOMAINS = frozenset(["seu.edu.cn"])

MIN_CONTENT_LENGTH = 10
MAX_CONTENT_LENGTH = 1_000_000
MIN_TITLE_LENGTH = 1
MAX_TITLE_LENGTH = 500

REQUIRED_FIELDS = ["news_id", "title", "url"]

from app.infrastructure.ingestion.validator_parts.result import (
    ValidationResult,
)

from app.infrastructure.ingestion.validator_parts.url import (
    URLValidator,
)

from app.infrastructure.ingestion.validator_parts.content import (
    ContentValidator,
)

from app.infrastructure.ingestion.validator_parts.document import (
    DocumentValidator,
)

def validate_url(url: str) -> bool:
    """快速验证 URL"""
    validator = URLValidator()
    return validator.validate(url).is_valid

def validate_content(content: str) -> bool:
    """快速验证内容"""
    validator = ContentValidator()
    return validator.validate(content).is_valid

def validate_document(document: dict[str, Any]) -> ValidationResult:
    """验证文档"""
    validator = DocumentValidator()
    return validator.validate(document)

def is_valid_document(document: dict[str, Any]) -> bool:
    """快速检查文档是否有效"""
    return validate_document(document).is_valid
