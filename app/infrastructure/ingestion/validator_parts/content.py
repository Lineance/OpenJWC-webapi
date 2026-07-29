from __future__ import annotations

from app.infrastructure.ingestion.validators import (
    ALLOWED_DOMAINS,
    ALLOWED_SCHEMES,
    Any,
    MAX_CONTENT_LENGTH,
    MAX_TITLE_LENGTH,
    MIN_CONTENT_LENGTH,
    MIN_TITLE_LENGTH,
    REQUIRED_FIELDS,
    URLValidator,
    ValidationResult,
    dataclass,
    field,
    logger,
    logging,
    re,
    urlparse,
)

class ContentValidator:
    """内容验证器"""

    def __init__(
        self,
        min_length: int = MIN_CONTENT_LENGTH,
        max_length: int = MAX_CONTENT_LENGTH,
        check_encoding: bool = True,
    ) -> None:
        """初始化内容验证器"""
        self._min_length = min_length
        self._max_length = max_length
        self._check_encoding = check_encoding

    def validate(self, content: str) -> ValidationResult:
        """验证内容"""
        result = ValidationResult()

        if not content:
            result.add_error("Content is empty")
            return result

        if not isinstance(content, str):
            result.add_error(f"Content must be string, got {type(content).__name__}")
            return result

        content_length = len(content)
        if content_length < self._min_length:
            result.add_error(f"Content too short: {content_length} < {self._min_length}")
        elif content_length > self._max_length:
            result.add_error(f"Content too long: {content_length} > {self._max_length}")

        if self._check_encoding:
            try:
                content.encode("utf-8")
            except UnicodeEncodeError as e:
                result.add_error(f"Invalid UTF-8 encoding: {e}")

        if content.strip() == "":
            result.add_error("Content contains only whitespace")

        return result
