from __future__ import annotations

from app.infrastructure.ingestion.validators import (
    ALLOWED_DOMAINS,
    ALLOWED_SCHEMES,
    Any,
    ContentValidator,
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

class DocumentValidator:
    """文档综合验证器"""

    def __init__(
        self,
        required_fields: list[str] | None = None,
        url_validator: URLValidator | None = None,
        content_validator: ContentValidator | None = None,
    ) -> None:
        """初始化文档验证器"""
        self._required_fields = required_fields or REQUIRED_FIELDS
        self._url_validator = url_validator or URLValidator()
        self._content_validator = content_validator or ContentValidator()
        self._title_validator = ContentValidator(
            min_length=MIN_TITLE_LENGTH,
            max_length=MAX_TITLE_LENGTH,
        )

    def validate(self, document: dict[str, Any]) -> ValidationResult:
        """验证文档"""
        result = ValidationResult()

        if not document:
            result.add_error("Document is empty")
            return result

        if not isinstance(document, dict):
            result.add_error(f"Document must be dict, got {type(document).__name__}")
            return result

        for _field in self._required_fields:
            if _field not in document or document[_field] is None:
                result.add_error(f"Missing required field: {_field}")
            elif isinstance(document[_field], str) and not document[_field].strip():
                result.add_error(f"Required field '{_field}' is empty")

        if "url" in document and document["url"]:
            url_result = self._url_validator.validate(document["url"])
            result.merge(url_result)

        if "title" in document and document["title"]:
            title_result = self._title_validator.validate(document["title"])
            for error in title_result.errors:
                result.add_error(f"Title: {error}")

        if "content_text" in document and document["content_text"]:
            content_result = self._content_validator.validate(document["content_text"])
            for error in content_result.errors:
                result.add_error(f"Content: {error}")

        if "news_id" in document and document["news_id"]:
            news_id = document["news_id"]
            if not re.match(r"^[a-zA-Z0-9_\-]+$", news_id):
                result.add_warning(f"news_id contains special characters: {news_id}")

        return result
