from __future__ import annotations

from app.infrastructure.ingestion.adapters.crawler import (
    Any,
    ArticleFields,
    DEFAULT_VALUES,
    FIELD_MAPPING,
    datetime,
    extract_first_sentence,
    json,
    logger,
    logging,
    normalize_datetime,
)

class CrawlerValidationMixin:
    """封装 CrawlerAdapter 的单一职责方法。"""

    def _generate_news_id(self, data: dict[str, Any]) -> str:
        """生成新闻 ID"""
        import hashlib

        url = data.get(ArticleFields.URL, "")
        if not url:

            title = data.get(ArticleFields.TITLE, "")
            url = title or str(datetime.now().timestamp())

        return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]

    def validate_conversion(self, raw_data: dict[str, Any]) -> tuple[bool, list[str]]:
        """验证转换是否成功"""
        errors = []

        required_fields = ["title", "url", "content"]
        errors = [
            f"Missing required field: {field}"
            for field in required_fields
            if field not in raw_data or not raw_data[field]
        ]

        if "url" in raw_data:
            from ..validators import validate_url

            if not validate_url(raw_data["url"]):
                errors.append("Invalid URL format")

        return len(errors) == 0, errors
