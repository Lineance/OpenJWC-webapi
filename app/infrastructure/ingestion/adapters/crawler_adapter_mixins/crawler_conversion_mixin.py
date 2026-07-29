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

class CrawlerConversionMixin:
    """封装 CrawlerAdapter 的单一职责方法。"""

    def convert_one(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """转换单条爬虫数据"""
        result: dict[str, Any] = {}

        normalized_raw = self._normalize_input(raw_data)

        for src_field, dst_field in self._field_mapping.items():
            if src_field in normalized_raw:
                value = normalized_raw[src_field]

                if src_field == "content" and isinstance(value, dict):
                    value = value.get("text", "")

                if dst_field == ArticleFields.PUBLISH_DATE:
                    value = normalize_datetime(value)

                result[dst_field] = value

        content_text = normalized_raw.get(ArticleFields.CONTENT_TEXT)
        if isinstance(content_text, str) and content_text.strip():
            result[ArticleFields.CONTENT_TEXT] = content_text

        attachments = normalized_raw.get(ArticleFields.ATTACHMENTS)
        if isinstance(attachments, list):
            result[ArticleFields.ATTACHMENTS] = [
                str(item) for item in attachments if item
            ]

        label = normalized_raw.get("label")
        tags = result.get(ArticleFields.TAGS, [])
        if not isinstance(tags, list):
            tags = [str(tags)] if tags else []
        if label:
            tags.append(str(label))
        if tags:

            dedup_tags = list(dict.fromkeys(tag for tag in tags if str(tag).strip()))
            result[ArticleFields.TAGS] = dedup_tags

        if ArticleFields.TITLE not in result or not result[ArticleFields.TITLE]:
            content_markdown = result.get(ArticleFields.CONTENT_MARKDOWN, "")
            if content_markdown:

                fallback_title = extract_first_sentence(
                    content_markdown, is_markdown=True, max_title_length=100
                )
                if fallback_title:
                    result[ArticleFields.TITLE] = fallback_title
                    logger.info(f"使用回退标题: {fallback_title[:50]}...")

        for field, default in self._default_values.items():
            if field not in result or result[field] is None:
                result[field] = default

        result[ArticleFields.METADATA] = self._build_metadata(
            raw_data=raw_data,
            existing_metadata=result.get(ArticleFields.METADATA),
        )

        if ArticleFields.NEWS_ID not in result or not result[ArticleFields.NEWS_ID]:
            result[ArticleFields.NEWS_ID] = self._generate_news_id(result)

        result[ArticleFields.LAST_UPDATED] = datetime.now()

        return result

    def _normalize_input(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """将不同爬虫输出对齐到统一输入字段集合。"""
        normalized = dict(raw_data)

        if not normalized.get("url") and normalized.get("detail_url"):
            normalized["url"] = normalized.get("detail_url")
        if not normalized.get("publish_date") and normalized.get("date"):
            normalized["publish_date"] = normalized.get("date")

        content_value = normalized.get("content")
        if isinstance(content_value, dict):
            text_value = content_value.get("text")
            if isinstance(text_value, str):
                normalized[ArticleFields.CONTENT_TEXT] = text_value

                if text_value.strip() and not normalized.get("content_markdown"):
                    normalized["content_markdown"] = text_value

            attachment_urls = content_value.get("attachment_urls")
            if isinstance(attachment_urls, list):
                normalized[ArticleFields.ATTACHMENTS] = attachment_urls
        elif isinstance(content_value, str):
            if content_value.strip() and not normalized.get("content_markdown"):
                normalized["content_markdown"] = content_value
                normalized[ArticleFields.CONTENT_TEXT] = content_value

        markdown_value = normalized.get("markdown")
        if isinstance(markdown_value, str) and markdown_value.strip():
            normalized["content_markdown"] = markdown_value
            normalized.setdefault(ArticleFields.CONTENT_TEXT, markdown_value)

        return normalized

    def _build_metadata(
        self, raw_data: dict[str, Any], existing_metadata: Any
    ) -> dict[str, Any]:
        """合并元数据并保留关键原始字段。"""
        metadata: dict[str, Any] = {}
        if isinstance(existing_metadata, dict):
            metadata.update(existing_metadata)

        for key in ["detail_url", "date", "label", "is_page", "success", "error"]:
            if key in raw_data and raw_data[key] is not None:
                metadata[key] = raw_data[key]

        return metadata
