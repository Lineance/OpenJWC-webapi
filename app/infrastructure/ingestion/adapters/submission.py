from datetime import datetime
from typing import Any

from app.domain.submission.models import SubmissionRecord
from app.infrastructure.storage.lancedb import ArticleFields


class SubmissionAdapter:
    def __init__(self, source_site: str = "用户投稿"):
        self._source_site = source_site

    def convert_one(self, record: SubmissionRecord, review: str = "") -> dict[str, Any]:
        detail_url = (
            record.detail_url
            or f"https://submission.openjwc.local/{record.submission_id}"
        )
        tags = [record.label] if record.label else []
        metadata = {
            "source": "submission",
            "submission_id": record.submission_id,
            "submitter_id": record.submitter_id,
            "detail_url": record.detail_url or "",
            "is_page": record.is_page,
            "label": record.label,
            "review": review or record.review,
            "status": str(record.status),
        }

        return {
            ArticleFields.NEWS_ID: record.submission_id,
            ArticleFields.TITLE: record.title,
            ArticleFields.URL: detail_url,
            ArticleFields.PUBLISH_DATE: record.date,
            ArticleFields.SOURCE_SITE: self._source_site,
            ArticleFields.AUTHOR: record.submitter_id,
            ArticleFields.TAGS: tags,
            ArticleFields.CONTENT_MARKDOWN: record.content_text,
            ArticleFields.CONTENT_TEXT: record.content_text,
            ArticleFields.ATTACHMENTS: record.attachment_urls,
            ArticleFields.CRAWL_VERSION: 1,
            ArticleFields.LAST_UPDATED: datetime.now(),
            ArticleFields.METADATA: metadata,
        }
