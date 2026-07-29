from __future__ import annotations

from app.infrastructure.storage.lancedb.schema import (
    ARTICLES_TABLE_NAME,
    Any,
    CONTENT_EMBEDDING_DIM,
    TITLE_EMBEDDING_DIM,
    datetime,
    pa,
)

class ArticleFields:
    """Article 表字段名常量，避免硬编码字符串"""

    NEWS_ID = "news_id"
    TITLE = "title"
    PUBLISH_DATE = "publish_date"
    URL = "url"
    SOURCE_SITE = "source_site"
    AUTHOR = "author"
    TAGS = "tags"
    CONTENT_MARKDOWN = "content_markdown"
    CONTENT_TEXT = "content_text"
    TITLE_EMBEDDING = "title_embedding"
    CONTENT_EMBEDDING = "content_embedding"
    CRAWL_VERSION = "crawl_version"
    LAST_UPDATED = "last_updated"
    METADATA = "metadata"
    ATTACHMENTS = "attachments"
