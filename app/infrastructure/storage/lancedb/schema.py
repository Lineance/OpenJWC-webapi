"""Schema Definitions - LanceDB 表结构定义"""

from datetime import datetime
from typing import Any

import pyarrow as pa

TITLE_EMBEDDING_DIM = 384

CONTENT_EMBEDDING_DIM = 1024

ARTICLES_TABLE_NAME = "articles"
from app.infrastructure.storage.lancedb.schema_parts.article_fields import (
    ArticleFields,
)

def get_article_schema() -> pa.Schema:
    """获取 Article 表的 PyArrow Schema"""
    return pa.schema(
        [

            pa.field(ArticleFields.NEWS_ID, pa.string(), nullable=False),
            pa.field(ArticleFields.TITLE, pa.string(), nullable=False),
            pa.field(
                ArticleFields.PUBLISH_DATE, pa.timestamp("us", tz="UTC"), nullable=True
            ),
            pa.field(ArticleFields.URL, pa.string(), nullable=False),
            pa.field(ArticleFields.SOURCE_SITE, pa.string(), nullable=True),
            pa.field(ArticleFields.AUTHOR, pa.string(), nullable=True),

            pa.field(ArticleFields.TAGS, pa.list_(pa.string()), nullable=True),

            pa.field(ArticleFields.CONTENT_MARKDOWN, pa.string(), nullable=True),
            pa.field(ArticleFields.CONTENT_TEXT, pa.string(), nullable=True),

            pa.field(ArticleFields.ATTACHMENTS, pa.list_(pa.string()), nullable=True),

            pa.field(
                ArticleFields.TITLE_EMBEDDING,
                pa.list_(pa.float32(), TITLE_EMBEDDING_DIM),
                nullable=False,
            ),
            pa.field(
                ArticleFields.CONTENT_EMBEDDING,
                pa.list_(pa.float32(), CONTENT_EMBEDDING_DIM),
                nullable=False,
            ),

            pa.field(ArticleFields.CRAWL_VERSION, pa.int32(), nullable=False),
            pa.field(
                ArticleFields.LAST_UPDATED, pa.timestamp("us", tz="UTC"), nullable=False
            ),

            pa.field(ArticleFields.METADATA, pa.string(), nullable=True),
        ]
    )

class ArticleRecord:
    """Article 记录的数据类，用于类型提示和构建记录"""

    def __init__(
        self,
        news_id: str,
        title: str,
        url: str,
        content_text: str,
        title_embedding: list[float],
        content_embedding: list[float],
        crawl_version: int = 1,
        publish_date: datetime | None = None,
        source_site: str | None = None,
        author: str | None = None,
        tags: list[str] | None = None,
        content_markdown: str | None = None,
        last_updated: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        attachments: list[str] | None = None,
    ) -> None:
        self.news_id = news_id
        self.title = title
        self.publish_date = publish_date
        self.url = url
        self.source_site = source_site
        self.author = author
        self.tags = tags or []
        self.content_markdown = content_markdown
        self.content_text = content_text
        self.title_embedding = title_embedding
        self.content_embedding = content_embedding
        self.crawl_version = crawl_version
        self.last_updated = last_updated or datetime.now()
        self.metadata = metadata
        self.attachments = attachments or []

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式，用于写入 LanceDB"""
        import json

        return {
            ArticleFields.NEWS_ID: self.news_id,
            ArticleFields.TITLE: self.title,
            ArticleFields.PUBLISH_DATE: self.publish_date,
            ArticleFields.URL: self.url,
            ArticleFields.SOURCE_SITE: self.source_site,
            ArticleFields.AUTHOR: self.author,
            ArticleFields.TAGS: self.tags,
            ArticleFields.CONTENT_MARKDOWN: self.content_markdown,
            ArticleFields.CONTENT_TEXT: self.content_text,
            ArticleFields.TITLE_EMBEDDING: self.title_embedding,
            ArticleFields.CONTENT_EMBEDDING: self.content_embedding,
            ArticleFields.CRAWL_VERSION: self.crawl_version,
            ArticleFields.LAST_UPDATED: self.last_updated,
            ArticleFields.METADATA: json.dumps(self.metadata, ensure_ascii=False)
            if self.metadata
            else None,
            ArticleFields.ATTACHMENTS: self.attachments,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ArticleRecord":
        """从字典创建 ArticleRecord 实例"""
        import contextlib
        import json

        metadata_raw = data.get(ArticleFields.METADATA)
        metadata: dict[str, Any] | None = None
        if isinstance(metadata_raw, str):
            with contextlib.suppress(json.JSONDecodeError):
                metadata = json.loads(metadata_raw)
        elif isinstance(metadata_raw, dict):
            metadata = metadata_raw

        content_text = data.get(ArticleFields.CONTENT_TEXT, "") or ""
        title_embedding = data.get(ArticleFields.TITLE_EMBEDDING) or []
        content_embedding = data.get(ArticleFields.CONTENT_EMBEDDING) or []

        return cls(
            news_id=data[ArticleFields.NEWS_ID],
            title=data[ArticleFields.TITLE],
            publish_date=data.get(ArticleFields.PUBLISH_DATE),
            url=data[ArticleFields.URL],
            source_site=data.get(ArticleFields.SOURCE_SITE),
            author=data.get(ArticleFields.AUTHOR),
            tags=data.get(ArticleFields.TAGS) or [],
            content_markdown=data.get(ArticleFields.CONTENT_MARKDOWN),
            content_text=content_text,
            title_embedding=title_embedding,
            content_embedding=content_embedding,
            crawl_version=data.get(ArticleFields.CRAWL_VERSION, 1),
            last_updated=data.get(ArticleFields.LAST_UPDATED),
            metadata=metadata,
            attachments=data.get(ArticleFields.ATTACHMENTS) or [],
        )
from app.infrastructure.storage.lancedb.schema_parts.index_config import (
    IndexConfig,
)
