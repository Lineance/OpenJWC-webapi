"""Article LanceModel - LanceDB 数据模型定义"""

import contextlib
import datetime
from typing import Any, cast

import pyarrow as pa
from lancedb.pydantic import LanceModel, Vector

class Article(LanceModel):
    """新闻文章 LanceDB 模型"""

    news_id: str
    title: str
    publish_date: datetime.datetime | None = None
    url: str
    source_site: str = ""
    author: str = ""
    tags: list[str] = []

    content_markdown: str = ""
    content_text: str = ""

    title_embedding: Vector(384)

    content_embedding: Vector(1024)

    crawl_version: int = 1
    last_updated: datetime.datetime

    metadata: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Article":
        """从字典创建 Article 实例"""

        if "publish_date" in data and isinstance(data["publish_date"], datetime.datetime):
            data["publish_date"] = data["publish_date"]

        if "last_updated" in data and isinstance(data["last_updated"], datetime.datetime):
            data["last_updated"] = data["last_updated"]

        if "title_embedding" in data and isinstance(data["title_embedding"], list):
            data["title_embedding"] = data["title_embedding"]

        if "content_embedding" in data and isinstance(data["content_embedding"], list):
            data["content_embedding"] = data["content_embedding"]

        if "metadata" in data and isinstance(data["metadata"], dict):
            import json

            data["metadata"] = json.dumps(data["metadata"], ensure_ascii=False)

        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典"""
        result = cast("dict[str, Any]", self.dict())

        if result["publish_date"]:
            result["publish_date"] = result["publish_date"]

        if result["last_updated"]:
            result["last_updated"] = result["last_updated"]

        if result["metadata"]:
            import json

            with contextlib.suppress(BaseException):
                result["metadata"] = json.loads(result["metadata"])

        return result

    @classmethod
    def get_schema(cls) -> pa.Schema:
        """获取 Arrow Schema"""
        return cls.to_arrow_schema()

    @classmethod
    def get_vector_fields(cls) -> dict[str, int]:
        """获取向量字段及其维度"""
        return {
            "title_embedding": 384,
            "content_embedding": 1024,
        }

    @classmethod
    def get_indexable_fields(cls) -> list[str]:
        """获取可索引字段列表"""
        return [
            "news_id",
            "title",
            "publish_date",
            "source_site",
            "author",
            "tags",
            "crawl_version",
        ]

    @classmethod
    def get_searchable_fields(cls) -> list[str]:
        """获取可搜索字段列表 (全文搜索)"""
        return [
            "title",
            "content_text",
            "source_site",
            "author",
        ]

    def validate_data(self) -> tuple[bool, list[str]]:
        """验证 Article 数据"""
        errors = []

        if not self.news_id:
            errors.append("news_id is required")
        if not self.title:
            errors.append("title is required")
        if not self.url:
            errors.append("url is required")

        if self.url:
            from urllib.parse import urlparse

            try:
                parsed = urlparse(self.url)
                if not parsed.scheme or not parsed.netloc:
                    errors.append("Invalid URL format")
            except Exception:
                errors.append("Invalid URL format")

        if self.title_embedding and len(self.title_embedding) != 384:
            errors.append(
                f"title_embedding dimension mismatch: expected 384, got {len(self.title_embedding)}"
            )

        if self.content_embedding and len(self.content_embedding) != 1024:
            errors.append(
                f"content_embedding dimension mismatch: expected 1024, got {len(self.content_embedding)}"
            )

        return len(errors) == 0, errors

from app.infrastructure.retrieval.schema.article_query import (
    ArticleQuery,
)
