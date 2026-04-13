"""
Article LanceModel - LanceDB 数据模型定义

定义用于 LanceDB 检索的 Article 模型，支持向量搜索和混合检索。

Responsibilities:
    - 定义 LanceDB 表结构
    - 支持向量索引和全文索引
    - 提供查询和过滤接口
"""

import contextlib
import datetime
from typing import Any, cast

import pyarrow as pa
from lancedb.pydantic import LanceModel, Vector


class Article(LanceModel):  # type: ignore[misc]
    """
    新闻文章 LanceDB 模型

    对应 LanceDB 表中的一行记录，支持向量搜索和混合检索。

    Fields:
        news_id: 新闻唯一标识符 (主键)
        title: 标题
        publish_date: 发布时间
        url: 原文链接
        source_site: 来源网站
        author: 作者
        tags: 标签列表
        content_markdown: 原始 Markdown 内容
        content_text: 纯文本内容 (用于向量化)
        title_embedding: 标题向量 (384 维)
        content_embedding: 正文向量 (1024 维)
        crawl_version: 抓取版本
        last_updated: 最后更新时间
        metadata: 扩展元数据 (JSON)
    """

    # 主键和基本字段
    news_id: str
    title: str
    publish_date: datetime.datetime | None = None
    url: str
    source_site: str = ""
    author: str = ""
    tags: list[str] = []

    # 内容字段
    content_markdown: str = ""
    content_text: str = ""

    # 向量字段
    # paraphrase-multilingual-MiniLM-L12-v2
    title_embedding: Vector(384)  # type: ignore
    # BAAI/bge-large-zh
    content_embedding: Vector(1024)  # type: ignore

    # 版本控制
    crawl_version: int = 1
    last_updated: datetime.datetime

    # 元数据
    metadata: str | None = None

    # =========================================================================
    # 类方法
    # =========================================================================

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Article":
        """
        从字典创建 Article 实例

        Args:
            data: 包含 Article 字段的字典

        Returns:
            Article 实例
        """
        # 处理日期字段
        if "publish_date" in data and isinstance(data["publish_date"], datetime.datetime):
            data["publish_date"] = data["publish_date"]

        if "last_updated" in data and isinstance(data["last_updated"], datetime.datetime):
            data["last_updated"] = data["last_updated"]

        # 处理向量字段
        if "title_embedding" in data and isinstance(data["title_embedding"], list):
            data["title_embedding"] = data["title_embedding"]

        if "content_embedding" in data and isinstance(data["content_embedding"], list):
            data["content_embedding"] = data["content_embedding"]

        # 处理元数据
        if "metadata" in data and isinstance(data["metadata"], dict):
            import json

            data["metadata"] = json.dumps(data["metadata"], ensure_ascii=False)

        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        """
        转换为字典

        Returns:
            包含 Article 字段的字典
        """
        result = cast("dict[str, Any]", self.dict())

        # 处理日期字段
        if result["publish_date"]:
            result["publish_date"] = result["publish_date"]

        if result["last_updated"]:
            result["last_updated"] = result["last_updated"]

        # 处理元数据
        if result["metadata"]:
            import json

            with contextlib.suppress(BaseException):
                result["metadata"] = json.loads(result["metadata"])

        return result

    # =========================================================================
    # 查询辅助方法
    # =========================================================================

    @classmethod
    def get_schema(cls) -> pa.Schema:
        """
        获取 Arrow Schema

        Returns:
            PyArrow Schema
        """
        return cls.to_arrow_schema()

    @classmethod
    def get_vector_fields(cls) -> dict[str, int]:
        """
        获取向量字段及其维度

        Returns:
            字段名到维度的映射
        """
        return {
            "title_embedding": 384,
            "content_embedding": 1024,
        }

    @classmethod
    def get_indexable_fields(cls) -> list[str]:
        """
        获取可索引字段列表

        Returns:
            字段名列表
        """
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
        """
        获取可搜索字段列表 (全文搜索)

        Returns:
            字段名列表
        """
        return [
            "title",
            "content_text",
            "source_site",
            "author",
        ]

    # =========================================================================
    # 验证方法
    # =========================================================================

    def validate_data(self) -> tuple[bool, list[str]]:
        """
        验证 Article 数据

        Returns:
            (是否有效, 错误信息列表)
        """
        errors = []

        # 检查必填字段
        if not self.news_id:
            errors.append("news_id is required")
        if not self.title:
            errors.append("title is required")
        if not self.url:
            errors.append("url is required")

        # 检查 URL 格式
        if self.url:
            from urllib.parse import urlparse

            try:
                parsed = urlparse(self.url)
                if not parsed.scheme or not parsed.netloc:
                    errors.append("Invalid URL format")
            except Exception:
                errors.append("Invalid URL format")

        # 检查向量维度
        if self.title_embedding and len(self.title_embedding) != 384:
            errors.append(
                f"title_embedding dimension mismatch: expected 384, got {len(self.title_embedding)}"
            )

        if self.content_embedding and len(self.content_embedding) != 1024:
            errors.append(
                f"content_embedding dimension mismatch: expected 1024, got {len(self.content_embedding)}"
            )

        return len(errors) == 0, errors


# =============================================================================
# 查询模型
# =============================================================================


class ArticleQuery(LanceModel):  # type: ignore[misc]
    """
    文章查询模型

    用于构建复杂的查询条件
    """

    # 关键词搜索
    keyword: str | None = None
    search_fields: list[str] = ["title", "content_text"]

    # 向量搜索
    vector_query: list[float] | None = None
    vector_field: str = "content_embedding"  # 向量字段: title_embedding 或 content_embedding
    similarity_threshold: float = 0.7

    # 过滤条件
    source_site: str | None = None
    author: str | None = None
    tags: list[str] | None = None
    start_date: datetime.datetime | None = None
    end_date: datetime.datetime | None = None
    min_crawl_version: int | None = None

    # 分页和排序
    limit: int = 10
    offset: int = 0
    order_by: str = "publish_date"
    order_desc: bool = True

    # 混合搜索权重
    keyword_weight: float = 0.3  # 全文搜索权重
    vector_weight: float = 0.7  # 向量搜索权重（语义相似度更重要）

    def build_where_clause(self) -> str:
        """
        构建 WHERE 子句

        Returns:
            SQL WHERE 子句
        """
        conditions = []

        if self.source_site:
            conditions.append(f"source_site = '{self.source_site}'")

        if self.author:
            conditions.append(f"author = '{self.author}'")

        if self.tags:
            tags_str = ", ".join(f"'{tag}'" for tag in self.tags)
            conditions.append(f"tags IN ({tags_str})")

        if self.start_date:
            # 使用 ISO 格式让 LanceDB 能够解析
            start_str = self.start_date.isoformat()
            conditions.append(f"publish_date >= CAST('{start_str}' AS timestamp)")

        if self.end_date:
            end_str = self.end_date.isoformat()
            conditions.append(f"publish_date <= CAST('{end_str}' AS timestamp)")

        if self.min_crawl_version:
            conditions.append(f"crawl_version >= {self.min_crawl_version}")

        return " AND ".join(conditions) if conditions else "1=1"

    def validate_data(self) -> tuple[bool, list[str]]:
        """
        验证查询参数

        Returns:
            (是否有效, 错误信息列表)
        """
        errors = []

        if self.limit <= 0 or self.limit > 100:
            errors.append("limit must be between 1 and 100")

        if self.offset < 0:
            errors.append("offset must be >= 0")

        if self.keyword_weight + self.vector_weight != 1.0:
            errors.append("keyword_weight + vector_weight must equal 1.0")

        if self.vector_query and self.vector_field not in ["title_embedding", "content_embedding"]:
            errors.append(
                f"vector_field must be 'title_embedding' or 'content_embedding', got {self.vector_field}"
            )

        if self.vector_query:
            if self.vector_field == "title_embedding":
                expected_dim = 384
            elif self.vector_field == "content_embedding":
                expected_dim = 1024
            elif self.vector_field == "both_embedding":
                expected_dim = 1024  # use content dimension as reference
            else:
                expected_dim = 1024
            if len(self.vector_query) != expected_dim:
                errors.append(
                    f"vector_query dimension mismatch: expected {expected_dim}, got {len(self.vector_query)}"
                )

        return len(errors) == 0, errors
