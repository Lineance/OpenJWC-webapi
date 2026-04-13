"""
Schema Definitions - LanceDB 表结构定义

东南大学新闻数据存储 Schema，包含:
- 结构化元数据字段
- 双向量字段 (标题 384d + 正文 1024d)
- 全文索引字段
- 版本控制字段

Responsibilities:
    - 定义 Article 数据结构
    - PyArrow Schema 生成
    - 字段常量和配置
"""

from datetime import datetime
from typing import Any

import pyarrow as pa

# =============================================================================
# 向量维度常量
# =============================================================================

# 标题向量: paraphrase-multilingual-MiniLM-L12-v2 (384 维)
TITLE_EMBEDDING_DIM = 384

# 正文向量: BAAI/bge-large-zh (1024 维)
CONTENT_EMBEDDING_DIM = 1024

# =============================================================================
# 表名常量
# =============================================================================

ARTICLES_TABLE_NAME = "articles"
ARTICLE_ORDER_TABLE_NAME = "article_order"

# =============================================================================
# 字段名常量
# =============================================================================


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
    ATTACHMENTS = "attachments"  # PDF等附件列表


# =============================================================================
# PyArrow Schema 定义
# =============================================================================


def get_article_order_schema() -> pa.Schema:
    """
    获取 article_order 表的 PyArrow Schema

    用于维护按时间排序的文章 ID 列表，避免每次都全表扫描。

    字段说明:
    - ordinal: 全局序号 (1, 2, 3, ...)
    - ordinal_by_category: 分类内序号 (每个分类独立从1开始)
    - news_id: 文章 ID
    - publish_date: 发布时间
    - category: 分类

    Returns:
        pa.Schema: PyArrow Schema 对象
    """
    return pa.schema(
        [
            pa.field("ordinal", pa.int32(), nullable=False),
            pa.field("ordinal_by_category", pa.int32(), nullable=False),
            pa.field("news_id", pa.string(), nullable=False),
            pa.field("publish_date", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("category", pa.string(), nullable=True),
        ]
    )


def get_article_schema() -> pa.Schema:
    """
    获取 Article 表的 PyArrow Schema

    字段说明:
    - news_id: 主键，唯一标识符 (如 "20240520_cs_dosti_lecture")
    - title: 新闻标题
    - publish_date: 发布时间 (带时区的时间戳)
    - url: 原始 URL
    - source_site: 来源站点 (如 "计算机科学与工程学院")
    - author: 作者
    - tags: 标签列表
    - content_markdown: 原始 Markdown 内容
    - content_text: 从 Markdown 提取的纯文本 (用于向量化和全文搜索)
    - title_embedding: 标题向量 (384 维)
    - content_embedding: 正文向量 (1024 维)
    - crawl_version: 爬取版本号 (递增)
    - last_updated: 最后更新时间
    - metadata: 额外元数据 (JSON 字符串格式)

    Returns:
        pa.Schema: PyArrow Schema 对象
    """
    return pa.schema(
        [
            # 主键和基本信息
            pa.field(ArticleFields.NEWS_ID, pa.string(), nullable=False),
            pa.field(ArticleFields.TITLE, pa.string(), nullable=False),
            pa.field(ArticleFields.PUBLISH_DATE, pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field(ArticleFields.URL, pa.string(), nullable=False),
            pa.field(ArticleFields.SOURCE_SITE, pa.string(), nullable=True),
            pa.field(ArticleFields.AUTHOR, pa.string(), nullable=True),
            # 标签列表
            pa.field(ArticleFields.TAGS, pa.list_(pa.string()), nullable=True),
            # 内容字段
            pa.field(ArticleFields.CONTENT_MARKDOWN, pa.string(), nullable=True),
            pa.field(ArticleFields.CONTENT_TEXT, pa.string(), nullable=True),
            # 附件字段 (PDF等)
            pa.field(ArticleFields.ATTACHMENTS, pa.list_(pa.string()), nullable=True),
            # 向量字段 (固定长度列表)
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
            # 版本控制
            pa.field(ArticleFields.CRAWL_VERSION, pa.int32(), nullable=False),
            pa.field(ArticleFields.LAST_UPDATED, pa.timestamp("us", tz="UTC"), nullable=False),
            # 元数据 (JSON 字符串)
            pa.field(ArticleFields.METADATA, pa.string(), nullable=True),
        ]
    )


# =============================================================================
# 数据模型类 (用于类型提示和验证)
# =============================================================================


class ArticleRecord:
    """
    Article 记录的数据类，用于类型提示和构建记录

    注意: 这不是 ORM 模型，仅用于数据传输和验证
    """

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
    ):
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

        # 处理可选字段的缺失值，提供默认值
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


# =============================================================================
# 索引配置
# =============================================================================


class IndexConfig:
    """索引配置常量"""

    # 向量索引类型
    VECTOR_INDEX_TYPE = "IVF_PQ"

    # IVF 分区数 (建议为 sqrt(N) 到 N/50)
    IVF_PARTITIONS = 256

    # PQ 子量化器数量 (必须是向量维度的因数: 1024=2^10, 可选 64/128/256/512)
    PQ_SUBQUANTIZERS = 64

    # 全文索引字段
    FTS_FIELDS = [ArticleFields.CONTENT_TEXT, ArticleFields.TITLE]

    # 全文索引使用 Tantivy
    FTS_USE_TANTIVY = True
