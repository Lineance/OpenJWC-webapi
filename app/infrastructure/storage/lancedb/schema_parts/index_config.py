from __future__ import annotations

from app.infrastructure.storage.lancedb.schema import (
    ARTICLES_TABLE_NAME,
    Any,
    ArticleFields,
    ArticleRecord,
    CONTENT_EMBEDDING_DIM,
    TITLE_EMBEDDING_DIM,
    datetime,
    get_article_schema,
    pa,
)

class IndexConfig:
    """索引配置常量"""

    VECTOR_INDEX_TYPE = "IVF_PQ"

    IVF_PARTITIONS = 256

    PQ_SUBQUANTIZERS = 64

    FTS_FIELDS = [ArticleFields.CONTENT_TEXT, ArticleFields.TITLE]

    FTS_USE_TANTIVY = True
