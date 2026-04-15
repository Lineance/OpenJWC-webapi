"""
Ingestion Layer - 数据摄取层

提供从原始数据到 LanceDB 的完整 ETL 流程。

主要模块:
- normalizers: 数据标准化 (Markdown → 纯文本, 日期格式化)
- embedder: 文本向量化 (双模型: 标题 384d + 正文 1024d)
- validators: 数据验证 (URL, 内容长度, 必填字段)
- dedup: 去重检测 (URL 哈希 + SimHash)
- pipeline: 完整 ETL 管道
- adapters: 数据源适配器

Usage:
    >>> from ingestion import ingest_documents, create_pipeline
    >>> result = ingest_documents(documents)
    >>> print(result.summary())
"""

from .embedder.clouds_embedder import Embedder
from .dedup import (
    DuplicateDetector,
    RepositoryDedup,
    compute_simhash,
    compute_url_hash,
    is_similar,
)
from .embedder_provider import embed_content, embed_query, embed_title, get_embedder
from .normalizers import (
    format_datetime,
    markdown_to_text,
    normalize_content,
    normalize_datetime,
    normalize_newlines,
    normalize_unicode,
    normalize_whitespace,
    strip_html,
    strip_markdown_simple,
    truncate_text,
    unescape_html,
)
from .pipeline import (
    IngestionPipeline,
    PipelineResult,
    ProcessResult,
    create_pipeline,
    ingest_documents,
)
from .validators import (
    ContentValidator,
    DocumentValidator,
    URLValidator,
    ValidationResult,
    is_valid_document,
    validate_content,
    validate_document,
    validate_url,
)

__all__ = [
    # Pipeline
    "IngestionPipeline",
    "PipelineResult",
    "ProcessResult",
    "create_pipeline",
    "ingest_documents",
    # Embedder
    "Embedder",
    "get_embedder",
    "embed_title",
    "embed_content",
    "embed_query",
    # Normalizers
    "markdown_to_text",
    "strip_markdown_simple",
    "strip_html",
    "unescape_html",
    "normalize_datetime",
    "format_datetime",
    "normalize_unicode",
    "normalize_whitespace",
    "normalize_newlines",
    "truncate_text",
    "normalize_content",
    # Validators
    "URLValidator",
    "ContentValidator",
    "DocumentValidator",
    "ValidationResult",
    "validate_url",
    "validate_content",
    "validate_document",
    "is_valid_document",
    # Dedup
    "DuplicateDetector",
    "RepositoryDedup",
    "compute_url_hash",
    "compute_simhash",
    "is_similar",
]
