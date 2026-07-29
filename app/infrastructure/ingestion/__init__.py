"""Ingestion Layer - 数据摄取层"""

from .embedder.clouds_embedder import Embedder
from .dedup import (
    DeduplicationService,
    DedupResult,
    compute_simhash,
    compute_url_hash,
    is_similar,
    normalize_url,
    url_hash,
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

    "IngestionPipeline",
    "PipelineResult",
    "ProcessResult",
    "create_pipeline",
    "ingest_documents",

    "Embedder",
    "get_embedder",
    "embed_title",
    "embed_content",
    "embed_query",

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

    "URLValidator",
    "ContentValidator",
    "DocumentValidator",
    "ValidationResult",
    "validate_url",
    "validate_content",
    "validate_document",
    "is_valid_document",

    "DeduplicationService",
    "DedupResult",
    "normalize_url",
    "url_hash",
    "compute_url_hash",
    "compute_simhash",
    "is_similar",
]
