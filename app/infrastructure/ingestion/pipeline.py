"""IngestionPipeline - ETL 数据摄取管道"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.infrastructure.storage.lancedb import (
    ArticleFields,
    ArticleRepository,
    get_article_repository,
    init_database,
)
from app.infrastructure.storage.sqlite.notice_repository import get_notice_repository

from .dedup import DeduplicationService
from .embedder_provider import EmbeddingClient, get_embedder
from .normalizers import normalize_content, normalize_datetime, normalize_markdown
from .tag_matcher import TagMatcher, get_tag_matcher
from .validators import DocumentValidator, ValidationResult

logger = logging.getLogger(__name__)
from app.infrastructure.ingestion.pipeline_mixins.models import (
    ProcessResult,
    PipelineResult,
)

from .pipeline_mixins.single_process_mixin import SingleProcessMixin
from .pipeline_mixins.batch_process_mixin import BatchProcessMixin
from .pipeline_mixins.pipeline_stage_mixin import PipelineStageMixin

class IngestionPipeline(SingleProcessMixin, BatchProcessMixin, PipelineStageMixin):
    """数据摄取管道"""

    def __init__(
        self,
        repository: ArticleRepository | None = None,
        embedder: EmbeddingClient | None = None,
        validator: DocumentValidator | None = None,
        tag_matcher: TagMatcher | None = None,
        skip_validation: bool = False,
        skip_dedup: bool = False,
        skip_embedding: bool = False,
        skip_tag_matching: bool = False,
        db_path: str | None = None,
    ) -> None:
        """初始化管道"""

        if db_path:
            init_database(db_path)

        self._repository = repository or get_article_repository()
        self._notice_repository = get_notice_repository()
        self._embedder = embedder or get_embedder()
        self._validator = validator or DocumentValidator()
        self._tag_matcher = tag_matcher or get_tag_matcher()
        self._dedup = DeduplicationService(self._repository)

        self._skip_validation = skip_validation
        self._skip_dedup = skip_dedup
        self._skip_embedding = skip_embedding
        self._skip_tag_matching = skip_tag_matching

        logger.info("IngestionPipeline initialized")

def create_pipeline(
    db_path: str | None = None,
    skip_validation: bool = False,
    skip_dedup: bool = False,
) -> IngestionPipeline:
    """创建数据摄取管道"""
    return IngestionPipeline(
        db_path=db_path,
        skip_validation=skip_validation,
        skip_dedup=skip_dedup,
    )

def ingest_documents(
    documents: list[dict[str, Any]],
    db_path: str | None = None,
) -> PipelineResult:
    """快速导入文档"""
    pipeline = create_pipeline(db_path=db_path)
    return pipeline.process_batch(documents)
