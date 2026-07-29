from __future__ import annotations

from app.infrastructure.ingestion.pipeline import (
    Any,
    ArticleFields,
    ArticleRepository,
    DeduplicationService,
    DocumentValidator,
    EmbeddingClient,
    PipelineResult,
    ProcessResult,
    TagMatcher,
    ValidationResult,
    dataclass,
    datetime,
    field,
    get_article_repository,
    get_embedder,
    get_notice_repository,
    get_tag_matcher,
    init_database,
    logger,
    logging,
    normalize_content,
    normalize_datetime,
    normalize_markdown,
)

class SingleProcessMixin:
    """封装 IngestionPipeline 的单一职责方法。"""

    def process_one(self, raw_data: dict[str, Any]) -> ProcessResult:
        """处理单条记录"""
        news_id = raw_data.get("news_id")
        url = raw_data.get("url")

        try:

            if not self._skip_validation:
                validation = self._validate(raw_data)
                if not validation.is_valid:
                    return ProcessResult(
                        news_id=news_id,
                        url=url,
                        status="invalid",
                        message="; ".join(validation.errors),
                    )

            normalized = self._normalize(raw_data)

            is_upsert = False
            if not self._skip_dedup:
                dedup_result = self._dedup.dedup([normalized])
                if dedup_result.duplicate_docs:
                    return ProcessResult(
                        news_id=news_id,
                        url=url,
                        status="duplicate",
                        message="Document already exists",
                    )
                if dedup_result.upsert_docs:
                    is_upsert = True

            if not self._skip_embedding:
                normalized = self._embed(normalized)

            if is_upsert:
                success = self._repository.upsert(normalized)
                status = "upsert"
            else:
                success = self._repository.add_one(normalized)
                status = "success"

            if success:
                self._sync_notice_projection([normalized])
                return ProcessResult(
                    news_id=news_id,
                    url=url,
                    status=status,
                    message="",
                )
            else:
                return ProcessResult(
                    news_id=news_id,
                    url=url,
                    status="error",
                    message="Failed to write to database",
                )

        except Exception as e:
            logger.exception(f"Error processing document: {e}")
            return ProcessResult(
                news_id=news_id,
                url=url,
                status="error",
                message=str(e),
            )
