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

class BatchProcessMixin:
    """封装 IngestionPipeline 的单一职责方法。"""

    def process_batch(
        self,
        raw_data_list: list[dict[str, Any]],
        batch_size: int = 32,
    ) -> PipelineResult:
        """批量处理记录"""
        import time

        start_time = time.time()
        result = PipelineResult()

        if not raw_data_list:
            return result

        logger.info(f"Processing batch of {len(raw_data_list)} documents")

        if not self._skip_validation:
            valid_docs = []
            for doc in raw_data_list:
                validation = self._validate(doc)
                if validation.is_valid:
                    valid_docs.append(doc)
                else:
                    result.add_result(
                        ProcessResult(
                            news_id=doc.get("news_id"),
                            url=doc.get("url"),
                            status="invalid",
                            message="; ".join(validation.errors),
                        )
                    )
        else:
            valid_docs = raw_data_list

        normalized_docs = [self._normalize(doc) for doc in valid_docs]

        upsert_docs: list[dict[str, Any]] = []
        if not self._skip_dedup:
            dedup_result = self._dedup.dedup(normalized_docs)

            for doc in dedup_result.duplicate_docs:
                result.add_result(
                    ProcessResult(
                        news_id=doc.get(ArticleFields.NEWS_ID),
                        url=doc.get(ArticleFields.URL),
                        status="duplicate",
                        message="Document already exists",
                    )
                )

            upsert_docs = dedup_result.upsert_docs
            docs_to_process = dedup_result.new_docs + upsert_docs
        else:
            docs_to_process = normalized_docs

        if not self._skip_embedding and docs_to_process:
            logger.info(f"Starting embedding for {len(docs_to_process)} documents...")
            try:
                docs_to_process = self._embed_batch(docs_to_process, batch_size)
                logger.info(f"Embedding completed for {len(docs_to_process)} documents")
            except Exception as e:
                logger.error(f"Embedding failed: {e}")
                raise

        if docs_to_process:
            logger.info(f"Starting write for {len(docs_to_process)} documents...")
            try:
                if not self._skip_dedup:
                    new_docs_write = [
                        d for d in docs_to_process if d not in upsert_docs
                    ]
                    upsert_docs_write = upsert_docs

                    if new_docs_write:
                        self._repository.add(new_docs_write)
                        self._sync_notice_projection(new_docs_write)
                        for doc in new_docs_write:
                            result.add_result(
                                ProcessResult(
                                    news_id=doc.get(ArticleFields.NEWS_ID),
                                    url=doc.get(ArticleFields.URL),
                                    status="success",
                                    message="",
                                )
                            )

                    if upsert_docs_write:
                        self._repository.upsert_batch(upsert_docs_write)
                        self._sync_notice_projection(upsert_docs_write)
                        for doc in upsert_docs_write:
                            result.add_result(
                                ProcessResult(
                                    news_id=doc.get(ArticleFields.NEWS_ID),
                                    url=doc.get(ArticleFields.URL),
                                    status="upsert",
                                    message="",
                                )
                            )
                else:
                    self._repository.add(docs_to_process)
                    self._sync_notice_projection(docs_to_process)
                    for doc in docs_to_process:
                        result.add_result(
                            ProcessResult(
                                news_id=doc.get(ArticleFields.NEWS_ID),
                                url=doc.get(ArticleFields.URL),
                                status="success",
                                message="",
                            )
                        )

                logger.info(f"Write completed for {len(docs_to_process)} documents")
            except Exception as e:
                logger.error(f"Batch write failed: {e}")
                for doc in docs_to_process:
                    result.add_result(
                        ProcessResult(
                            news_id=doc.get(ArticleFields.NEWS_ID),
                            url=doc.get(ArticleFields.URL),
                            status="error",
                            message=str(e),
                        )
                    )

        result.elapsed_seconds = time.time() - start_time
        logger.info(result.summary())
        return result
