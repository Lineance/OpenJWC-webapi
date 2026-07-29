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

class PipelineStageMixin:
    """封装 IngestionPipeline 的单一职责方法。"""

    def _validate(self, data: dict[str, Any]) -> ValidationResult:
        """验证数据"""
        return self._validator.validate(data)

    def _normalize(self, data: dict[str, Any]) -> dict[str, Any]:
        """标准化数据"""
        result = {}

        result[ArticleFields.NEWS_ID] = data.get("news_id", "")

        raw_title = data.get("title", "")
        if not raw_title:

            content_markdown = data.get("content_markdown", "")
            if content_markdown:
                from app.infrastructure.ingestion.normalizers import extract_first_sentence

                extracted_title = extract_first_sentence(
                    content_markdown, is_markdown=True, max_title_length=100
                )
                if extracted_title:
                    raw_title = extracted_title
                    logger.debug(f"从内容中提取标题: {extracted_title[:50]}...")

        result[ArticleFields.TITLE] = raw_title
        result[ArticleFields.URL] = data.get("url", "")

        result[ArticleFields.PUBLISH_DATE] = normalize_datetime(
            data.get("publish_date")
        )

        result[ArticleFields.SOURCE_SITE] = data.get("source_site", "")
        result[ArticleFields.AUTHOR] = data.get("author", "")
        result[ArticleFields.TAGS] = data.get("tags", [])

        content_markdown = data.get("content_markdown", "")
        content_markdown = normalize_markdown(content_markdown)
        result[ArticleFields.CONTENT_MARKDOWN] = content_markdown

        if "content_text" in data and data["content_text"]:
            result[ArticleFields.CONTENT_TEXT] = data["content_text"]
        else:
            result[ArticleFields.CONTENT_TEXT] = normalize_content(
                content_markdown, is_markdown=True
            )

        attachments = data.get(ArticleFields.ATTACHMENTS)
        if isinstance(attachments, list):
            result[ArticleFields.ATTACHMENTS] = [
                str(item) for item in attachments if item
            ]

        result[ArticleFields.CRAWL_VERSION] = data.get("crawl_version", 1)
        result[ArticleFields.LAST_UPDATED] = datetime.now()

        metadata = data.get("metadata")
        if metadata:
            import json

            result[ArticleFields.METADATA] = (
                json.dumps(metadata, ensure_ascii=False)
                if isinstance(metadata, dict)
                else metadata
            )
        else:
            result[ArticleFields.METADATA] = None

        return result

    def _embed(self, data: dict[str, Any]) -> dict[str, Any]:
        """生成向量嵌入"""
        title = data.get(ArticleFields.TITLE, "")
        content = data.get(ArticleFields.CONTENT_TEXT, "")

        title_vecs, content_vecs = self._embedder.embed_batch([title], [content])

        data[ArticleFields.TITLE_EMBEDDING] = title_vecs[0] if title_vecs else []
        data[ArticleFields.CONTENT_EMBEDDING] = content_vecs[0] if content_vecs else []

        if not self._skip_tag_matching:
            data = self._match_tags(data)

        return data

    def _match_tags(self, data: dict[str, Any]) -> dict[str, Any]:
        """匹配内容标签"""
        content_embedding = data.get(ArticleFields.CONTENT_EMBEDDING)
        if not content_embedding:
            logger.warning("Cannot match tags: content embedding is missing")
            return data

        try:

            matched_tags = self._tag_matcher.match_tags(content_embedding)

            if matched_tags:

                existing_tags = data.get(ArticleFields.TAGS, [])
                all_tags = list(set(existing_tags + matched_tags))
                data[ArticleFields.TAGS] = all_tags
                logger.debug(
                    f"Matched {len(matched_tags)} tags for article: {data.get(ArticleFields.NEWS_ID)}"
                )
            else:
                logger.debug(
                    f"No tags matched for article: {data.get(ArticleFields.NEWS_ID)}"
                )

            return data
        except Exception as e:
            logger.error(f"Failed to match tags: {e}")
            return data

    def _embed_batch(
        self,
        docs: list[dict[str, Any]],
        batch_size: int = 32,
    ) -> list[dict[str, Any]]:
        """批量生成向量嵌入"""

        titles = [doc.get(ArticleFields.TITLE, "") for doc in docs]
        contents = [doc.get(ArticleFields.CONTENT_TEXT, "") for doc in docs]

        title_vecs, content_vecs = self._embedder.embed_batch(
            titles, contents, batch_size
        )

        for i, (title_vec, content_vec) in enumerate(
            zip(title_vecs, content_vecs, strict=False)
        ):
            docs[i][ArticleFields.TITLE_EMBEDDING] = title_vec
            docs[i][ArticleFields.CONTENT_EMBEDDING] = content_vec

        return docs

    def _write(self, data: dict[str, Any]) -> bool:
        """写入数据库"""
        return bool(self._repository.add_one(data))

    def _sync_notice_projection(self, docs: list[dict[str, Any]]) -> None:
        try:
            projected = self._notice_repository.upsert_many_from_articles(docs)
            if projected != len(docs):
                logger.warning(
                    "Notice projection partial success: projected=%s expected=%s",
                    projected,
                    len(docs),
                )
        except Exception as e:
            logger.warning(f"Notice projection failed: {e}")
