from __future__ import annotations

from app.infrastructure.storage.lancedb.repository import (
    Any,
    ArticleFields,
    ArticleRecord,
    RepositorySystemError,
    SQLGuard,
    _INDEX_ENSURE_INTERVAL,
    _index_ensure_lock,
    _safe_publish_date_str,
    cast,
    datetime,
    get_connection,
    init_database,
    logger,
    logging,
    sanitize,
    threading,
    time,
)

class ArticleUpsertMixin:
    """封装 ArticleRepository 的单一职责方法。"""

    def exists(self, news_id: str) -> bool:
        """检查记录是否存在"""
        return self.get(news_id) is not None

    def exists_by_url(self, url: str) -> bool:
        """检查 URL 是否存在（查询前先规范化，与 DeduplicationService 一致）"""
        try:

            from app.infrastructure.ingestion.dedup import normalize_url

            normalized_url = normalize_url(url)
            safe_url = sanitize(normalized_url)
            results = (
                self._table.search()
                .where(f"{ArticleFields.URL} = '{safe_url}'")
                .limit(1)
                .to_list()
            )
            return len(results) > 0
        except Exception as e:
            logger.error(f"Failed to check URL existence: {e}")
            return False

    def find_by_news_ids(
        self,
        news_ids: list[str],
    ) -> list[dict[str, Any]]:
        """批量按 news_id 查询（WHERE news_id IN (...) 一次查询）"""
        if not news_ids:
            return []

        try:
            safe_ids = [sanitize(news_id) for news_id in news_ids]
            in_clause = ", ".join([f"'{news_id}'" for news_id in safe_ids])
            where_clause = f"{ArticleFields.NEWS_ID} IN ({in_clause})"
            docs = (
                self._table.search()
                .where(where_clause)
                .select(
                    [
                        ArticleFields.NEWS_ID,
                        ArticleFields.URL,
                        ArticleFields.PUBLISH_DATE,
                        ArticleFields.TITLE,
                        ArticleFields.CONTENT_TEXT,
                        ArticleFields.CONTENT_MARKDOWN,
                        ArticleFields.SOURCE_SITE,
                        ArticleFields.AUTHOR,
                        ArticleFields.TAGS,
                        ArticleFields.ATTACHMENTS,
                        ArticleFields.METADATA,
                        ArticleFields.CRAWL_VERSION,
                        ArticleFields.LAST_UPDATED,
                    ]
                )
                .to_list()
            )
        except Exception as e:
            logger.warning(f"Batch fetch by news_ids failed, fallback to per-id: {e}")
            docs = []
            for news_id in news_ids:
                result = self.get(news_id)
                if result:
                    docs.append(result)

        docs_by_id: dict[str, dict[str, Any]] = {
            doc.get(ArticleFields.NEWS_ID, ""): doc for doc in docs
        }
        ordered: list[dict[str, Any]] = []
        for nid in news_ids:
            doc = docs_by_id.get(nid)
            if doc is not None:
                ordered.append(doc)
        return ordered

    def upsert(self, data: dict[str, Any]) -> bool:
        """单条 UPSERT（INSERT or UPDATE）"""
        try:
            record = ArticleRecord.from_dict(data)
            record.crawl_version = record.crawl_version + 1
            record.last_updated = datetime.now()
            record_dict = record.to_dict()

            self._table.merge_insert(
                ArticleFields.NEWS_ID
            ).when_matched_update_all().execute([record_dict])

            logger.debug(f"Upserted article: {record.news_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert article: {e}")
            return False

    def upsert_batch(self, data_list: list[dict[str, Any]]) -> int:
        """批量 UPSERT"""
        if not data_list:
            return 0

        try:
            records = []
            for data in data_list:
                try:
                    record = ArticleRecord.from_dict(data)
                    record.crawl_version = record.crawl_version + 1
                    record.last_updated = datetime.now()
                    records.append(record.to_dict())
                except Exception as e:
                    logger.warning(f"Failed to convert article for upsert: {e}")
                    continue

            if not records:
                return 0

            self._table.merge_insert(
                ArticleFields.NEWS_ID
            ).when_matched_update_all().execute(records)

            logger.info(f"Upserted {len(records)} articles")
            return len(records)
        except Exception as e:
            logger.error(f"Failed to upsert articles: {e}")
            return 0
