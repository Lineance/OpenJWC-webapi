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

class ArticleBulkMixin:
    """封装 ArticleRepository 的单一职责方法。"""

    def count(self) -> int:
        """获取总记录数"""
        try:
            return int(self._table.count_rows())
        except Exception as e:
            logger.error(f"Failed to count articles: {e}")
            return 0

    def count_by_source(self) -> dict[str, int]:
        """按来源统计记录数"""
        try:
            results = self._table.search().select([ArticleFields.SOURCE_SITE]).to_list()
            counts: dict[str, int] = {}
            for doc in results:
                source = doc.get(ArticleFields.SOURCE_SITE, "未知")
                counts[source] = counts.get(source, 0) + 1
            return counts
        except Exception as e:
            logger.error(f"Failed to count by source: {e}")
            return {}

    def count_by_date(self, group_by: str = "month") -> dict[str, int]:
        """按日期统计记录数"""
        try:
            results = (
                self._table.search().select([ArticleFields.PUBLISH_DATE]).to_list()
            )
            counts: dict[str, int] = {}

            for doc in results:
                date = doc.get(ArticleFields.PUBLISH_DATE)
                if not date:
                    continue

                if group_by == "day":
                    key = date.strftime("%Y-%m-%d")
                elif group_by == "month":
                    key = date.strftime("%Y-%m")
                else:
                    key = date.strftime("%Y")

                counts[key] = counts.get(key, 0) + 1

            return counts
        except Exception as e:
            logger.error(f"Failed to count by date: {e}")
            return {}

    def bulk_update(self, updates: list[dict[str, Any]]) -> int:
        """批量更新记录"""
        if not updates:
            return 0

        try:

            for update in updates:
                update[ArticleFields.LAST_UPDATED] = datetime.now()

            self._table.merge_insert(
                ArticleFields.NEWS_ID
            ).when_matched_update_all().execute(updates)
            logger.info(f"Bulk updated {len(updates)} articles")
            return len(updates)
        except Exception as e:
            logger.error(f"Failed to bulk update articles: {e}")
            return 0

    def bulk_delete(self, news_ids: list[str]) -> int:
        """批量删除记录"""
        if not news_ids:
            return 0

        deleted = 0
        for news_id in news_ids:
            if self.delete(news_id):
                deleted += 1
        return deleted
