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

class ArticleReadMixin:
    """封装 ArticleRepository 的单一职责方法。"""

    def get_latest(self, limit: int = 10) -> list[dict[str, Any]]:
        """获取最新的记录"""
        return self.find_all(limit=limit)

    def get_oldest(self, limit: int = 10) -> list[dict[str, Any]]:
        """获取最旧的记录"""
        try:
            results = self._table.search().limit(limit).to_list()
            return sorted(
                results,
                key=lambda x: x.get(ArticleFields.PUBLISH_DATE, ""),
                reverse=False,
            )
        except Exception as e:
            logger.error(f"Failed to get oldest articles: {e}")
            return []

    def get_article_content(self, news_id: str) -> dict[str, Any] | None:
        doc = self.get(news_id)
        if not doc:
            return None

        return {
            "title": str(doc.get(ArticleFields.TITLE, "")),
            "content_text": str(doc.get(ArticleFields.CONTENT_TEXT, "") or ""),
            "date": _safe_publish_date_str(doc.get(ArticleFields.PUBLISH_DATE)),
        }
