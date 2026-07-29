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

class ArticleWriteMixin:
    """封装 ArticleRepository 的单一职责方法。"""

    def add_one(self, data: dict[str, Any]) -> bool:
        """添加单条记录"""
        try:

            record = ArticleRecord.from_dict(data)
            record_dict = record.to_dict()

            self._table.add([record_dict])
            self._ensure_indices_after_write()
            logger.debug(f"Added article: {record.news_id}")
            return True
        except (OSError, PermissionError, IOError) as e:
            logger.error(f"Failed to add article: {e}")
            raise RepositorySystemError(f"Failed to add article: {e}") from e
        except Exception as e:
            logger.error(f"Failed to add article: {e}")
            return False

    def add(self, data_list: list[dict[str, Any]]) -> int:
        """批量添加记录"""
        if not data_list:
            return 0

        try:

            records = []
            for data in data_list:
                try:
                    record = ArticleRecord.from_dict(data)
                    records.append(record.to_dict())
                except Exception as e:
                    logger.warning(f"Failed to convert article data: {e}")
                    continue

            if not records:
                return 0

            self._table.add(records)
            self._ensure_indices_after_write()
            logger.info(f"Added {len(records)} articles")
            return len(records)
        except Exception as e:
            logger.error(f"Failed to add articles: {e}")
            return 0

    def get(self, news_id: str) -> dict[str, Any] | None:
        """根据 ID 获取记录"""
        try:
            results = (
                self._table.search()
                .where(f"{ArticleFields.NEWS_ID} = '{sanitize(news_id)}'")
                .limit(1)
                .to_list()
            )
            return results[0] if results else None
        except Exception as e:
            logger.error(f"Failed to get article {news_id}: {e}")
            return None

    def update(self, news_id: str, updates: dict[str, Any]) -> bool:
        """更新记录"""
        try:

            update_data = updates.copy()
            update_data[ArticleFields.NEWS_ID] = news_id
            update_data[ArticleFields.LAST_UPDATED] = datetime.now()

            self._table.merge_insert(
                ArticleFields.NEWS_ID
            ).when_matched_update_all().execute([update_data])
            logger.debug(f"Updated article: {news_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update article {news_id}: {e}")
            return False

    def delete(self, news_id: str) -> bool:
        """删除记录"""
        try:
            safe_news_id = sanitize(news_id)
            self._table.delete(f"{ArticleFields.NEWS_ID} = '{safe_news_id}'")
            logger.info(f"Deleted article from LanceDB: {news_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete article {news_id}: {e}")
            return False
