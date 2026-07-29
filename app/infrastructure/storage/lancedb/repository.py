"""Repository Pattern - LanceDB 表 CRUD 操作"""

import logging
import threading
import time
from datetime import datetime
from typing import Any, cast

from .connection import get_connection, init_database
from .exceptions import RepositorySystemError
from .guard import SQLGuard, sanitize
from .schema import ArticleFields, ArticleRecord

logger = logging.getLogger(__name__)

_index_ensure_lock = threading.Lock()
_INDEX_ENSURE_INTERVAL = 120.0

def _safe_publish_date_str(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, datetime):
        return value.date().isoformat()

    if hasattr(value, "isoformat"):
        text = str(value.isoformat())
    else:
        text = str(value)

    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]

    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).date().isoformat()
    except ValueError:
        return text

from .article_repository_mixins.article_write_mixin import ArticleWriteMixin
from .article_repository_mixins.article_find_mixin import ArticleFindMixin
from .article_repository_mixins.article_bulk_mixin import ArticleBulkMixin
from .article_repository_mixins.article_upsert_mixin import ArticleUpsertMixin
from .article_repository_mixins.article_read_mixin import ArticleReadMixin

class ArticleRepository(ArticleWriteMixin, ArticleFindMixin, ArticleBulkMixin, ArticleUpsertMixin, ArticleReadMixin):
    """Article 数据仓库"""

    def __init__(self, table: Any = None, db_path: str | None = None) -> None:
        """初始化仓库"""
        if table is None:

            conn = init_database(db_path, create_indices=False)
            self._table = conn.get_table()
        else:
            self._table = table

        self._guard = SQLGuard()
        self._last_index_ensure_ts = 0.0
        logger.info(f"ArticleRepository initialized for table: {self._table.name}")

    def _fetch_docs_by_news_ids(
        self,
        news_ids: list[str],
        select_fields: list[str],
    ) -> list[dict[str, Any]]:
        if not news_ids:
            return []

        try:
            safe_ids = [sanitize(news_id) for news_id in news_ids]
            in_clause = ", ".join([f"'{news_id}'" for news_id in safe_ids])
            where_clause = f"{ArticleFields.NEWS_ID} IN ({in_clause})"
            docs = (
                self._table.search().where(where_clause).select(select_fields).to_list()
            )
        except Exception as e:
            logger.warning(
                f"Batch fetch by news IDs failed, fallback to per-id query: {e}"
            )
            docs = []
            for news_id in news_ids:
                try:
                    result = (
                        self._table.search()
                        .where(f"{ArticleFields.NEWS_ID} = '{sanitize(news_id)}'")
                        .select(select_fields)
                        .limit(1)
                        .to_list()
                    )
                    if result:
                        docs.append(result[0])
                except Exception as single_error:
                    logger.warning(
                        f"Fetch by news ID failed: id={news_id}, error={single_error}"
                    )

        docs_by_id: dict[str, dict[str, Any]] = {}
        for doc in docs:
            doc_id = str(doc.get(ArticleFields.NEWS_ID, ""))
            if doc_id:
                docs_by_id[doc_id] = doc

        ordered_docs: list[dict[str, Any]] = []
        for news_id in news_ids:
            doc = docs_by_id.get(news_id)
            if doc is not None:
                ordered_docs.append(doc)

        return ordered_docs

    def _ensure_indices_after_write(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_index_ensure_ts < _INDEX_ENSURE_INTERVAL:
            return

        with _index_ensure_lock:
            now = time.monotonic()
            if not force and now - self._last_index_ensure_ts < _INDEX_ENSURE_INTERVAL:
                return
            try:
                get_connection().create_indices()
                self._last_index_ensure_ts = now
            except Exception as e:
                logger.warning(f"Post-write index ensure failed: {e}")

    @property
    def table(self) -> Any:
        """获取底层表对象"""
        return self._table

    @property
    def schema(self) -> Any:
        """获取表结构"""
        return self._table.schema

def get_article_repository(db_path: str | None = None) -> ArticleRepository:
    """获取 ArticleRepository 单例"""
    return ArticleRepository(db_path=db_path)

def create_article_repository(table: Any = None) -> ArticleRepository:
    """创建 ArticleRepository 实例"""
    return ArticleRepository(table=table)
