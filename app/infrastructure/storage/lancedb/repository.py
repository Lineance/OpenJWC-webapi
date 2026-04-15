"""
Repository Pattern - LanceDB 表 CRUD 操作

提供 Article 表的 CRUD 操作接口，支持批量操作和复杂查询。

Responsibilities:
    - Article 表 CRUD 操作
    - 带 SQL 过滤器的查询构建
    - 批量操作支持
    - 事务性写入
"""

import json
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


def _safe_publish_date_str(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


def _extract_metadata(record: dict[str, Any]) -> dict[str, Any]:
    metadata = record.get(ArticleFields.METADATA)
    if isinstance(metadata, dict):
        return metadata
    if isinstance(metadata, str) and metadata:
        try:
            loaded = json.loads(metadata)
            if isinstance(loaded, dict):
                return loaded
        except json.JSONDecodeError:
            return {}
    return {}


def _extract_label(record: dict[str, Any]) -> str | None:
    tags = record.get(ArticleFields.TAGS)
    if isinstance(tags, list) and tags:
        first = tags[0]
        return str(first) if first is not None else None

    metadata = _extract_metadata(record)
    label = metadata.get("label")
    if label is not None:
        return str(label)

    source = record.get(ArticleFields.SOURCE_SITE)
    return str(source) if source else None


def _to_notice_item(record: dict[str, Any]) -> dict[str, Any]:
    metadata = _extract_metadata(record)
    attachments = record.get(ArticleFields.ATTACHMENTS)
    if not isinstance(attachments, list):
        attachments = []

    detail_url = metadata.get("detail_url") or record.get(ArticleFields.URL) or ""
    is_page = bool(metadata.get("is_page", True))

    return {
        "id": str(record.get(ArticleFields.NEWS_ID, "")),
        "label": _extract_label(record),
        "title": str(record.get(ArticleFields.TITLE, "")),
        "date": _safe_publish_date_str(record.get(ArticleFields.PUBLISH_DATE)),
        "detail_url": str(detail_url),
        "is_page": is_page,
        "content_text": str(record.get(ArticleFields.CONTENT_TEXT, "") or ""),
        "attachments": [str(item) for item in attachments],
    }


# =============================================================================
# Article 仓库类
# =============================================================================


class ArticleRepository:
    """
    Article 数据仓库

    提供 Article 表的完整 CRUD 操作接口。

    Features:
        - 单条记录 CRUD
        - 批量插入和更新
        - 复杂查询构建
        - 分页和排序
        - 事务性操作

    Usage:
        >>> repo = ArticleRepository()
        >>> # 添加记录
        >>> repo.add_one(article_data)
        >>> # 批量添加
        >>> repo.add_batch(articles)
        >>> # 查询
        >>> results = repo.find_by_source("教务处", limit=10)
    """

    def __init__(self, table: Any = None, db_path: str | None = None) -> None:
        """
        初始化仓库

        Args:
            table: LanceDB 表对象
            db_path: 数据库路径
        """
        if table is None:
            # 初始化数据库并获取表
            conn = init_database(db_path, create_indices=False)
            self._table = conn.get_table()
        else:
            self._table = table

        self._guard = SQLGuard()
        self._notice_cache_lock = threading.Lock()
        self._notice_labels_cache: list[str] | None = None
        self._notice_labels_cache_ts: float = 0.0
        self._notice_labels_cache_ttl_sec = 60.0
        logger.info(f"ArticleRepository initialized for table: {self._table.name}")

    _NOTICE_FULL_SELECT_FIELDS = [
        ArticleFields.NEWS_ID,
        ArticleFields.TITLE,
        ArticleFields.PUBLISH_DATE,
        ArticleFields.URL,
        ArticleFields.SOURCE_SITE,
        ArticleFields.TAGS,
        ArticleFields.METADATA,
        ArticleFields.ATTACHMENTS,
        ArticleFields.CONTENT_TEXT,
    ]

    _NOTICE_LABEL_SELECT_FIELDS = [
        ArticleFields.NEWS_ID,
        ArticleFields.SOURCE_SITE,
        ArticleFields.TAGS,
        ArticleFields.METADATA,
    ]

    def _invalidate_notice_cache(self) -> None:
        with self._notice_cache_lock:
            self._notice_labels_cache = None
            self._notice_labels_cache_ts = 0.0

    def _fetch_docs_by_news_ids(
        self,
        news_ids: list[str],
        select_fields: list[str],
    ) -> list[dict[str, Any]]:
        """Batch fetch records by IDs while preserving caller order."""
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

    def _iter_ordered_news_ids(self, chunk_size: int = 500) -> list[str]:
        """Read ordered IDs in chunks from article_order table."""
        conn = self._get_synced_connection()
        _, total = conn.get_ordered_news_ids(offset=0, limit=1)
        if total <= 0:
            return []

        ordered_ids: list[str] = []
        for chunk_offset in range(0, total, chunk_size):
            chunk_ids, _ = conn.get_ordered_news_ids(
                offset=chunk_offset,
                limit=min(chunk_size, total - chunk_offset),
            )
            if not chunk_ids:
                continue
            ordered_ids.extend(chunk_ids)
        return ordered_ids

    def _get_synced_connection(self):
        """Keep article_order in sync with articles for stable notice pagination."""
        conn = get_connection()
        try:
            order_table = conn.create_article_order_table(exist_ok=True)
            order_count = int(order_table.count_rows())
            article_count = int(self._table.count_rows())
            if order_count != article_count:
                logger.info(
                    "article_order is out of sync, rebuilding "
                    f"(order={order_count}, articles={article_count})"
                )
                conn.rebuild_article_order()
        except Exception as e:
            logger.warning(f"Failed to ensure article_order sync: {e}")
        return conn

    @property
    def table(self) -> Any:
        """获取底层表对象"""
        return self._table

    @property
    def schema(self) -> Any:
        """获取表结构"""
        return self._table.schema

    # =========================================================================
    # CRUD 操作
    # =========================================================================

    def add_one(self, data: dict[str, Any]) -> bool:
        """
        添加单条记录

        Args:
            data: 文章数据字典

        Returns:
            是否成功
        """
        try:
            # 转换为 ArticleRecord 并验证
            record = ArticleRecord.from_dict(data)
            record_dict = record.to_dict()

            # 插入数据
            self._table.add([record_dict])
            self._invalidate_notice_cache()
            logger.debug(f"Added article: {record.news_id}")
            return True
        except (OSError, PermissionError, IOError) as e:
            logger.error(f"Failed to add article: {e}")
            raise RepositorySystemError(f"Failed to add article: {e}") from e
        except Exception as e:
            logger.error(f"Failed to add article: {e}")
            return False

    def add(self, data_list: list[dict[str, Any]]) -> int:
        """
        批量添加记录

        Args:
            data_list: 文章数据字典列表

        Returns:
            成功添加的数量
        """
        if not data_list:
            return 0

        try:
            # 转换为 ArticleRecord 列表
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

            # 批量插入
            self._table.add(records)
            self._invalidate_notice_cache()
            logger.info(f"Added {len(records)} articles")
            return len(records)
        except Exception as e:
            logger.error(f"Failed to add articles: {e}")
            return 0

    def get(self, news_id: str) -> dict[str, Any] | None:
        """
        根据 ID 获取记录

        Args:
            news_id: 新闻 ID

        Returns:
            文章数据字典，如果不存在则返回 None
        """
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
        """
        更新记录

        Args:
            news_id: 新闻 ID
            updates: 更新字段字典

        Returns:
            是否成功
        """
        try:
            # 构建更新数据
            update_data = updates.copy()
            update_data[ArticleFields.NEWS_ID] = news_id
            update_data[ArticleFields.LAST_UPDATED] = datetime.now()

            # 使用 merge_insert 进行更新
            self._table.merge_insert(
                ArticleFields.NEWS_ID
            ).when_matched_update_all().execute([update_data])
            self._invalidate_notice_cache()
            logger.debug(f"Updated article: {news_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update article {news_id}: {e}")
            return False

    def delete(self, news_id: str) -> bool:
        """
        删除记录

        Args:
            news_id: 新闻 ID

        Returns:
            是否成功
        """
        try:
            safe_news_id = sanitize(news_id)
            self._table.delete(f"{ArticleFields.NEWS_ID} = '{safe_news_id}'")
            self._invalidate_notice_cache()
            logger.info(f"Deleted article from LanceDB: {news_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete article {news_id}: {e}")
            return False

    # =========================================================================
    # 查询操作
    # =========================================================================

    def find_all(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """
        获取所有记录

        Args:
            limit: 返回数量限制
            offset: 偏移量

        Returns:
            文章数据列表
        """
        try:
            results = self._table.search().limit(limit).offset(offset).to_list()
            return sorted(
                results,
                key=lambda x: x.get(ArticleFields.PUBLISH_DATE, ""),
                reverse=True,
            )
        except (OSError, PermissionError, IOError) as e:
            logger.error(f"Failed to find all articles: {e}")
            raise RepositorySystemError(f"Failed to find all articles: {e}") from e
        except Exception as e:
            logger.error(f"Failed to find all articles: {e}")
            return []

    def find_by_source(self, source_site: str, limit: int = 50) -> list[dict[str, Any]]:
        """
        根据来源查找记录

        Args:
            source_site: 来源网站
            limit: 返回数量限制

        Returns:
            文章数据列表
        """
        try:
            safe_source = sanitize(source_site)
            results = (
                self._table.search()
                .where(f"{ArticleFields.SOURCE_SITE} = '{safe_source}'")
                .limit(limit)
                .to_list()
            )
            return sorted(
                results,
                key=lambda x: x.get(ArticleFields.PUBLISH_DATE, ""),
                reverse=True,
            )
        except Exception as e:
            logger.error(f"Failed to find articles by source {source_site}: {e}")
            return []

    def find_by_author(self, author: str, limit: int = 50) -> list[dict[str, Any]]:
        """
        根据作者查找记录

        Args:
            author: 作者
            limit: 返回数量限制

        Returns:
            文章数据列表
        """
        try:
            safe_author = sanitize(author)
            results = (
                self._table.search()
                .where(f"{ArticleFields.AUTHOR} = '{safe_author}'")
                .limit(limit)
                .to_list()
            )
            return sorted(
                results,
                key=lambda x: x.get(ArticleFields.PUBLISH_DATE, ""),
                reverse=True,
            )
        except Exception as e:
            logger.error(f"Failed to find articles by author {author}: {e}")
            return []

    def find_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        根据日期范围查找记录

        Args:
            start_date: 开始日期
            end_date: 结束日期
            limit: 返回数量限制

        Returns:
            文章数据列表
        """
        try:
            where_clause = (
                f"{ArticleFields.PUBLISH_DATE} >= '{start_date.isoformat()}' "
                f"AND {ArticleFields.PUBLISH_DATE} <= '{end_date.isoformat()}'"
            )
            results = self._table.search().where(where_clause).limit(limit).to_list()
            return sorted(
                results,
                key=lambda x: x.get(ArticleFields.PUBLISH_DATE, ""),
                reverse=True,
            )
        except Exception as e:
            logger.error(f"Failed to find articles by date range: {e}")
            return []

    def find_by_tags(self, tags: list[str], limit: int = 50) -> list[dict[str, Any]]:
        """
        根据标签查找记录

        Args:
            tags: 标签列表
            limit: 返回数量限制

        Returns:
            文章数据列表
        """
        if not tags:
            return []

        try:
            # 构建标签条件
            tag_conditions = []
            for tag in tags:
                safe_tag = sanitize(tag)
                tag_conditions.append(f"'{safe_tag}' = ANY({ArticleFields.TAGS})")

            where_clause = " OR ".join(tag_conditions)
            results = self._table.search().where(where_clause).limit(limit).to_list()
            return sorted(
                results,
                key=lambda x: x.get(ArticleFields.PUBLISH_DATE, ""),
                reverse=True,
            )
        except Exception as e:
            logger.error(f"Failed to find articles by tags {tags}: {e}")
            return []

    def search_text(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """
        全文搜索

        Args:
            query: 搜索查询
            limit: 返回数量限制

        Returns:
            文章数据列表
        """
        try:
            safe_query = sanitize(query)
            results = (
                self._table.search(query=safe_query, query_type="fts")
                .limit(limit)
                .to_list()
            )
            return cast("list[dict[str, Any]]", results)
        except Exception as e:
            logger.error(f"Failed to search text '{query}': {e}")
            return []

    # =========================================================================
    # 统计操作
    # =========================================================================

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
        """
        按日期统计记录数

        Args:
            group_by: 分组方式 (day, month, year)

        Returns:
            日期到数量的映射
        """
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
                else:  # year
                    key = date.strftime("%Y")

                counts[key] = counts.get(key, 0) + 1

            return counts
        except Exception as e:
            logger.error(f"Failed to count by date: {e}")
            return {}

    # =========================================================================
    # 批量操作
    # =========================================================================

    def bulk_update(self, updates: list[dict[str, Any]]) -> int:
        """
        批量更新记录

        Args:
            updates: 更新数据列表，每个字典必须包含 news_id

        Returns:
            成功更新的数量
        """
        if not updates:
            return 0

        try:
            # 添加更新时间戳
            for update in updates:
                update[ArticleFields.LAST_UPDATED] = datetime.now()

            # 执行批量更新
            self._table.merge_insert(
                ArticleFields.NEWS_ID
            ).when_matched_update_all().execute(updates)
            self._invalidate_notice_cache()
            logger.info(f"Bulk updated {len(updates)} articles")
            return len(updates)
        except Exception as e:
            logger.error(f"Failed to bulk update articles: {e}")
            return 0

    def bulk_delete(self, news_ids: list[str]) -> int:
        """
        批量删除记录

        Args:
            news_ids: 新闻 ID 列表

        Returns:
            成功删除的数量
        """
        if not news_ids:
            return 0

        deleted = 0
        for news_id in news_ids:
            if self.delete(news_id):
                deleted += 1
        return deleted

    # =========================================================================
    # 辅助方法
    # =========================================================================

    def exists(self, news_id: str) -> bool:
        """检查记录是否存在"""
        return self.get(news_id) is not None

    def exists_by_url(self, url: str) -> bool:
        """检查 URL 是否存在"""
        try:
            safe_url = sanitize(url)
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

    # =========================================================================
    # Notices queries
    # =========================================================================

    def list_for_notices(
        self,
        limit: int = 20,
        offset: int = 0,
        label: str | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        """
        Return notice list data from LanceDB.
        """
        try:
            conn = self._get_synced_connection()

            if label is None:
                ordered_ids, total = conn.get_ordered_news_ids(
                    offset=offset, limit=limit
                )
                docs = self._fetch_docs_by_news_ids(
                    ordered_ids,
                    self._NOTICE_FULL_SELECT_FIELDS,
                )
                return total, [_to_notice_item(doc) for doc in docs]

            # Apply label filtering in chunked mode to avoid full-table load.
            filtered_ids: list[str] = []
            all_ordered_ids = self._iter_ordered_news_ids(chunk_size=500)

            for chunk_offset in range(0, len(all_ordered_ids), 500):
                chunk_ids = all_ordered_ids[chunk_offset : chunk_offset + 500]
                chunk_docs = self._fetch_docs_by_news_ids(
                    chunk_ids,
                    self._NOTICE_LABEL_SELECT_FIELDS,
                )
                for doc in chunk_docs:
                    if _extract_label(doc) == label:
                        doc_id = str(doc.get(ArticleFields.NEWS_ID, ""))
                        if doc_id:
                            filtered_ids.append(doc_id)

            total = len(filtered_ids)
            page_ids = filtered_ids[offset : offset + limit]
            page_docs = self._fetch_docs_by_news_ids(
                page_ids,
                self._NOTICE_FULL_SELECT_FIELDS,
            )
            return total, [_to_notice_item(doc) for doc in page_docs]
        except Exception as e:
            logger.error(f"Failed to list notices from LanceDB: {e}")
            return 0, []

    def get_notice_labels(self) -> list[str]:
        """Return unique labels sorted by latest appearance."""
        try:
            now = time.monotonic()
            with self._notice_cache_lock:
                if (
                    self._notice_labels_cache is not None
                    and now - self._notice_labels_cache_ts
                    < self._notice_labels_cache_ttl_sec
                ):
                    return list(self._notice_labels_cache)

            labels: list[str] = []
            seen: set[str] = set()

            all_ordered_ids = self._iter_ordered_news_ids(chunk_size=500)
            for chunk_offset in range(0, len(all_ordered_ids), 500):
                chunk_ids = all_ordered_ids[chunk_offset : chunk_offset + 500]
                chunk_docs = self._fetch_docs_by_news_ids(
                    chunk_ids,
                    self._NOTICE_LABEL_SELECT_FIELDS,
                )
                for doc in chunk_docs:
                    label = _extract_label(doc)
                    if not label or label in seen:
                        continue
                    seen.add(label)
                    labels.append(label)

            with self._notice_cache_lock:
                self._notice_labels_cache = list(labels)
                self._notice_labels_cache_ts = time.monotonic()

            return labels
        except Exception as e:
            logger.error(f"Failed to get notice labels from LanceDB: {e}")
            return []

    def get_notice_total_labels(self) -> int:
        """Return count of unique labels for notices endpoints."""
        return len(self.get_notice_labels())

    def get_notice_info(self, news_id: str) -> dict[str, Any] | None:
        """Return lightweight notice info by ID for admin checks."""
        doc = self.get(news_id)
        if not doc:
            return None
        mapped = _to_notice_item(doc)
        return {
            "id": mapped["id"],
            "label": mapped["label"],
            "title": mapped["title"],
            "date": mapped["date"],
            "detail_url": mapped["detail_url"],
            "is_page": mapped["is_page"],
        }

    def get_notice_content(self, news_id: str) -> dict[str, Any] | None:
        """Return title/content/date payload for chat-context compatibility."""
        doc = self.get(news_id)
        if not doc:
            return None

        return {
            "title": str(doc.get(ArticleFields.TITLE, "")),
            "content_text": str(doc.get(ArticleFields.CONTENT_TEXT, "") or ""),
            "date": _safe_publish_date_str(doc.get(ArticleFields.PUBLISH_DATE)),
        }


# =============================================================================
# 便捷函数
# =============================================================================


def get_article_repository(db_path: str | None = None) -> ArticleRepository:
    """
    获取 ArticleRepository 单例

    Args:
        db_path: 数据库路径

    Returns:
        ArticleRepository 实例
    """
    return ArticleRepository(db_path=db_path)


def create_article_repository(table: Any = None) -> ArticleRepository:
    """
    创建 ArticleRepository 实例

    Args:
        table: LanceDB 表对象

    Returns:
        ArticleRepository 实例
    """
    return ArticleRepository(table=table)
