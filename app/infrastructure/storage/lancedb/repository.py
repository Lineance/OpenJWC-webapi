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

# =============================================================================
# 全局同步检查控制
# =============================================================================

_last_sync_check = 0.0
_SYNC_CHECK_INTERVAL = 300  # 5分钟内不重复检查
_sync_rebuild_lock = threading.Lock()
_index_ensure_lock = threading.Lock()
_INDEX_ENSURE_INTERVAL = 120.0


def _safe_publish_date_str(value: Any) -> str:
    if value is None:
        return ""

    # Keep external API contract stable: notices date should be YYYY-mm-dd.
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
        self._last_index_ensure_ts = 0.0
        # Label 缓存
        self._labels_cache: list[str] | None = None
        self._labels_cache_ts: float = 0.0
        self._labels_cache_ttl_sec: float = 300.0  # 5分钟缓存
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
        """Keep article_order and label_stats in sync with articles.

        使用检查间隔避免每次请求都执行 count_rows() 检查。
        同时重建 article_order 和 label_stats。
        """
        global _last_sync_check
        conn = get_connection()

        now = time.time()
        if now - _last_sync_check > _SYNC_CHECK_INTERVAL:
            with _sync_rebuild_lock:
                # 再次检查时间（可能其他线程已重建）
                if now - _last_sync_check > _SYNC_CHECK_INTERVAL:
                    try:
                        order_table = conn.create_article_order_table(exist_ok=True)
                        order_count = int(order_table.count_rows())
                        article_count = int(self._table.count_rows())
                        if order_count != article_count:
                            logger.info(
                                f"Rebuilding article_order (order={order_count}, articles={article_count})"
                            )
                            conn.rebuild_article_order()
                        # 同时重建 label_stats
                        self.rebuild_label_stats()
                        _last_sync_check = time.time()
                    except Exception as e:
                        logger.warning(f"Sync check failed: {e}")

        return conn

    def _maybe_rebuild_order(self) -> None:
        """写入后检查是否需要重建，仅在新数据导致数量不匹配时触发"""
        try:
            conn = get_connection()
            order_table = conn.create_article_order_table(exist_ok=True)
            order_count = int(order_table.count_rows())
            article_count = int(self._table.count_rows())
            if order_count != article_count:
                logger.info(
                    f"New articles added, rebuilding article_order (order={order_count}, articles={article_count})"
                )
                conn.rebuild_article_order()
        except Exception as e:
            logger.warning(f"Post-write sync check failed: {e}")

    @staticmethod
    def _get_label_stats_schema():
        """Return label_stats schema used by notice label APIs."""
        import pyarrow as pa

        return pa.schema(
            [
                pa.field("label", pa.string(), nullable=False),
                pa.field("count", pa.int32(), nullable=False),
                pa.field("order", pa.int32(), nullable=False),
            ]
        )

    def _get_or_create_label_stats_table(self):
        """Get cached label_stats table, create it if missing."""
        conn = get_connection()
        try:
            return conn.get_table("label_stats")
        except ValueError:
            conn.db.create_table("label_stats", schema=self._get_label_stats_schema())
            return conn.get_table("label_stats")

    def _ensure_indices_after_write(self, force: bool = False) -> None:
        """Ensure indices after writes with throttling to avoid repeated rebuilds."""
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
            self._ensure_indices_after_write()
            self._maybe_rebuild_order()
            self._update_label_stats_for_article(record_dict.get(ArticleFields.TAGS))
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
            self._ensure_indices_after_write()
            self._maybe_rebuild_order()
            # 更新 label 统计
            for record in records:
                self._update_label_stats_for_article(record.get(ArticleFields.TAGS))
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
        """检查 URL 是否存在（查询前先规范化，与 DeduplicationService 一致）"""
        try:
            # 延迟导入避免循环依赖
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
        """
        批量按 news_id 查询（WHERE news_id IN (...) 一次查询）

        Args:
            news_ids: news_id 列表

        Returns:
            匹配的 article 记录列表（按 news_ids 顺序）
        """
        if not news_ids:
            return []

        try:
            safe_ids = [sanitize(news_id) for news_id in news_ids]
            in_clause = ", ".join([f"'{news_id}'" for news_id in safe_ids])
            where_clause = f"{ArticleFields.NEWS_ID} IN ({in_clause})"
            docs = (
                self._table.search()
                .where(where_clause)
                .select([
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
                ])
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
        """
        单条 UPSERT（INSERT or UPDATE）

        Args:
            data: article 数据字典

        Returns:
            是否成功
        """
        try:
            record = ArticleRecord.from_dict(data)
            record.crawl_version = record.crawl_version + 1
            record.last_updated = datetime.now()
            record_dict = record.to_dict()

            self._table.merge_insert(
                ArticleFields.NEWS_ID
            ).when_matched_update_all().execute([record_dict])

            self._maybe_rebuild_order()
            self._update_label_stats_for_article(record_dict.get(ArticleFields.TAGS))
            logger.debug(f"Upserted article: {record.news_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert article: {e}")
            return False

    def upsert_batch(self, data_list: list[dict[str, Any]]) -> int:
        """
        批量 UPSERT

        Args:
            data_list: article 数据字典列表

        Returns:
            成功数量
        """
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

            self._maybe_rebuild_order()
            for record in records:
                self._update_label_stats_for_article(record.get(ArticleFields.TAGS))

            logger.info(f"Upserted {len(records)} articles")
            return len(records)
        except Exception as e:
            logger.error(f"Failed to upsert articles: {e}")
            return 0

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

            # 直接使用 article_order.category (= label) 过滤
            ordered_ids, total = conn.get_ordered_news_ids(
                offset=offset, limit=limit, category=label
            )
            docs = self._fetch_docs_by_news_ids(
                ordered_ids,
                self._NOTICE_FULL_SELECT_FIELDS,
            )
            return total, [_to_notice_item(doc) for doc in docs]
        except Exception as e:
            logger.error(f"Failed to list notices from LanceDB: {e}")
            return self._list_for_notices_fallback(
                limit=limit, offset=offset, label=label
            )

    def _list_for_notices_fallback(
        self,
        limit: int,
        offset: int,
        label: str | None,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Fallback notice list using direct article scan when order table is unavailable."""
        try:
            docs = (
                self._table.search().select(self._NOTICE_FULL_SELECT_FIELDS).to_list()
            )

            if label:
                docs = [doc for doc in docs if _extract_label(doc) == label]

            docs.sort(
                key=lambda item: _safe_publish_date_str(
                    item.get(ArticleFields.PUBLISH_DATE)
                ),
                reverse=True,
            )
            total = len(docs)
            page_docs = docs[offset : offset + limit]
            logger.warning(
                "list_for_notices fallback path hit: total=%s, returned=%s, label=%s",
                total,
                len(page_docs),
                label,
            )
            return total, [_to_notice_item(doc) for doc in page_docs]
        except Exception as fallback_error:
            logger.error(f"Fallback notice listing failed: {fallback_error}")
            return 0, []

    def get_notice_labels(self) -> list[str]:
        """Return unique labels from label_stats ordered by `order` field."""
        try:
            label_stats = self._get_or_create_label_stats_table()
            results = label_stats.search().to_list()
            results.sort(key=lambda r: r.get("order", 0))
            labels = [str(r.get("label")) for r in results if r.get("label")]

            self._labels_cache = labels
            self._labels_cache_ts = time.monotonic()
            return labels
        except Exception:
            # 表不存在，触发重建
            logger.info("label_stats table not found, rebuilding...")
            self.rebuild_label_stats()
            try:
                label_stats = self._get_or_create_label_stats_table()
                results = label_stats.search().to_list()
                results.sort(key=lambda r: r.get("order", 0))
                labels = [str(r.get("label")) for r in results if r.get("label")]
                self._labels_cache = labels
                self._labels_cache_ts = time.monotonic()
                return labels
            except Exception:
                return []

    def get_notice_total_labels(self) -> int:
        """Return count of unique labels from cached labels list."""
        if (
            self._labels_cache is not None
            and time.monotonic() - self._labels_cache_ts < self._labels_cache_ttl_sec
        ):
            return len(self._labels_cache)

        labels = self.get_notice_labels()
        if labels:
            return len(labels)

        try:
            label_stats = self._get_or_create_label_stats_table()
            return label_stats.count_rows()
        except Exception:
            # 表不存在，触发重建
            logger.info("label_stats table not found, rebuilding...")
            self.rebuild_label_stats()
            try:
                return self._get_or_create_label_stats_table().count_rows()
            except Exception:
                return 0

    def rebuild_label_stats(self) -> dict[str, int]:
        """Rebuild label_stats table ordered by article_order's ordinal."""
        try:
            conn = get_connection()
            order_table = conn.create_article_order_table(exist_ok=True)
            total = order_table.count_rows()
            order_results = order_table.search().limit(max(total, 1000)).to_list()
            order_results.sort(key=lambda r: r.get("ordinal", 0))

            seen: dict[str, int] = {}
            counter: dict[str, int] = {}
            for r in order_results:
                cat = r.get("category", "")
                if not cat:
                    continue
                if cat not in seen:
                    seen[cat] = len(seen)
                counter[cat] = counter.get(cat, 0) + 1

            sorted_labels = sorted(seen.keys(), key=lambda x: seen[x])

            try:
                conn.drop_table("label_stats")
            except Exception:
                pass

            conn.db.create_table("label_stats", schema=self._get_label_stats_schema())

            stats_table = conn.get_table("label_stats")
            if sorted_labels:
                stats_table.add(
                    [
                        {"label": label, "count": counter[label], "order": seen[label]}
                        for label in sorted_labels
                    ]
                )

            logger.info(f"Rebuilt label_stats with {len(sorted_labels)} labels")
            return counter
        except Exception as e:
            logger.error(f"Failed to rebuild label_stats: {e}")
            return {}

    def _update_label_stats_for_article(self, tags: list | None) -> None:
        """Update label_stats when an article is added. Reorders all labels based on article_order."""
        if not tags:
            return
        label = tags[0] if tags else None
        if not label:
            return
        
        try:
            conn = get_connection()
            stats_table = conn.db.open_table("label_stats")

            # 检查 label 是否存在
            existing = stats_table.search().where(f"label = '{label}'").limit(1).to_list()
            if existing:
                # 更新 count
                old_count = existing[0].get("count", 0)
                # 删除旧记录
                stats_table.delete(f"label = '{label}'")
                # 添加新记录
                stats_table.add([{"label": label, "count": old_count + 1}])
            else:
                # 新增 label
                stats_table.add([{"label": label, "count": 1}])
        except Exception as e:
            logger.warning(f"Failed to update label_stats: {e}")

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
