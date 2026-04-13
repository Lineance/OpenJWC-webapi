"""
Repository Pattern - LanceDB 表 CRUD 操作

提供 Article 表的 CRUD 操作接口，支持批量操作和复杂查询。

Responsibilities:
    - Article 表 CRUD 操作
    - 带 SQL 过滤器的查询构建
    - 批量操作支持
    - 事务性写入
"""

import logging
from datetime import datetime
from typing import Any, cast

from .connection import init_database
from .exceptions import RepositorySystemError
from .guard import SQLGuard, sanitize
from .schema import ArticleFields, ArticleRecord

logger = logging.getLogger(__name__)


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
        logger.info(f"ArticleRepository initialized for table: {self._table.name}")

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
