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

class ArticleFindMixin:
    """封装 ArticleRepository 的单一职责方法。"""

    def find_all(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """获取所有记录"""
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
        """根据来源查找记录"""
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
        """根据作者查找记录"""
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
        """根据日期范围查找记录"""
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
        """根据标签查找记录"""
        if not tags:
            return []

        try:

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
        """全文搜索"""
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
