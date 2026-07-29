from __future__ import annotations

from app.infrastructure.storage.lancedb.connection import (
    ARTICLES_TABLE_NAME,
    Any,
    ArticleFields,
    DEFAULT_DB_PATH,
    IndexConfig,
    Path,
    TYPE_CHECKING,
    _find_project_root,
    _resolve_db_path,
    _table_names,
    get_article_schema,
    lancedb,
    logger,
    logging,
    os,
    threading,
)

class ConnectionManagementMixin:
    """封装 LanceDBConnection 的单一职责方法。"""

    def table_exists(self, name: str = ARTICLES_TABLE_NAME) -> bool:
        """检查表是否存在"""
        return name in _table_names(self._db)

    def drop_table(self, name: str) -> None:
        """删除表"""
        logger.warning(f"Dropping table: {name}")
        self._db.drop_table(name)
        with self._table_lock:
            self._tables.pop(name, None)

    def health_check(self) -> dict[str, Any]:
        """执行健康检查"""
        try:
            tables = _table_names(self._db)
            articles_count = 0

            if ARTICLES_TABLE_NAME in tables:
                articles_count = self.get_table(ARTICLES_TABLE_NAME).count_rows()

            return {
                "status": "healthy",
                "db_path": self._db_path,
                "tables": tables,
                "articles_count": articles_count,
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
            }

    @classmethod
    def reset(cls) -> None:
        """重置单例实例 (仅用于测试)"""
        with cls._lock:
            if cls._instance is not None:
                tables = getattr(cls._instance, "_tables", None)
                if tables is not None:
                    tables.clear()
                cls._instance = None
                logger.warning("LanceDB connection reset")
