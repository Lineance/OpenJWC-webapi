from __future__ import annotations

from app.infrastructure.storage.lancedb.tag_repository import (
    Any,
    LanceDBConnection,
    RepositorySystemError,
    TAGS_TABLE_NAME,
    TAG_EMBEDDING_DIM,
    TagFields,
    TagIndexConfig,
    TagRecord,
    annotations,
    datetime,
    get_connection,
    get_tag_schema,
    lancedb,
    logger,
    logging,
)

class TagStatsMixin:
    """封装 TagRepository 的单一职责方法。"""

    def count(self) -> int:
        """获取标签总数"""
        try:
            return int(self._table.count_rows())
        except Exception as e:
            logger.error(f"Failed to count tags: {e}")
            return 0

    def count_by_category(self) -> dict[str, int]:
        """按分类统计标签数"""
        try:
            results = self._table.search().select([TagFields.CATEGORY]).to_list()
            counts: dict[str, int] = {}
            for data in results:
                category = data.get(TagFields.CATEGORY, "unknown")
                counts[category] = counts.get(category, 0) + 1
            return counts
        except Exception as e:
            logger.error(f"Failed to count tags by category: {e}")
            return {}

    def exists(self, tag_id: str) -> bool:
        """检查标签是否存在"""
        return self.get(tag_id) is not None

    def exists_by_name(self, name: str) -> bool:
        """检查标签名称是否存在"""
        return self.get_by_name(name) is not None

    def get_latest(self, limit: int = 10) -> list[TagRecord]:
        """获取最新的标签"""
        return self.find_all(limit=limit)

    def clear_all(self) -> bool:
        """清空所有标签 (仅用于测试)"""
        logger.warning("Clearing all tags from table")
        try:

            self._conn.db.drop_table(TAGS_TABLE_NAME)

            self._table = self._get_or_create_table()
            return True
        except Exception as e:
            logger.error(f"Failed to clear tags: {e}")
            return False
