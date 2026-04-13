"""
Tag Repository - 标签数据 CRUD 操作

提供 Tag 表的 CRUD 操作接口，支持标签的增删改查和向量操作。

Responsibilities:
    - Tag 表 CRUD 操作
    - 标签向量查询和匹配
    - 批量操作支持
    - 标签加载和初始化
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import lancedb

from .connection import LanceDBConnection, get_connection
from .exceptions import RepositorySystemError
from .tag_schema import (
    TAG_EMBEDDING_DIM,
    TagFields,
    TagIndexConfig,
    TagRecord,
    get_tag_schema,
)

logger = logging.getLogger(__name__)

# =============================================================================
# 表名常量
# =============================================================================

TAGS_TABLE_NAME = "tags"

# =============================================================================
# Tag 仓库类
# =============================================================================


class TagRepository:
    """
    Tag 数据仓库

    提供 Tag 表的完整 CRUD 操作接口。

    Features:
        - 标签的增删改查
        - 向量相似度查询
        - 标签分类管理
        - 批量操作支持
        - 标签初始化

    Usage:
        >>> repo = TagRepository()
        >>> # 添加标签
        >>> repo.add_one(tag_data)
        >>> # 批量添加
        >>> repo.add_batch(tags)
        >>> # 相似度查询
        >>> matches = repo.find_similar_tags(query_vector, top_k=5)
    """

    def __init__(self, connection: LanceDBConnection | None = None):
        """
        初始化仓库

        Args:
            connection: LanceDB 连接实例，如果为 None 则使用默认连接
        """
        self._conn = connection or get_connection()
        self._table = self._get_or_create_table()
        logger.info(f"TagRepository initialized for table: {self._table.name}")

    def _get_or_create_table(self) -> lancedb.table.Table:
        """
        获取或创建 tags 表

        Returns:
            LanceDB Table 对象
        """
        table_names = self._conn.db.table_names()

        if TAGS_TABLE_NAME in table_names:
            logger.info(f"Opening existing table: {TAGS_TABLE_NAME}")
            return self._conn.get_table(TAGS_TABLE_NAME)

        # 创建新表
        logger.info(f"Creating new table: {TAGS_TABLE_NAME}")
        schema = get_tag_schema()
        table = self._conn.db.create_table(TAGS_TABLE_NAME, schema=schema)
        logger.info(f"Table '{TAGS_TABLE_NAME}' created successfully")
        return table

    @property
    def table(self) -> lancedb.table.Table:
        """获取底层表对象"""
        return self._table

    @property
    def schema(self) -> Any:
        """获取表结构"""
        return self._table.schema

    # =========================================================================
    # CRUD 操作
    # =========================================================================

    def add_one(self, tag_record: TagRecord) -> bool:
        """
        添加单个标签

        Args:
            tag_record: TagRecord 实例

        Returns:
            是否成功
        """
        try:
            data = tag_record.to_dict()
            self._table.add([data])
            logger.debug(f"Added tag: {tag_record.tag_id} - {tag_record.name}")
            return True
        except (OSError, PermissionError, IOError) as e:
            logger.error(f"Failed to add tag {tag_record.tag_id}: {e}")
            raise RepositorySystemError(f"Failed to add tag: {e}") from e
        except Exception as e:
            logger.error(f"Failed to add tag {tag_record.tag_id}: {e}")
            return False

    def add_batch(self, tag_records: list[TagRecord]) -> int:
        """
        批量添加标签

        Args:
            tag_records: TagRecord 列表

        Returns:
            成功添加的数量
        """
        if not tag_records:
            return 0

        try:
            data_list = [tag.to_dict() for tag in tag_records]
            self._table.add(data_list)
            logger.info(f"Added {len(tag_records)} tags")
            return len(tag_records)
        except Exception as e:
            logger.error(f"Failed to add tags: {e}")
            return 0

    def get(self, tag_id: str) -> TagRecord | None:
        """
        根据 ID 获取标签

        Args:
            tag_id: 标签 ID

        Returns:
            TagRecord 实例，如果不存在则返回 None
        """
        try:
            results = (
                self._table.search()
                .where(f"{TagFields.TAG_ID} = '{tag_id}'")
                .limit(1)
                .to_list()
            )
            return TagRecord.from_dict(results[0]) if results else None
        except Exception as e:
            logger.error(f"Failed to get tag {tag_id}: {e}")
            return None

    def get_by_name(self, name: str) -> TagRecord | None:
        """
        根据名称获取标签

        Args:
            name: 标签名称

        Returns:
            TagRecord 实例，如果不存在则返回 None
        """
        try:
            # 使用全文搜索或精确匹配
            results = (
                self._table.search()
                .where(f"{TagFields.NAME} = '{name}'")
                .limit(1)
                .to_list()
            )
            return TagRecord.from_dict(results[0]) if results else None
        except Exception as e:
            logger.error(f"Failed to get tag by name '{name}': {e}")
            return None

    def update(self, tag_id: str, updates: dict[str, Any]) -> bool:
        """
        更新标签

        Args:
            tag_id: 标签 ID
            updates: 更新字段字典

        Returns:
            是否成功
        """
        try:
            # 构建更新数据
            update_data = updates.copy()
            update_data[TagFields.TAG_ID] = tag_id
            update_data[TagFields.UPDATED_AT] = datetime.now()

            # 使用 merge_insert 进行更新
            self._table.merge_insert(
                TagFields.TAG_ID
            ).when_matched_update_all().execute([update_data])
            logger.debug(f"Updated tag: {tag_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update tag {tag_id}: {e}")
            return False

    def update_record(self, tag_record: TagRecord) -> bool:
        """
        更新 TagRecord

        Args:
            tag_record: 更新后的 TagRecord

        Returns:
            是否成功
        """
        return self.update(tag_record.tag_id, tag_record.to_dict())

    def delete(self, tag_id: str) -> bool:
        """
        删除标签

        Args:
            tag_id: 标签 ID

        Returns:
            是否成功
        """
        try:
            # LanceDB 不支持直接删除，需要覆盖写入
            logger.warning(
                f"LanceDB doesn't support direct deletion, tag {tag_id} marked for cleanup"
            )
            # 可选：标记为删除状态
            return self.update(tag_id, {"deleted": True})
        except Exception as e:
            logger.error(f"Failed to delete tag {tag_id}: {e}")
            return False

    # =========================================================================
    # 查询操作
    # =========================================================================

    def find_all(self, limit: int = 100, offset: int = 0) -> list[TagRecord]:
        """
        获取所有标签

        Args:
            limit: 返回数量限制
            offset: 偏移量

        Returns:
            TagRecord 列表
        """
        try:
            results = self._table.search().limit(limit).offset(offset).to_list()
            return [TagRecord.from_dict(data) for data in results]
        except Exception as e:
            logger.error(f"Failed to find all tags: {e}")
            return []

    def find_by_category(self, category: str, limit: int = 50) -> list[TagRecord]:
        """
        根据分类查找标签

        Args:
            category: 分类名称
            limit: 返回数量限制

        Returns:
            TagRecord 列表
        """
        try:
            results = (
                self._table.search()
                .where(f"{TagFields.CATEGORY} = '{category}'")
                .limit(limit)
                .to_list()
            )
            return [TagRecord.from_dict(data) for data in results]
        except Exception as e:
            logger.error(f"Failed to find tags by category {category}: {e}")
            return []

    def search_by_name(self, query: str, limit: int = 20) -> list[TagRecord]:
        """
        根据名称搜索标签

        Args:
            query: 搜索关键词
            limit: 返回数量限制

        Returns:
            TagRecord 列表
        """
        try:
            results = (
                self._table.search(query=query, query_type="fts").limit(limit).to_list()
            )
            return [TagRecord.from_dict(data) for data in results]
        except Exception as e:
            logger.error(f"Failed to search tags by name '{query}': {e}")
            return []

    # =========================================================================
    # 向量操作
    # =========================================================================

    def find_similar_tags(
        self,
        query_embedding: list[float],
        top_k: int = 5,
        threshold: float = 0.0,
    ) -> list[tuple[TagRecord, float]]:
        """
        查找与查询向量相似的标签

        Args:
            query_embedding: 查询向量 (1024 维)
            top_k: 返回最相似的前 K 个结果
            threshold: 相似度阈值 (0.0-1.0)

        Returns:
            (TagRecord, similarity_score) 元组列表
        """
        if not query_embedding or len(query_embedding) != TAG_EMBEDDING_DIM:
            logger.error(
                f"Invalid query embedding dimension: {len(query_embedding) if query_embedding else 'empty'}"
            )
            return []

        try:
            # 使用向量搜索 - LanceDB API
            results = self._table.search(query_embedding).limit(top_k).to_list()

            # 过滤并转换为 TagRecord
            similar_tags = []
            for result in results:
                # LanceDB 返回 _distance 字段，表示距离（越小越相似）
                # 对于余弦相似度，距离 = 1 - 相似度
                distance = result.get("_distance", 2.0)  # 默认值 2.0（最不相似）

                # 将距离转换为相似度分数
                # 余弦相似度范围：[-1, 1]，但通常 normalized 后为 [0, 1]
                # distance = 1 - cosine_similarity
                similarity_score = 1.0 - distance if distance <= 1.0 else -distance

                if similarity_score >= threshold:
                    tag_record = TagRecord.from_dict(result)
                    similar_tags.append((tag_record, similarity_score))

            # 按相似度降序排序
            similar_tags.sort(key=lambda x: x[1], reverse=True)
            return similar_tags
        except Exception as e:
            logger.error(f"Failed to find similar tags: {e}")
            return []

    def find_tags_for_content(
        self,
        content_embedding: list[float],
        top_k: int = 3,
        threshold: float = 0.75,
    ) -> list[str]:
        """
        为内容寻找合适的标签 (严格匹配模式)

        Args:
            content_embedding: 内容向量
            top_k: 返回标签数量上限
            threshold: 相似度阈值 (严格匹配: 0.75)

        Returns:
            标签 ID 列表
        """
        similar_tags = self.find_similar_tags(
            query_embedding=content_embedding,
            top_k=top_k * 2,  # 获取更多以进行阈值过滤
            threshold=threshold,
        )

        # 按相似度降序排序并返回 tag_id
        similar_tags.sort(key=lambda x: x[1], reverse=True)
        return [tag.tag_id for tag, score in similar_tags[:top_k]]

    def get_all_embeddings(self) -> list[tuple[str, list[float]]]:
        """
        获取所有标签的 ID 和向量

        Returns:
            (tag_id, embedding) 元组列表
        """
        try:
            results = (
                self._table.search()
                .select([TagFields.TAG_ID, TagFields.EMBEDDING])
                .to_list()
            )
            return [
                (data[TagFields.TAG_ID], data[TagFields.EMBEDDING]) for data in results
            ]
        except Exception as e:
            logger.error(f"Failed to get all embeddings: {e}")
            return []

    # =========================================================================
    # 批量操作
    # =========================================================================

    def bulk_update(self, tag_records: list[TagRecord]) -> int:
        """
        批量更新标签

        Args:
            tag_records: 要更新的 TagRecord 列表

        Returns:
            成功更新的数量
        """
        if not tag_records:
            return 0

        try:
            # 准备更新数据
            update_data = []
            for record in tag_records:
                data = record.to_dict()
                data[TagFields.UPDATED_AT] = datetime.now()
                update_data.append(data)

            # 执行批量更新
            self._table.merge_insert(
                TagFields.TAG_ID
            ).when_matched_update_all().execute(update_data)
            logger.info(f"Bulk updated {len(tag_records)} tags")
            return len(tag_records)
        except Exception as e:
            logger.error(f"Failed to bulk update tags: {e}")
            return 0

    # =========================================================================
    # 索引管理
    # =========================================================================

    def create_indices(self) -> bool:
        """
        创建标签表的索引

        规则：
        1. 向量索引：只有在数据行数 >= 256 时才创建
        2. 全文索引：为每个字段单独创建

        Returns:
            是否成功
        """
        try:
            row_count = self._table.count_rows()
            if row_count == 0:
                logger.warning("Table is empty, skipping index creation")
                return False

            logger.info(f"Creating indices for tags table ({row_count} rows)")

            indices_created = False

            # 1. 创建向量索引（仅在行数 >= 256 时创建）
            if row_count >= 256:
                try:
                    self._table.create_index(
                        metric="cosine",
                        vector_column_name=TagFields.EMBEDDING,
                        index_type=TagIndexConfig.VECTOR_INDEX_TYPE,
                        num_partitions=min(TagIndexConfig.IVF_PARTITIONS, row_count),
                        num_sub_vectors=TagIndexConfig.PQ_SUBQUANTIZERS,
                        replace=True,
                    )
                    logger.info(
                        f"Vector index created on '{TagFields.EMBEDDING}' (row_count={row_count} >= 256)"
                    )
                    indices_created = True
                except Exception as e:
                    logger.error(f"Failed to create vector index: {e}")
                    # 向量索引失败不影响全文索引创建
            else:
                logger.info(
                    f"Skipping vector index creation: row_count={row_count} < 256"
                )

            # 2. 创建全文索引（为每个字段单独创建）
            for field in TagIndexConfig.FTS_FIELDS:
                try:
                    self._table.create_fts_index(
                        field,
                        use_tantivy=True,
                        replace=True,
                    )
                    logger.info(f"FTS index created on '{field}'")
                    indices_created = True
                except Exception as e:
                    logger.error(f"Failed to create FTS index on field '{field}': {e}")
                    # 继续为其他字段创建索引

            return indices_created
        except Exception as e:
            logger.error(f"Failed to create indices: {e}")
            return False

    # =========================================================================
    # 统计操作
    # =========================================================================

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

    # =========================================================================
    # 辅助方法
    # =========================================================================

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
        """
        清空所有标签 (仅用于测试)

        Warning:
            此操作不可恢复
        """
        logger.warning("Clearing all tags from table")
        try:
            # LanceDB 不支持直接清空，可以删除并重建表
            self._conn.db.drop_table(TAGS_TABLE_NAME)
            # 重新创建表
            self._table = self._get_or_create_table()
            return True
        except Exception as e:
            logger.error(f"Failed to clear tags: {e}")
            return False


# =============================================================================
# 便捷函数
# =============================================================================


def get_tag_repository(connection: LanceDBConnection | None = None) -> TagRepository:
    """
    获取 TagRepository 实例

    Args:
        connection: LanceDB 连接实例

    Returns:
        TagRepository 实例
    """
    return TagRepository(connection=connection)


def create_tag_repository() -> TagRepository:
    """
    创建 TagRepository 实例

    Returns:
        TagRepository 实例
    """
    return get_tag_repository()
