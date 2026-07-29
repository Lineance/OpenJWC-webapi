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

class TagFindMixin:
    """封装 TagRepository 的单一职责方法。"""

    def find_all(self, limit: int = 100, offset: int = 0) -> list[TagRecord]:
        """获取所有标签"""
        try:
            results = self._table.search().limit(limit).offset(offset).to_list()
            return [TagRecord.from_dict(data) for data in results]
        except Exception as e:
            logger.error(f"Failed to find all tags: {e}")
            return []

    def find_by_category(self, category: str, limit: int = 50) -> list[TagRecord]:
        """根据分类查找标签"""
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
        """根据名称搜索标签"""
        try:
            results = (
                self._table.search(query=query, query_type="fts").limit(limit).to_list()
            )
            return [TagRecord.from_dict(data) for data in results]
        except Exception as e:
            logger.error(f"Failed to search tags by name '{query}': {e}")
            return []

    def find_similar_tags(
        self,
        query_embedding: list[float],
        top_k: int = 5,
        threshold: float = 0.0,
    ) -> list[tuple[TagRecord, float]]:
        """查找与查询向量相似的标签"""
        if not query_embedding or len(query_embedding) != TAG_EMBEDDING_DIM:
            logger.error(
                f"Invalid query embedding dimension: {len(query_embedding) if query_embedding else 'empty'}"
            )
            return []

        try:

            results = self._table.search(query_embedding).limit(top_k).to_list()

            similar_tags = []
            for result in results:

                distance = result.get("_distance", 2.0)

                similarity_score = 1.0 - distance if distance <= 1.0 else -distance

                if similarity_score >= threshold:
                    tag_record = TagRecord.from_dict(result)
                    similar_tags.append((tag_record, similarity_score))

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
        """为内容寻找合适的标签 (严格匹配模式)"""
        similar_tags = self.find_similar_tags(
            query_embedding=content_embedding,
            top_k=top_k * 2,
            threshold=threshold,
        )

        similar_tags.sort(key=lambda x: x[1], reverse=True)
        return [tag.tag_id for tag, score in similar_tags[:top_k]]

    def get_all_embeddings(self) -> list[tuple[str, list[float]]]:
        """获取所有标签的 ID 和向量"""
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
