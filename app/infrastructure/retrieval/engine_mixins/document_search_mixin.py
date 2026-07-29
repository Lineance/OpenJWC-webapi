from __future__ import annotations

from app.infrastructure.retrieval.engine import (
    Any,
    ArticleQuery,
    LanceStore,
    Literal,
    RetrievalEmbedder,
    create_store,
    get_retrieval_embedder,
    logger,
    logging,
)

class DocumentSearchMixin:
    """封装 RetrievalEngine 的单一职责方法。"""

    def get_document(self, news_id: str) -> dict[str, Any] | None:
        """获取单个文档"""
        try:
            results = (
                self._store.table.search()
                .where(f"news_id = '{news_id}'")
                .limit(1)
                .to_list()
            )
            return results[0] if results else None
        except Exception as e:
            logger.error(f"Failed to get document {news_id}: {e}")
            return None

    def get_similar_documents(
        self,
        news_id: str,
        field: str = "content",
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """获取相似文档"""

        doc = self.get_document(news_id)
        if not doc:
            return []

        vector_field = f"{field}_embedding"
        if vector_field not in doc:
            return []

        return self._store.vector_search(
            query_vector=doc[vector_field],
            vector_field=vector_field,
            limit=limit + 1,
            where=f"news_id != '{news_id}'",
        )[:limit]

    def get_statistics(self) -> dict[str, Any]:
        """获取统计信息"""
        try:
            count = self._store.count()
            info = self._store.info()

            sources: dict[str, int] = {}
            try:
                results = self._store.table.search().select(["source_site"]).to_list()
                for doc in results:
                    source = doc.get("source_site", "未知")
                    sources[source] = sources.get(source, 0) + 1
            except Exception as e:
                print(f"RetrievalEngine Exception:{e}")
                pass

            time_range = {}
            try:
                results = self._store.table.search().select(["publish_date"]).to_list()
                dates = [
                    doc["publish_date"]
                    for doc in results
                    if doc.get("publish_date") is not None
                ]
                if dates:
                    time_range["min"] = min(dates)
                    time_range["max"] = max(dates)
            except Exception as e:
                print(f"RetrievalEngine Exception:{e}")
                pass

            return {
                "total_documents": count,
                "table_info": info,
                "source_distribution": sources,
                "time_range": time_range,
            }
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}
