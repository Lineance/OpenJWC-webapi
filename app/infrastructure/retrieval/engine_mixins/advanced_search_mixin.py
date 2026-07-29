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

class AdvancedSearchMixin:
    """封装 RetrievalEngine 的单一职责方法。"""

    def advanced_search(
        self,
        query: str,
        vector_weight: float = 0.6,
        keyword_weight: float = 0.4,
        title_weight: float = 0.3,
        content_weight: float = 0.7,
        limit: int = 10,
        **filters: Any,
    ) -> dict[str, Any]:
        """高级混合搜索"""

        query_obj = ArticleQuery(
            keyword=query,
            keyword_weight=keyword_weight,
            vector_weight=vector_weight,
            limit=limit,
            **filters,
        )

        results = self._hybrid_search(query_obj)

        for result in results:

            title_sim = 0.0
            content_sim = 0.0

            if "title_embedding" in result:
                title_vec = self._embedder.embed_query(query, field="title")
                if isinstance(title_vec, tuple):
                    title_vec = title_vec[0]
                title_sim = self._embedder.cosine_similarity(
                    title_vec, result["title_embedding"]
                )

            if "content_embedding" in result:
                content_vec = self._embedder.embed_query(query, field="content")
                if isinstance(content_vec, tuple):
                    content_vec = (
                        content_vec[1] if len(content_vec) > 1 else content_vec[0]
                    )
                content_sim = self._embedder.cosine_similarity(
                    content_vec, result["content_embedding"]
                )

            vector_score = title_sim * title_weight + content_sim * content_weight
            keyword_score = result.get("_score", 0.5)

            result["_vector_score"] = vector_score
            result["_keyword_score"] = keyword_score
            result["_final_score"] = (
                vector_score * vector_weight + keyword_score * keyword_weight
            )

        results.sort(key=lambda x: x.get("_final_score", 0), reverse=True)

        return {
            "query": query,
            "search_type": "advanced",
            "weights": {
                "vector": vector_weight,
                "keyword": keyword_weight,
                "title": title_weight,
                "content": content_weight,
            },
            "total": len(results),
            "results": results[:limit],
        }
