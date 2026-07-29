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

class SemanticSearchMixin:
    """封装 RetrievalEngine 的单一职责方法。"""

    def semantic_search(
        self,
        query: str,
        field: Literal["title", "content", "both"] = "content",
        similarity_threshold: float = 0.7,
        limit: int = 10,
        **filters: Any,
    ) -> dict[str, Any]:
        """语义搜索 (纯向量)"""
        query_obj = ArticleQuery(
            keyword=query,
            vector_field=f"{field}_embedding",
            similarity_threshold=similarity_threshold,
            limit=limit,
            **filters,
        )

        results = self._vector_search(query_obj)

        results.sort(key=lambda x: x.get("_score", 0), reverse=True)

        return {
            "query": query,
            "search_type": "semantic",
            "field": field,
            "similarity_threshold": similarity_threshold,
            "total": len(results),
            "results": results[:limit],
        }

    def keyword_search(
        self,
        query: str,
        fields: list[str] | None = None,
        match_type: str = "any",
        limit: int = 10,
        **filters: Any,
    ) -> dict[str, Any]:
        """关键词搜索 (纯全文)"""
        if fields is None:
            fields = ["title", "content_text"]

        query_obj = ArticleQuery(
            keyword=query,
            search_fields=fields,
            limit=limit,
            **filters,
        )

        results = self._fulltext_search(query_obj)

        if match_type == "all":
            keywords = query.lower().split()
            filtered = []
            for result in results:
                text = " ".join(str(result.get(field, "")) for field in fields).lower()
                if all(keyword in text for keyword in keywords):
                    filtered.append(result)
            results = filtered
        elif match_type == "phrase":
            filtered = []
            for result in results:
                text = " ".join(str(result.get(field, "")) for field in fields).lower()
                if query.lower() in text:
                    filtered.append(result)
            results = filtered

        return {
            "query": query,
            "search_type": "keyword",
            "fields": fields,
            "match_type": match_type,
            "total": len(results),
            "results": results[:limit],
        }
