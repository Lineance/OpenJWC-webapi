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

class BasicSearchMixin:
    """封装 RetrievalEngine 的单一职责方法。"""

    def search(
        self,
        query: str,
        search_type: str = "hybrid",
        limit: int = 10,
        offset: int = 0,
        **filters: Any,
    ) -> dict[str, Any]:
        """通用搜索接口"""

        query_obj = ArticleQuery(
            keyword=query,
            limit=limit,
            offset=offset,
            **filters,
        )

        is_valid, errors = query_obj.validate_data()
        if not is_valid:
            raise ValueError(f"Invalid query: {errors}")

        if search_type == "vector":
            results = self._vector_search(query_obj)
        elif search_type == "fulltext":
            results = self._fulltext_search(query_obj)
        else:
            results = self._hybrid_search(query_obj)

        paginated_results = results[offset : offset + limit]

        return {
            "query": query,
            "search_type": search_type,
            "total": len(results),
            "limit": limit,
            "offset": offset,
            "results": paginated_results,
            "filters": filters,
        }

    def _vector_search(self, query_obj: ArticleQuery) -> list[dict[str, Any]]:
        """向量搜索"""

        vector: list[float] | tuple[list[float], list[float]]
        if query_obj.vector_query:
            vector = query_obj.vector_query
        else:

            keyword = query_obj.keyword or ""

            field_str = query_obj.vector_field.replace("_embedding", "")

            field: Literal["title", "content", "both"]
            if field_str == "title":
                field = "title"
            elif field_str == "content":
                field = "content"
            else:

                field = "content"

            vector = self._embedder.embed_query(
                keyword,
                field=field,
            )

        if isinstance(vector, tuple):
            vector = vector[0]

        results = self._store.vector_search(
            query_vector=vector,
            vector_field=query_obj.vector_field,
            limit=query_obj.limit * 3,
            where=query_obj.build_where_clause(),
        )
        return results

    def _fulltext_search(self, query_obj: ArticleQuery) -> list[dict[str, Any]]:
        """全文搜索"""
        if not query_obj.keyword:
            return []

        return self._store.fulltext_search(
            query=query_obj.keyword,
            fields=query_obj.search_fields,
            limit=query_obj.limit * 3,
            where=query_obj.build_where_clause(),
        )

    def _hybrid_search(self, query_obj: ArticleQuery) -> list[dict[str, Any]]:
        """混合搜索"""
        return self._store.hybrid_search(
            query=query_obj.keyword or "",
            query_obj=query_obj,
        )
