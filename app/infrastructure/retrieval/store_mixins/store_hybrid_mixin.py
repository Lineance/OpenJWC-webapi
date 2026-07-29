from __future__ import annotations

from app.infrastructure.retrieval.store import (
    ARTICLES_TABLE_NAME,
    Any,
    Article,
    ArticleFields,
    ArticleQuery,
    ArticleRepository,
    Literal,
    RetrievalEmbedder,
    Table,
    cast,
    get_article_repository,
    get_connection,
    get_retrieval_embedder,
    init_database,
    logger,
    logging,
    pa,
    re,
)

class StoreHybridMixin:
    """封装 LanceStore 的单一职责方法。"""

    def hybrid_search(
        self,
        query: str,
        query_obj: ArticleQuery | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """混合搜索 (向量 + 全文)"""
        if query_obj is None:
            query_obj = ArticleQuery(keyword=query, **kwargs)

        is_valid, errors = query_obj.validate_data()
        if not is_valid:
            raise ValueError(f"Invalid query: {errors}")

        vector_results: list[dict[str, Any]] = []
        title_vector_results: list[dict[str, Any]] = []
        content_vector_results: list[dict[str, Any]] = []

        if query_obj.vector_query or query_obj.keyword:
            keyword = query_obj.keyword or ""

            if query_obj.vector_query:

                vector_results = self.vector_search(
                    query_vector=query_obj.vector_query,
                    vector_field=query_obj.vector_field,
                    limit=query_obj.limit * 2,
                    where=query_obj.build_where_clause(),
                )
            elif query_obj.keyword:

                title_vec, content_vec = self._embedder.embed_query(
                    keyword,
                    field="both",
                )

                title_vec = (
                    list[float](title_vec)
                    if isinstance(title_vec, (list, tuple))
                    else [float(title_vec)]
                )
                content_vec = (
                    list[float](content_vec)
                    if isinstance(content_vec, (list, tuple))
                    else [float(content_vec)]
                )

                title_vector_results = self.vector_search(
                    query_vector=title_vec,
                    vector_field="title_embedding",
                    limit=query_obj.limit * 2,
                    offset=query_obj.offset,
                    where=query_obj.build_where_clause(),
                )

                content_vector_results = self.vector_search(
                    query_vector=content_vec,
                    vector_field="content_embedding",
                    limit=query_obj.limit * 2,
                    offset=query_obj.offset,
                    where=query_obj.build_where_clause(),
                )

                vector_results = self._merge_vector_results(
                    title_vector_results,
                    content_vector_results,
                    title_weight=0.3,
                    content_weight=0.7,
                )

        text_results = []
        if query_obj.keyword:
            text_results = self.fulltext_search(
                query=query_obj.keyword,
                fields=query_obj.search_fields,
                limit=query_obj.limit * 2,
                offset=query_obj.offset,
                where=query_obj.build_where_clause(),
            )

        if not vector_results and not text_results and not query_obj.keyword:
            where_clause = query_obj.build_where_clause()
            if where_clause and where_clause != "1=1":
                try:

                    filtered_results = (
                        self.table.search()
                        .where(where_clause)
                        .limit(query_obj.limit)
                        .offset(query_obj.offset)
                        .to_list()
                    )
                    return filtered_results
                except Exception as e:
                    logger.warning(f"Filtered search failed: {e}")

            try:
                return (
                    self.table.search()
                    .order_by("last_updated", descending=True)
                    .limit(query_obj.limit)
                    .offset(query_obj.offset)
                    .to_list()
                )
            except Exception as e:
                logger.warning(f"Empty search order_by failed: {e}")

                return (
                    self.table.search()
                    .limit(query_obj.limit)
                    .offset(query_obj.offset)
                    .to_list()
                )

        return self._fuse_results(
            vector_results,
            text_results,
            query_obj.keyword_weight,
            query_obj.vector_weight,
            query_obj.limit,
        )

    def _merge_vector_results(
        self,
        title_results: list[dict[str, Any]],
        content_results: list[dict[str, Any]],
        title_weight: float = 0.3,
        content_weight: float = 0.7,
    ) -> list[dict[str, Any]]:
        """合并标题和正文向量搜索结果"""

        scores: dict[str, float] = {}
        all_docs: dict[str, dict[str, Any]] = {}

        for i, doc in enumerate(title_results):
            doc_id = doc.get(ArticleFields.NEWS_ID)
            if doc_id:
                rank_score = 1.0 / (i + 1)
                scores[doc_id] = scores.get(doc_id, 0) + rank_score * title_weight
                all_docs[doc_id] = doc

        for i, doc in enumerate(content_results):
            doc_id = doc.get(ArticleFields.NEWS_ID)
            if doc_id:
                rank_score = 1.0 / (i + 1)
                scores[doc_id] = scores.get(doc_id, 0) + rank_score * content_weight
                if doc_id not in all_docs:
                    all_docs[doc_id] = doc

        sorted_docs = sorted(
            scores.items(),
            key=lambda x: x[1],
            reverse=True,
        )

        result: list[dict[str, Any]] = []
        for doc_id, score in sorted_docs:
            matched_doc = all_docs.get(doc_id)
            if matched_doc is not None:
                matched_doc["_score"] = score
                matched_doc["_title_score"] = scores.get(doc_id, 0) * title_weight
                matched_doc["_content_score"] = scores.get(doc_id, 0) * content_weight
                result.append(matched_doc)

        return result

    def _fuse_results(
        self,
        vector_results: list[dict[str, Any]],
        text_results: list[dict[str, Any]],
        text_weight: float,
        vector_weight: float,
        limit: int,
    ) -> list[dict[str, Any]]:
        """融合向量和全文搜索结果"""

        scores: dict[str, float] = {}

        for i, doc in enumerate(vector_results):
            doc_id = doc.get(ArticleFields.NEWS_ID)
            if doc_id:

                rank_score = 1.0 / (i + 1)

                position_boost = max(0.5, 1.0 - (i / 20))
                scores[doc_id] = (
                    scores.get(doc_id, 0) + rank_score * vector_weight * position_boost
                )

        for i, doc in enumerate(text_results):
            doc_id = doc.get(ArticleFields.NEWS_ID)
            if doc_id:
                rank_score = 1.0 / (i + 1)

                text_weight_adjusted = text_weight * max(0.3, 1.0 - (i / 15))
                scores[doc_id] = (
                    scores.get(doc_id, 0) + rank_score * text_weight_adjusted
                )

        all_docs: dict[str, dict[str, Any]] = {}
        for doc in vector_results + text_results:
            doc_id = doc.get(ArticleFields.NEWS_ID)
            if isinstance(doc_id, str):
                all_docs[doc_id] = doc

        sorted_docs = sorted(
            scores.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:limit]

        result: list[dict[str, Any]] = []
        for doc_id, score in sorted_docs:
            matched_doc = all_docs.get(doc_id)
            if matched_doc is not None:
                matched_doc["_score"] = score
                result.append(matched_doc)

        return result
