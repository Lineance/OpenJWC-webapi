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

class StoreSearchMixin:
    """封装 LanceStore 的单一职责方法。"""

    def vector_search(
        self,
        query_vector: list[float],
        vector_field: str = "content_embedding",
        limit: int = 10,
        offset: int = 0,
        where: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """向量搜索"""
        try:
            results = (
                self.table.search(
                    query=query_vector,
                    vector_column_name=vector_field,
                )
                .limit(limit)
                .offset(offset)
            )

            if where:
                results = results.where(where)

            return cast("list[dict[str, Any]]", results.to_list())
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            raise

    def fulltext_search(
        self,
        query: str,
        fields: list[str] | None = None,
        limit: int = 10,
        offset: int = 0,
        where: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """全文搜索"""
        if fields is None:
            fields = Article.get_searchable_fields()

        try:

            results = (
                self.table.search(
                    query=query,
                    query_type="fts",
                )
                .limit(limit)
                .offset(offset)
            )

            if where:
                results = results.where(where)

            return cast("list[dict[str, Any]]", results.to_list())
        except Exception as e:
            error_msg = str(e)
            if not self._is_fts_index_missing_error(error_msg):
                logger.error(f"Fulltext search failed: {e}")
                raise

            logger.warning(
                "Fulltext index unavailable or corrupted, trying to rebuild index before fallback: %s",
                error_msg,
            )

            try:
                self.create_fulltext_index(fields=fields)
                retried = (
                    self.table.search(
                        query=query,
                        query_type="fts",
                    )
                    .limit(limit)
                    .offset(offset)
                )
                if where:
                    retried = retried.where(where)
                return cast("list[dict[str, Any]]", retried.to_list())
            except Exception as retry_error:
                logger.warning(
                    "Rebuilding FTS index failed, fallback to simple text search: %s",
                    retry_error,
                )
                return self._simple_text_search(query, fields, limit, where)

    def _is_fts_index_missing_error(self, error_msg: str) -> bool:
        """判断是否为 FTS 索引缺失/损坏相关错误。"""
        msg = error_msg.lower()
        patterns = [
            "cannot perform full text search unless an inverted index has been created",
            "fts index does not exist",
            "file not found",
            "filenotfounderror",
            ".arrow",
            "no such file or directory",
            "tantivy",
        ]
        return any(pattern in msg for pattern in patterns)

    def _simple_text_search(
        self,
        query: str,
        fields: list[str],
        limit: int = 10,
        where: str | None = None,
    ) -> list[dict[str, Any]]:
        """简单的文本搜索（降级方案）"""
        try:

            all_docs = self.table.to_pandas().to_dict("records")

            filtered_docs = all_docs
            if where:

                filtered_docs = self._apply_simple_where(filtered_docs, where)

            scored_docs = []
            for doc in filtered_docs:
                score = 0.0

                for field in fields:
                    if field in doc:
                        text = str(doc[field]).lower()
                        query_terms = query.lower().split()

                        for term in query_terms:
                            if term in text:

                                if text == term:
                                    score += 2.0
                                elif re.search(rf"\b{re.escape(term)}\b", text):
                                    score += 1.5
                                else:
                                    score += 1.0

                if score > 0:
                    doc["_score"] = score
                    scored_docs.append(doc)

            scored_docs.sort(key=lambda x: x.get("_score", 0), reverse=True)

            return scored_docs[:limit]

        except Exception as e:
            logger.error(f"Simple text search failed: {e}")
            return []

    def _apply_simple_where(
        self, docs: list[dict[str, Any]], where: str
    ) -> list[dict[str, Any]]:
        """应用简单的 where 条件"""
        if not where:
            return docs

        try:
            filtered = [
                doc for doc in docs if self._evaluate_simple_condition(doc, where)
            ]
            return filtered
        except Exception as e:
            logger.warning(f"Failed to apply where condition: {e}")
            return docs

    def _evaluate_simple_condition(self, doc: dict[str, Any], condition: str) -> bool:
        """评估简单的条件"""

        condition = condition.strip()

        if "!=" in condition:
            field, value = condition.split("!=")
            field = field.strip()
            value = value.strip().strip("'\"")
            return str(doc.get(field, "")) != value

        elif "=" in condition:
            field, value = condition.split("=")
            field = field.strip()
            value = value.strip().strip("'\"")
            return str(doc.get(field, "")) == value

        return True
