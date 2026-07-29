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

class StoreWriteMixin:
    """封装 LanceStore 的单一职责方法。"""

    def add_documents(
        self,
        documents: list[dict[str, Any]],
        generate_embeddings: bool = True,
        batch_size: int = 100,
    ) -> int:
        """批量添加文档"""
        if not documents:
            return 0

        articles = []
        for doc in documents:
            try:
                article = Article.from_dict(doc)
                is_valid, errors = article.validate_data()
                if not is_valid:
                    logger.warning(f"Invalid article skipped: {errors}")
                    continue

                articles.append(article)
            except Exception as e:
                logger.warning(f"Failed to convert document: {e}")
                continue

        try:
            (
                self.table.merge_insert("news_id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute(articles)
            )
            logger.info(f"Added/Updated {len(articles)} documents")
            return len(articles)
        except Exception as e:
            logger.error(f"Failed to add documents: {e}")
            raise

    def update_documents(
        self,
        updates: list[dict[str, Any]],
        merge_key: str = ArticleFields.NEWS_ID,
    ) -> int:
        """批量更新文档"""
        if not updates:
            return 0

        try:
            (
                self.table.merge_insert(merge_key)
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute(updates)
            )
            logger.info(f"Updated {len(updates)} documents")
            return len(updates)
        except Exception as e:
            logger.error(f"Failed to update documents: {e}")
            raise
