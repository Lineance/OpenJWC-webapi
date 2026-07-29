"""Retrieval Engine - 混合检索引擎"""

import logging
from typing import Any, Literal

from .schema.article import ArticleQuery
from .store import LanceStore, create_store
from .utils.embedding import RetrievalEmbedder, get_retrieval_embedder

logger = logging.getLogger(__name__)

from .engine_mixins.basic_search_mixin import BasicSearchMixin
from .engine_mixins.semantic_search_mixin import SemanticSearchMixin
from .engine_mixins.advanced_search_mixin import AdvancedSearchMixin
from .engine_mixins.document_search_mixin import DocumentSearchMixin

class RetrievalEngine(BasicSearchMixin, SemanticSearchMixin, AdvancedSearchMixin, DocumentSearchMixin):
    """混合检索引擎"""

    def __init__(
        self,
        store: LanceStore | None = None,
        embedder: RetrievalEmbedder | None = None,
        db_path: str | None = None,
        table_name: str = "articles",
    ) -> None:
        """初始化检索引擎"""
        if store is None:

            store = create_store(db_path, table_name, create_indices=False)

        if isinstance(store, str):
            logger.error(f"create_store returned a string: {store}")
            raise TypeError(f"Expected LanceStore but got string: {store}")

        if not hasattr(store, "hybrid_search"):
            logger.error("store object missing hybrid_search method")
            raise TypeError("store object is not a LanceStore instance")

        self._store = store
        self._embedder = embedder or get_retrieval_embedder()

        logger.info(
            f"RetrievalEngine initialized with store type: {type(store).__name__}"
        )

def create_engine(
    db_path: str | None = None,
    table_name: str = "articles",
) -> RetrievalEngine:
    """创建检索引擎"""
    return RetrievalEngine(db_path=db_path, table_name=table_name)

def get_engine() -> RetrievalEngine:
    """获取检索引擎单例"""
    return create_engine()
