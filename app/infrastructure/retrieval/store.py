"""LanceStore - LanceDB 表操作封装"""

import logging
import re
from typing import Any, Literal, cast

import pyarrow as pa
from lancedb.table import Table

from app.infrastructure.storage.lancedb import (
    ArticleRepository,
    get_article_repository,
    get_connection,
    init_database,
)
from app.infrastructure.storage.lancedb.schema import ARTICLES_TABLE_NAME, ArticleFields

from .schema.article import Article, ArticleQuery
from .utils.embedding import RetrievalEmbedder, get_retrieval_embedder

logger = logging.getLogger(__name__)

from .store_mixins.store_index_mixin import StoreIndexMixin
from .store_mixins.store_search_mixin import StoreSearchMixin
from .store_mixins.store_hybrid_mixin import StoreHybridMixin
from .store_mixins.store_write_mixin import StoreWriteMixin

class LanceStore(StoreIndexMixin, StoreSearchMixin, StoreHybridMixin, StoreWriteMixin):
    """LanceDB 表操作封装"""

    def __init__(
        self,
        table: Table | None = None,
        repository: ArticleRepository | None = None,
        embedder: RetrievalEmbedder | None = None,
        db_path: str | None = None,
        table_name: str = "articles",
    ) -> None:
        """初始化 LanceStore"""
        self._table = table

        if not self._table and db_path:
            self._initialize_table(db_path, table_name)

        if repository is not None:
            self._repository = repository
        elif self._table is not None:
            self._repository = ArticleRepository(table=self._table)
        else:
            self._repository = get_article_repository(db_path=db_path)

        self._embedder = embedder or get_retrieval_embedder()

        logger.info(f"LanceStore initialized for table: {table_name}")

    def _initialize_table(self, db_path: str, table_name: str) -> None:
        """初始化表"""
        try:
            if table_name == ARTICLES_TABLE_NAME:
                conn = init_database(db_path, create_indices=False)
                self._table = conn.get_table(table_name)
                logger.info(
                    f"Opened existing table via connection manager: {table_name}"
                )
                return

            conn = get_connection(db_path)
            if conn.table_exists(table_name):
                self._table = conn.get_table(table_name)
                logger.info(
                    f"Opened existing table via connection manager: {table_name}"
                )
            else:

                schema = Article.get_schema()
                conn.db.create_table(table_name, schema=schema)
                self._table = conn.get_table(table_name)
                logger.info(f"Created new table via connection manager: {table_name}")
        except Exception as e:
            logger.error(f"Failed to initialize table: {e}")
            raise

    @property
    def table(self) -> Table:
        """获取表对象"""
        if self._table is None:
            raise ValueError("Table not initialized")
        return self._table

    def count(self) -> int:
        """获取记录数"""
        return int(self.table.count_rows())

    def schema(self) -> pa.Schema:
        """获取表结构"""
        return self.table.schema

    def info(self) -> dict[str, Any]:
        """获取表信息"""
        return {
            "name": self.table.name,
            "count": self.count(),
            "schema": str(self.schema()),
            "indices": self.list_indices(),
        }

def create_store(
    db_path: str | None = None,
    table_name: str = "articles",
    create_indices: bool = False,
) -> LanceStore:
    """创建 LanceStore 实例"""
    store = LanceStore(db_path=db_path, table_name=table_name)

    if create_indices:
        try:

            count = store.count()
            if count > 0:

                try:
                    store.create_vector_index("content_embedding")
                    logger.info("Created content vector index")
                except Exception as e:
                    logger.warning(f"Failed to create content vector index: {e}")

                    if "train=False" in str(e):
                        logger.info("Table may be empty, will retry after adding data")
                try:
                    store.create_vector_index("title_embedding")
                    logger.info("Created title vector index")
                except Exception as e:
                    logger.warning(f"Failed to create title vector index: {e}")

            try:
                store.create_fulltext_index()
                logger.info("Created fulltext index")
            except Exception as e:
                logger.warning(f"Failed to create fulltext index: {e}")

                if "empty" in str(e).lower():
                    logger.info("Table may be empty, will retry after adding data")

            logger.info("Indices creation attempted")
        except Exception as e:
            logger.warning(f"Failed to create indices: {e}")

    return store

def get_store(
    db_path: str | None = None,
    table_name: str = "articles",
) -> LanceStore:
    """获取 LanceStore 单例"""
    return create_store(db_path, table_name, create_indices=False)
