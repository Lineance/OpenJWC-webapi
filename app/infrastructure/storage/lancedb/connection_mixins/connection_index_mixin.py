from __future__ import annotations

from app.infrastructure.storage.lancedb.connection import (
    ARTICLES_TABLE_NAME,
    Any,
    ArticleFields,
    DEFAULT_DB_PATH,
    IndexConfig,
    Path,
    TYPE_CHECKING,
    _find_project_root,
    _resolve_db_path,
    _table_names,
    get_article_schema,
    lancedb,
    logger,
    logging,
    os,
    threading,
)

class ConnectionIndexMixin:
    """封装 LanceDBConnection 的单一职责方法。"""

    def create_articles_table(self, exist_ok: bool = True) -> "Table":
        """创建 articles 表并初始化索引"""
        table_names = _table_names(self._db)

        if ARTICLES_TABLE_NAME in table_names:
            if exist_ok:
                logger.info(
                    f"Table '{ARTICLES_TABLE_NAME}' already exists, returning existing"
                )
                return self.get_table(ARTICLES_TABLE_NAME)
            raise ValueError(f"Table '{ARTICLES_TABLE_NAME}' already exists")

        logger.info(f"Creating table: {ARTICLES_TABLE_NAME}")
        schema = get_article_schema()
        table = self._db.create_table(ARTICLES_TABLE_NAME, schema=schema)

        with self._table_lock:
            self._tables[ARTICLES_TABLE_NAME] = table

        logger.info(f"Table '{ARTICLES_TABLE_NAME}' created successfully")
        return table

    def create_indices(self, table_name: str = ARTICLES_TABLE_NAME) -> None:
        """为表创建索引 (向量索引 + 全文索引)"""
        table = self.get_table(table_name)

        row_count = table.count_rows()
        if row_count == 0:
            logger.warning(f"Table '{table_name}' is empty, skipping index creation")
            return

        logger.info(f"Creating indices for table '{table_name}' ({row_count} rows)")

        if row_count >= 256:
            try:
                table.create_index(
                    metric="cosine",
                    vector_column_name=ArticleFields.CONTENT_EMBEDDING,
                    index_type=IndexConfig.VECTOR_INDEX_TYPE,
                    num_partitions=min(IndexConfig.IVF_PARTITIONS, row_count),
                    num_sub_vectors=IndexConfig.PQ_SUBQUANTIZERS,
                    replace=True,
                )
                logger.info(
                    f"Vector index created on '{ArticleFields.CONTENT_EMBEDDING}' (row_count={row_count} >= 256)"
                )
            except Exception as e:
                logger.warning(f"Failed to create vector index: {e}")
        else:
            logger.info(f"Skipping vector index creation: row_count={row_count} < 256")

        try:
            for field in IndexConfig.FTS_FIELDS:
                try:
                    table.create_fts_index(
                        field,
                        use_tantivy=IndexConfig.FTS_USE_TANTIVY,
                        replace=True,
                    )
                    logger.info(f"FTS index created on '{field}'")
                except Exception as e:
                    logger.warning(
                        f"Failed to create FTS index on field '{field}': {e}"
                    )

        except Exception as e:
            logger.warning(f"Failed to create FTS indices: {e}")
