from __future__ import annotations

from app.infrastructure.storage.lancedb.tag_repository import (
    Any,
    LanceDBConnection,
    RepositorySystemError,
    TAGS_TABLE_NAME,
    TAG_EMBEDDING_DIM,
    TagFields,
    TagIndexConfig,
    TagRecord,
    annotations,
    datetime,
    get_connection,
    get_tag_schema,
    lancedb,
    logger,
    logging,
)

class TagIndexMixin:
    """封装 TagRepository 的单一职责方法。"""

    def bulk_update(self, tag_records: list[TagRecord]) -> int:
        """批量更新标签"""
        if not tag_records:
            return 0

        try:

            update_data = []
            for record in tag_records:
                data = record.to_dict()
                data[TagFields.UPDATED_AT] = datetime.now()
                update_data.append(data)

            self._table.merge_insert(
                TagFields.TAG_ID
            ).when_matched_update_all().execute(update_data)
            logger.info(f"Bulk updated {len(tag_records)} tags")
            return len(tag_records)
        except Exception as e:
            logger.error(f"Failed to bulk update tags: {e}")
            return 0

    def create_indices(self) -> bool:
        """创建标签表的索引"""
        try:
            row_count = self._table.count_rows()
            if row_count == 0:
                logger.warning("Table is empty, skipping index creation")
                return False

            logger.info(f"Creating indices for tags table ({row_count} rows)")

            indices_created = False

            if row_count >= 256:
                try:
                    self._table.create_index(
                        metric="cosine",
                        vector_column_name=TagFields.EMBEDDING,
                        index_type=TagIndexConfig.VECTOR_INDEX_TYPE,
                        num_partitions=min(TagIndexConfig.IVF_PARTITIONS, row_count),
                        num_sub_vectors=TagIndexConfig.PQ_SUBQUANTIZERS,
                        replace=True,
                    )
                    logger.info(
                        f"Vector index created on '{TagFields.EMBEDDING}' (row_count={row_count} >= 256)"
                    )
                    indices_created = True
                except Exception as e:
                    logger.error(f"Failed to create vector index: {e}")

            else:
                logger.info(
                    f"Skipping vector index creation: row_count={row_count} < 256"
                )

            for field in TagIndexConfig.FTS_FIELDS:
                try:
                    self._table.create_fts_index(
                        field,
                        use_tantivy=True,
                        replace=True,
                    )
                    logger.info(f"FTS index created on '{field}'")
                    indices_created = True
                except Exception as e:
                    logger.error(f"Failed to create FTS index on field '{field}': {e}")

            return indices_created
        except Exception as e:
            logger.error(f"Failed to create indices: {e}")
            return False
