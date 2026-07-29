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

class StoreIndexMixin:
    """封装 LanceStore 的单一职责方法。"""

    def create_vector_index(
        self,
        field: str = "content_embedding",
        index_type: Literal[
            "IVF_FLAT", "IVF_SQ", "IVF_PQ", "IVF_HNSW_SQ", "IVF_HNSW_PQ", "IVF_RQ"
        ] = "IVF_PQ",
        num_partitions: int = 256,
        num_sub_vectors: int = 64,
        adaptive: bool = True,
        min_data_for_training: int = 10,
        min_partitions: int = 4,
        max_partitions: int = 256,
        enable_brute_force_fallback: bool = True,
        **kwargs: Any,
    ) -> None:
        """创建向量索引（支持暴力检索回退）"""
        try:
            if field not in ["title_embedding", "content_embedding"]:
                raise ValueError(f"Invalid vector field: {field}")

            existing = self.list_indices()
            if any(idx["column"] == field for idx in existing):
                logger.info(f"Vector index for {field} already exists")
                return

            data_count = self.count()

            if enable_brute_force_fallback and data_count < 256:
                logger.info(f"数据量不足256条 ({data_count} < 256)，回退到暴力向量检索")
                logger.info("  说明：IVF-PQ索引需要至少256条数据进行训练")
                logger.info("  暴力检索：使用线性扫描，计算查询向量与所有向量的相似度")
                logger.info(f"  性能：{data_count}条数据，毫秒级响应")
                return

            final_num_partitions = num_partitions
            final_index_type = index_type

            if adaptive:
                if data_count < min_data_for_training:
                    logger.info(
                        f"数据量不足({data_count} < {min_data_for_training})，跳过索引创建"
                    )
                    logger.info(
                        "    注意：少量数据时IVF-PQ索引需要训练，建议积累数据后重试"
                    )
                    return
                else:

                    import math

                    calculated_partitions = int(
                        min(max_partitions, math.sqrt(data_count) * 2)
                    )
                    calculated_partitions = max(min_partitions, calculated_partitions)

                    calculated_partitions = min(calculated_partitions, data_count)

                    if calculated_partitions != num_partitions:
                        logger.info(
                            f"自适应调整参数: {data_count}条数据 -> {calculated_partitions}个分区"
                        )
                        final_num_partitions = calculated_partitions

            logger.info(
                f"创建{final_index_type}索引 for {field} (数据量: {data_count}, 分区数: {final_num_partitions})"
            )

            self.table.create_index(
                vector_column_name=field,
                index_type=final_index_type,
                metric="cosine",
                replace=True,
                num_partitions=final_num_partitions,
                num_sub_vectors=num_sub_vectors,
                **kwargs,
            )
            logger.info(f"成功创建 {final_index_type} 索引 for {field}")

        except Exception as e:
            error_msg = str(e)
            if (
                "KMeans cannot train" in error_msg
                or "Not enough rows to train PQ" in error_msg
            ):
                logger.warning(f"向量索引训练失败（数据量不足）: {error_msg}")
                logger.warning("建议：积累更多数据（至少256条）或使用暴力检索")
                if enable_brute_force_fallback:
                    logger.info("已启用暴力检索回退，向量搜索将使用线性扫描")
            else:
                logger.error(f"创建向量索引失败: {e}")
                raise

    def create_fulltext_index(
        self,
        fields: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """创建全文索引"""
        try:
            if fields is None:
                fields = Article.get_searchable_fields()

            for field in fields:
                try:
                    self.table.create_fts_index(
                        field,
                        replace=True,
                        **kwargs,
                    )
                    logger.info(f"Created fulltext index for field: '{field}'")
                except Exception as e:

                    error_str = str(e).lower()
                    if "already exists" in error_str and "index" in error_str:
                        logger.info(
                            f"FTS index for field '{field}' already exists, skipping creation"
                        )
                    else:
                        raise
        except Exception as e:
            logger.error(f"Failed to create fulltext index: {e}")
            raise

    def list_indices(self) -> list[dict[str, Any]]:
        """列出所有索引"""
        try:
            indices = self.table.list_indices()

            return [
                {
                    "name": idx.name,
                    "type": idx.index_type,
                    "column": getattr(idx, "column", None)
                    or getattr(idx, "columns", None),
                }
                for idx in indices
            ]
        except Exception as e:
            logger.warning(f"Failed to list indices: {e}")
            return []

    def optimize_indices(self) -> None:
        """优化索引"""
        try:
            self.table.optimize()
            logger.info("Indices optimized")
        except Exception as e:
            logger.error(f"Failed to optimize indices: {e}")
            raise
