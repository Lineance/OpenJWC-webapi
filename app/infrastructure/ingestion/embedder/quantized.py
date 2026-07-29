from __future__ import annotations

from app.infrastructure.ingestion.embedder.local_embedder import (
    Any,
    BGE_PASSAGE_PREFIX,
    BGE_QUERY_PREFIX,
    CONTENT_EMBEDDING_DIM,
    CONTENT_MODEL_NAME,
    EmbedOperationsMixin,
    EmbedQuantizationMixin,
    Embedder,
    LOCAL_MODEL_CACHE,
    Literal,
    Path,
    Self,
    SentenceTransformer,
    TITLE_EMBEDDING_DIM,
    TITLE_MODEL_FALLBACKS,
    TITLE_MODEL_NAME,
    _ensure_model_available,
    _find_model_snapshot_path,
    _is_model_cached_locally,
    _iter_model_cache_dirs,
    _load_first_available_model,
    _load_model_with_auto_download,
    cast,
    logger,
    logging,
    os,
    snapshot_download,
    threading,
    try_to_load_from_cache,
)

class QuantizedEmbedder(Embedder):
    """量化向量化器 - 预量化模型以减少内存占用"""

    def __init__(
        self,
        quantization_type: Literal["int8", "fp16", "none"] = "int8",
        quantize_on_init: bool = True,
    ) -> None:
        """初始化量化向量化器"""
        super().__init__()
        self._quantization_type = quantization_type

        if quantize_on_init:
            self.apply_quantization(quantization_type)
            logger.info(
                f"QuantizedEmbedder initialized with {quantization_type} quantization"
            )

    @property
    def quantization_type(self) -> str:
        """获取量化类型"""
        return self._quantization_type

    def reapply_quantization(self, new_type: Literal["int8", "fp16", "none"]) -> None:
        """重新应用量化（重置模型后）"""
        self._quantization_type = new_type
        self.apply_quantization(new_type)
        logger.info(f"Quantization reapplied with type: {new_type}")

    def get_memory_saving(self) -> dict[str, Any]:
        """获取内存节省估计"""
        base_memory = {
            "title_model": 384 * 4,
            "content_model": 1024 * 4,
        }

        if self._quantization_type == "int8":

            return {
                "quantization_type": "int8",
                "estimated_saving_percentage": 75,
                "title_model_size_kb": base_memory["title_model"] * 0.25 / 1024,
                "content_model_size_kb": base_memory["content_model"] * 0.25 / 1024,
                "total_saving_kb": (
                    base_memory["title_model"] + base_memory["content_model"]
                )
                * 0.75
                / 1024,
            }
        elif self._quantization_type == "fp16":

            return {
                "quantization_type": "fp16",
                "estimated_saving_percentage": 50,
                "title_model_size_kb": base_memory["title_model"] * 0.5 / 1024,
                "content_model_size_kb": base_memory["content_model"] * 0.5 / 1024,
                "total_saving_kb": (
                    base_memory["title_model"] + base_memory["content_model"]
                )
                * 0.5
                / 1024,
            }
        else:
            return {
                "quantization_type": "none",
                "estimated_saving_percentage": 0,
                "title_model_size_kb": base_memory["title_model"] / 1024,
                "content_model_size_kb": base_memory["content_model"] / 1024,
                "total_saving_kb": 0,
            }
