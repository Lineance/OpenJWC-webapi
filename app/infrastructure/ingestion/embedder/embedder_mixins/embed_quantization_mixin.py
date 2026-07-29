from __future__ import annotations

from app.infrastructure.ingestion.embedder.local_embedder import (
    Any,
    BGE_PASSAGE_PREFIX,
    BGE_QUERY_PREFIX,
    CONTENT_EMBEDDING_DIM,
    CONTENT_MODEL_NAME,
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

class EmbedQuantizationMixin:
    """封装 Embedder 的单一职责方法。"""

    @classmethod
    def reset(cls) -> None:
        """重置单例实例 (仅用于测试)"""
        with cls._lock:
            if cls._instance is not None:
                cls._instance._initialized = False
                cls._instance = None
                logger.warning("Embedder reset")

    def apply_quantization(
        self, quantization_type: Literal["int8", "fp16", "none"] = "int8"
    ) -> None:
        """应用量化以减少内存占用"""
        try:
            import torch

            if quantization_type == "int8":

                if self.title_model is not None:
                    quantize_dynamic = cast(Any, torch.quantization).quantize_dynamic
                    self.title_model = quantize_dynamic(
                        self.title_model, {torch.nn.Linear}, dtype=torch.qint8
                    )
                    logger.info("Title model quantized to INT8")

                if self.content_model is not None:
                    quantize_dynamic = cast(Any, torch.quantization).quantize_dynamic
                    self.content_model = quantize_dynamic(
                        self.content_model, {torch.nn.Linear}, dtype=torch.qint8
                    )
                    logger.info("Content model quantized to INT8")

            elif quantization_type == "fp16":

                if self.title_model is not None:
                    self.title_model = self.title_model.half()
                    logger.info("Title model converted to FP16")

                if self.content_model is not None:
                    self.content_model = self.content_model.half()
                    logger.info("Content model converted to FP16")

            elif quantization_type == "none":
                logger.info("No quantization applied")

            else:
                raise ValueError(f"Unknown quantization type: {quantization_type}")

        except ImportError:
            logger.warning("PyTorch not available, quantization skipped")
        except Exception as e:
            logger.error(f"Failed to apply quantization: {e}")
