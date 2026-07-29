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
    QuantizedEmbedder,
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

def get_quantized_embedder(
    quantization_type: Literal["int8", "fp16", "none"] = "int8",
) -> QuantizedEmbedder:
    """获取量化向量化器单例"""

    Embedder.reset()
    return QuantizedEmbedder(quantization_type=quantization_type)

def embed_title_quantized(
    text: str, quantization_type: Literal["int8", "fp16", "none"] = "int8"
) -> list[float]:
    """使用量化模型向量化单个标题"""
    embedder = get_quantized_embedder(quantization_type)
    result = embedder.embed_titles([text])
    return result[0] if result else [0.0] * TITLE_EMBEDDING_DIM

def embed_content_quantized(
    text: str, quantization_type: Literal["int8", "fp16", "none"] = "int8"
) -> list[float]:
    """使用量化模型向量化单个正文"""
    embedder = get_quantized_embedder(quantization_type)
    result = embedder.embed_contents([text])
    return result[0] if result else [0.0] * CONTENT_EMBEDDING_DIM

def get_embedder() -> Embedder:
    """获取 Embedder 单例"""
    return Embedder()

def embed_title(text: str) -> list[float]:
    """向量化单个标题"""
    embedder = get_embedder()
    result = embedder.embed_titles([text])
    return result[0] if result else [0.0] * TITLE_EMBEDDING_DIM

def embed_content(text: str) -> list[float]:
    """向量化单个正文"""
    embedder = get_embedder()
    result = embedder.embed_contents([text])
    return result[0] if result else [0.0] * CONTENT_EMBEDDING_DIM

def embed_query(text: str) -> list[float]:
    """向量化查询文本"""
    embedder = get_embedder()
    return embedder.embed_query(text)

def get_embedder_with_options(
    use_quantization: bool = True,
    quantization_type: Literal["int8", "fp16"] = "int8",
) -> Embedder:
    """获取可配置的向量化器"""
    if use_quantization:
        return get_quantized_embedder(quantization_type)
    else:
        return get_embedder()
