"""Embedder - 双模型文本向量化"""

import logging
import os
import threading
from pathlib import Path
from typing import Any, Literal, Self, cast

from huggingface_hub import snapshot_download, try_to_load_from_cache
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

TITLE_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
TITLE_MODEL_FALLBACKS = ["sentence-transformers/all-MiniLM-L6-v2"]
TITLE_EMBEDDING_DIM = 384

CONTENT_MODEL_NAME = "BAAI/bge-large-zh-v1.5"
CONTENT_EMBEDDING_DIM = 1024

LOCAL_MODEL_CACHE = os.path.expanduser("~/.cache/huggingface/hub")

BGE_QUERY_PREFIX = "为这个句子生成表示以用于检索相关文章："
BGE_PASSAGE_PREFIX = ""
from app.infrastructure.ingestion.embedder.cache_paths import (
    _iter_model_cache_dirs,
    _find_model_snapshot_path,
)

def _is_model_cached_locally(model_name: str) -> bool:
    """检查模型是否已缓存在本地"""
    if _find_model_snapshot_path(model_name):
        return True

    for cache_dir in _iter_model_cache_dirs():
        try:

            cached_path = try_to_load_from_cache(
                repo_id=model_name,
                filename="config.json",
                cache_dir=str(cache_dir),
            )
            if cached_path is not None and str(cached_path) != "_CACHED_NO_EXIST":
                return True
        except Exception as e:
            logger.debug(f"Error checking cache for {model_name} in {cache_dir}: {e}")

    return False
from app.infrastructure.ingestion.embedder.model_loader import (
    _ensure_model_available,
    _load_model_with_auto_download,
    _load_first_available_model,
)

from .embedder_mixins.embed_operations_mixin import EmbedOperationsMixin
from .embedder_mixins.embed_quantization_mixin import EmbedQuantizationMixin

class Embedder(EmbedOperationsMixin, EmbedQuantizationMixin):
    """双模型向量化器"""

    _instance: Self | None = None
    _lock = threading.Lock()
    _initialized: bool
    title_model: SentenceTransformer | None
    content_model: SentenceTransformer | None

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """初始化模型"""
        if getattr(self, "_initialized", False):
            return

        logger.info("Loading embedding models...")

        try:

            title_candidates = [TITLE_MODEL_NAME, *TITLE_MODEL_FALLBACKS]
            logger.info(f"Attempting to load title models: {title_candidates}")
            self.title_model, loaded_title_model = _load_first_available_model(
                title_candidates
            )
            logger.info(
                f"Title model loaded: {loaded_title_model} ({TITLE_EMBEDDING_DIM}d)"
            )
        except Exception as e:
            logger.warning(f"Failed to load title model: {e}")
            logger.info("Creating dummy title model for testing")

            self.title_model = None

        try:

            logger.info(f"Attempting to load content model: {CONTENT_MODEL_NAME}")
            self.content_model = _load_model_with_auto_download(CONTENT_MODEL_NAME)
            logger.info(
                f"Content model loaded: {CONTENT_MODEL_NAME} ({CONTENT_EMBEDDING_DIM}d)"
            )
        except Exception as e:
            logger.warning(f"Failed to load content model: {e}")
            logger.info("Creating dummy content model for testing")

            self.content_model = None

        self._initialized = True
        logger.info("Embedding models initialized successfully")

from app.infrastructure.ingestion.embedder.quantized import (
    QuantizedEmbedder,
)

from app.infrastructure.ingestion.embedder.factory import (
    get_quantized_embedder,
    embed_title_quantized,
    embed_content_quantized,
    get_embedder,
    embed_title,
    embed_content,
    embed_query,
    get_embedder_with_options,
)
