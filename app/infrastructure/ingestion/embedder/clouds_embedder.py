"""
Cloud Embedder - 云端文本向量化

接口对齐 local_embedder:
- embed_titles -> 384 维
- embed_contents -> 1024 维
- embed_query -> 1024 维
- embed_batch / get_dimensions / reset

实现方式保留云端调用逻辑:
- ZhipuAI embedding-3
- tenacity 指数退避重试
- 认证失败后自动重建 client
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Self, cast

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from zhipuai import ZhipuAI
from zhipuai.core._errors import (
    APIAuthenticationError,
    APIConnectionError,
    APIInternalError,
    APIReachLimitError,
    APITimeoutError,
)

from app.infrastructure.storage.sqlite.sql_db_service import db

logger = logging.getLogger(__name__)

# 与 LanceDB schema 对齐的维度常量
TITLE_EMBEDDING_DIM = 384
CONTENT_EMBEDDING_DIM = 1024

TITLE_MODEL_NAME = "zhipu/embedding-3"
CONTENT_MODEL_NAME = "zhipu/embedding-3"
CLOUD_EMBEDDING_MODEL = "embedding-3"


class CloudEmbedder:
    """Cloud embedding client with local-compatible API."""

    _instance: Self | None = None
    _lock = threading.Lock()
    _initialized: bool

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return

        self._model_name = CLOUD_EMBEDDING_MODEL
        self._client = self._build_client()
        self._initialized = True
        logger.info("CloudEmbedder initialized")

    @staticmethod
    def _build_client() -> ZhipuAI:
        api_key = str(db.get_system_setting("zhipu_api_key") or "").strip()
        return ZhipuAI(api_key=api_key, timeout=60)

    def reinitialize_client(self) -> None:
        """Recreate client using latest API key from settings."""
        logger.info("Reinitializing ZhipuAI client")
        self._client = self._build_client()

    @retry(
        retry=retry_if_exception_type(
            (
                APIConnectionError,
                APITimeoutError,
                APIInternalError,
                APIReachLimitError,
            )
        ),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(4),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _call_embedding(self, text: str) -> list[float]:
        try:
            response = self._client.embeddings.create(
                model=self._model_name,
                input=text,
            )
        except APIAuthenticationError as e:
            logger.warning(
                "ZhipuAI auth failed, retrying with refreshed API key: %s", e
            )
            self.reinitialize_client()
            response = self._client.embeddings.create(
                model=self._model_name,
                input=text,
            )

        return cast("list[float]", response.data[0].embedding)

    @staticmethod
    def _fit_dimension(vector: list[float], target_dim: int) -> list[float]:
        if len(vector) == target_dim:
            return vector
        if len(vector) > target_dim:
            return vector[:target_dim]
        return vector + [0.0] * (target_dim - len(vector))

    def _embed_one(self, text: str, target_dim: int) -> list[float]:
        vector = self._call_embedding(text)
        return self._fit_dimension(vector, target_dim)

    def embed_titles(self, texts: list[str], batch_size: int = 32) -> list[list[float]]:
        if not texts:
            return []

        _ = batch_size  # Cloud API is called per text in current implementation.
        vectors: list[list[float]] = []
        for text in texts:
            try:
                vectors.append(self._embed_one(text, TITLE_EMBEDDING_DIM))
            except Exception as e:
                logger.error("Failed to embed title: %s", e)
                vectors.append([0.0] * TITLE_EMBEDDING_DIM)
        return vectors

    def embed_contents(
        self, texts: list[str], batch_size: int = 32
    ) -> list[list[float]]:
        if not texts:
            return []

        _ = batch_size
        vectors: list[list[float]] = []
        for text in texts:
            try:
                vectors.append(self._embed_one(text, CONTENT_EMBEDDING_DIM))
            except Exception as e:
                logger.error("Failed to embed content: %s", e)
                vectors.append([0.0] * CONTENT_EMBEDDING_DIM)
        return vectors

    def embed_query(self, query: str) -> list[float]:
        if not query:
            return [0.0] * CONTENT_EMBEDDING_DIM

        try:
            return self._embed_one(query, CONTENT_EMBEDDING_DIM)
        except Exception as e:
            logger.error("Failed to embed query: %s", e)
            return [0.0] * CONTENT_EMBEDDING_DIM

    def embed_batch(
        self,
        titles: list[str],
        contents: list[str],
        batch_size: int = 32,
    ) -> tuple[list[list[float]], list[list[float]]]:
        if len(titles) != len(contents):
            raise ValueError("Titles and contents must have the same length")

        title_vectors = self.embed_titles(titles, batch_size=batch_size)
        content_vectors = self.embed_contents(contents, batch_size=batch_size)
        return title_vectors, content_vectors

    def get_dimensions(self) -> dict[str, Any]:
        return {
            "title": TITLE_EMBEDDING_DIM,
            "content": CONTENT_EMBEDDING_DIM,
            "title_model": TITLE_MODEL_NAME,
            "content_model": CONTENT_MODEL_NAME,
        }

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            if cls._instance is not None:
                cls._instance._initialized = False
                cls._instance = None
                logger.warning("CloudEmbedder reset")


# Keep naming style close to local_embedder for compatibility.
Embedder = CloudEmbedder


def get_embedder() -> CloudEmbedder:
    return CloudEmbedder()


def embed_title(text: str) -> list[float]:
    result = get_embedder().embed_titles([text])
    return result[0] if result else [0.0] * TITLE_EMBEDDING_DIM


def embed_content(text: str) -> list[float]:
    result = get_embedder().embed_contents([text])
    return result[0] if result else [0.0] * CONTENT_EMBEDDING_DIM


def embed_query(text: str) -> list[float]:
    return get_embedder().embed_query(text)
