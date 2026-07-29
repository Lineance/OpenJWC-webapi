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

class EmbedOperationsMixin:
    """封装 Embedder 的单一职责方法。"""

    def embed_titles(self, texts: list[str], batch_size: int = 32) -> list[list[float]]:
        """向量化标题"""
        if not texts:
            return []

        if self.title_model is None:
            import random

            logger.warning("Using random vectors for titles (model not loaded)")
            return [
                [random.uniform(-0.1, 0.1) for _ in range(TITLE_EMBEDDING_DIM)]
                for _ in texts
            ]

        try:

            embeddings = self.title_model.encode(
                texts,
                batch_size=batch_size,
                show_progress_bar=False,
                normalize_embeddings=True,
            )
            return cast("list[list[float]]", embeddings.tolist())
        except Exception as e:
            logger.error(f"Failed to embed titles: {e}")

            return [[0.0] * TITLE_EMBEDDING_DIM for _ in texts]

    def embed_contents(
        self, texts: list[str], batch_size: int = 32
    ) -> list[list[float]]:
        """向量化正文"""
        if not texts:
            return []

        if self.content_model is None:
            import random

            logger.warning("Using random vectors for contents (model not loaded)")
            return [
                [random.uniform(-0.1, 0.1) for _ in range(CONTENT_EMBEDDING_DIM)]
                for _ in texts
            ]

        try:

            prefixed_texts = [BGE_PASSAGE_PREFIX + text for text in texts]

            embeddings = self.content_model.encode(
                prefixed_texts,
                batch_size=batch_size,
                show_progress_bar=False,
                normalize_embeddings=True,
            )
            return cast("list[list[float]]", embeddings.tolist())
        except Exception as e:
            logger.error(f"Failed to embed contents: {e}")

            return [[0.0] * CONTENT_EMBEDDING_DIM for _ in texts]

    def embed_query(self, query: str) -> list[float]:
        """向量化查询文本 (用于检索)"""

        if self.content_model is None:
            import random

            logger.warning("Using random vector for query (model not loaded)")
            return [random.uniform(-0.1, 0.1) for _ in range(CONTENT_EMBEDDING_DIM)]

        try:

            prefixed_query = BGE_QUERY_PREFIX + query

            embedding = self.content_model.encode(
                prefixed_query,
                show_progress_bar=False,
                normalize_embeddings=True,
            )
            return embedding.tolist()
        except Exception as e:
            logger.error(f"Failed to embed query: {e}")

            return [0.0] * CONTENT_EMBEDDING_DIM

    def embed_batch(
        self,
        titles: list[str],
        contents: list[str],
        batch_size: int = 32,
    ) -> tuple[list[list[float]], list[list[float]]]:
        """批量向量化标题和正文"""
        if len(titles) != len(contents):
            raise ValueError("Titles and contents must have the same length")

        title_vectors = self.embed_titles(titles, batch_size)
        content_vectors = self.embed_contents(contents, batch_size)

        return title_vectors, content_vectors

    def get_dimensions(self) -> dict[str, Any]:
        """获取向量维度信息"""
        return {
            "title": TITLE_EMBEDDING_DIM,
            "content": CONTENT_EMBEDDING_DIM,
            "title_model": TITLE_MODEL_NAME,
            "content_model": CONTENT_MODEL_NAME,
        }
