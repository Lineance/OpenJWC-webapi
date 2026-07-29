"""Embedding Utilities - 检索专用向量化工具"""

import logging
import sys
from pathlib import Path
from typing import Any, Literal, cast

_root = Path(__file__).resolve().parents[3]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

try:
    from app.infrastructure.ingestion.embedder_provider import (
        EmbeddingClient,
        get_embedder,
    )
except ImportError:
    from app.infrastructure.ingestion.embedder_provider import (
        EmbeddingClient,
        get_embedder,
    )

logger = logging.getLogger(__name__)

from .embedding_mixins.embedding_math_mixin import EmbeddingMathMixin

class RetrievalEmbedder(EmbeddingMathMixin):
    """检索专用向量化器"""

    def __init__(self, embedder: Any | None = None) -> None:
        """初始化检索向量化器"""
        self._embedder: EmbeddingClient = embedder or get_embedder()

        self._model_info = self._embedder.get_dimensions()

    def embed_query(
        self,
        query: str,
        field: Literal["title", "content", "both"] = "content",
        normalize: bool = True,
    ) -> list[float] | tuple[list[float], list[float]]:
        """向量化查询文本"""
        if not query:

            if field == "title":
                return [0.0] * self._model_info["title"]
            elif field == "content":
                return [0.0] * self._model_info["content"]
            else:
                return [0.0] * self._model_info["title"], [0.0] * self._model_info[
                    "content"
                ]

        content_model_name = self._model_info.get("content_model", "")

        if "bge" in content_model_name.lower():
            query_with_prefix = f"为这个句子生成表示以用于检索相关文章：{query}"
        else:
            query_with_prefix = query

        if field == "title":

            return cast("list[float]", self._embedder.embed_titles([query])[0])
        elif field == "content":
            return cast(
                "list[float]", self._embedder.embed_contents([query_with_prefix])[0]
            )
        else:
            title_vec = cast("list[float]", self._embedder.embed_titles([query])[0])
            content_vec = cast(
                "list[float]", self._embedder.embed_contents([query_with_prefix])[0]
            )
            return title_vec, content_vec

    def embed_queries(
        self,
        queries: list[str],
        field: Literal["title", "content"] = "content",
        normalize: bool = True,
        batch_size: int = 32,
    ) -> list[list[float]]:
        """批量向量化查询文本"""
        if not queries:
            return []

        content_model_name = self._model_info.get("content_model", "")

        if field == "content" and "bge" in content_model_name.lower():
            processed = [f"为这个句子生成表示以用于检索相关文章：{q}" for q in queries]
        else:
            processed = queries

        if field == "title":
            return cast(
                "list[list[float]]", self._embedder.embed_titles(processed, batch_size)
            )
        else:
            return cast(
                "list[list[float]]",
                self._embedder.embed_contents(processed, batch_size),
            )

    def embed_hybrid_query(
        self,
        query: str,
        title_weight: float = 0.3,
        content_weight: float = 0.7,
        normalize: bool = True,
    ) -> tuple[list[float], list[float]]:
        """向量化混合查询 (标题 + 正文)"""

        _ = (title_weight, content_weight)

        title_vec = cast(
            "list[float]", self.embed_query(query, field="title", normalize=normalize)
        )
        content_vec = cast(
            "list[float]", self.embed_query(query, field="content", normalize=normalize)
        )

        return title_vec, content_vec

_retrieval_embedder: RetrievalEmbedder | None = None

def get_retrieval_embedder() -> RetrievalEmbedder:
    """获取检索向量化器单例"""
    global _retrieval_embedder
    if _retrieval_embedder is None:
        _retrieval_embedder = RetrievalEmbedder()
    return _retrieval_embedder

def embed_query(
    query: str,
    field: Literal["title", "content", "both"] = "content",
) -> list[float] | tuple[list[float], list[float]]:
    """快速向量化查询"""
    return get_retrieval_embedder().embed_query(query, field)

def cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
    """快速计算余弦相似度"""
    return RetrievalEmbedder.cosine_similarity(vec1, vec2)
