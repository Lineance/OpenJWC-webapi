"""
Embedding Utilities - 检索专用向量化工具

为检索查询提供向量化功能，支持查询前缀和批量处理。

Responsibilities:
    - 查询文本向量化 (带 BGE 前缀)
    - 批量查询向量化
    - 向量归一化和相似度计算
"""

import logging
import sys
from pathlib import Path
from typing import Any, Literal, cast

# 添加项目根目录到 Python 路径
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


# =============================================================================
# 检索向量化器
# =============================================================================


class RetrievalEmbedder:
    """
    检索专用向量化器

    扩展基础 Embedder，添加检索专用功能:
    - 查询前缀处理 (BGE 模型)
    - 批量查询向量化
    - 多字段向量化 (标题/正文)
    """

    def __init__(self, embedder: Any | None = None) -> None:
        """
        初始化检索向量化器

        Args:
            embedder: 基础向量化器
        """
        self._embedder: EmbeddingClient = embedder or get_embedder()
        # 获取模型信息
        self._model_info = self._embedder.get_dimensions()

    def embed_query(
        self,
        query: str,
        field: Literal["title", "content", "both"] = "content",
        normalize: bool = True,
    ) -> list[float] | tuple[list[float], list[float]]:
        """
        向量化查询文本

        Args:
            query: 查询文本
            field: 目标字段 (title/content/both)
            normalize: 是否归一化

        Returns:
            向量或向量元组
        """
        if not query:
            # 返回零向量
            if field == "title":
                return [0.0] * self._model_info["title"]
            elif field == "content":
                return [0.0] * self._model_info["content"]
            else:  # both
                return [0.0] * self._model_info["title"], [0.0] * self._model_info[
                    "content"
                ]

        # 检查是否使用 BGE 模型
        content_model_name = self._model_info.get("content_model", "")

        # 添加 BGE 查询前缀
        if "bge" in content_model_name.lower():
            query_with_prefix = f"为这个句子生成表示以用于检索相关文章：{query}"
        else:
            query_with_prefix = query

        if field == "title":
            # 标题查询通常不需要特殊前缀
            return cast("list[float]", self._embedder.embed_titles([query])[0])
        elif field == "content":
            return cast(
                "list[float]", self._embedder.embed_contents([query_with_prefix])[0]
            )
        else:  # both
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
        """
        批量向量化查询文本

        Args:
            queries: 查询文本列表
            field: 目标字段
            normalize: 是否归一化
            batch_size: 批处理大小

        Returns:
            向量列表
        """
        if not queries:
            return []

        # 检查是否使用 BGE 模型
        content_model_name = self._model_info.get("content_model", "")

        # 添加 BGE 查询前缀
        if field == "content" and "bge" in content_model_name.lower():
            processed = [f"为这个句子生成表示以用于检索相关文章：{q}" for q in queries]
        else:
            processed = queries

        if field == "title":
            return cast(
                "list[list[float]]", self._embedder.embed_titles(processed, batch_size)
            )
        else:  # content
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
        """
        向量化混合查询 (标题 + 正文)

        Args:
            query: 查询文本
            title_weight: 标题向量权重 (当前未使用，保留以供扩展)
            content_weight: 正文向量权重 (当前未使用，保留以供扩展)
            normalize: 是否归一化

        Returns:
            (标题向量, 正文向量) 元组
        """
        # 忽略未使用的参数，避免警告
        _ = (title_weight, content_weight)

        # 明确告知类型检查器返回值应为 list[float]
        title_vec = cast(
            "list[float]", self.embed_query(query, field="title", normalize=normalize)
        )
        content_vec = cast(
            "list[float]", self.embed_query(query, field="content", normalize=normalize)
        )

        return title_vec, content_vec

    # =========================================================================
    # 相似度计算
    # =========================================================================

    @staticmethod
    def cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
        """
        计算余弦相似度

        Args:
            vec1: 向量1
            vec2: 向量2

        Returns:
            余弦相似度 (-1 到 1)
        """
        import numpy as np

        v1 = np.array(vec1)
        v2 = np.array(vec2)

        dot = np.dot(v1, v2)
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return float(dot / (norm1 * norm2))

    @staticmethod
    def euclidean_distance(vec1: list[float], vec2: list[float]) -> float:
        """
        计算欧几里得距离

        Args:
            vec1: 向量1
            vec2: 向量2

        Returns:
            欧几里得距离
        """
        import numpy as np

        v1 = np.array(vec1)
        v2 = np.array(vec2)

        return float(np.linalg.norm(v1 - v2))

    @staticmethod
    def similarity_to_distance(similarity: float) -> float:
        """
        将相似度转换为距离

        Args:
            similarity: 余弦相似度

        Returns:
            距离 (0 到 2)
        """
        return 1.0 - similarity

    # =========================================================================
    # 向量操作
    # =========================================================================

    @staticmethod
    def normalize_vector(vec: list[float]) -> list[float]:
        """
        L2 归一化向量

        Args:
            vec: 输入向量

        Returns:
            归一化后的向量
        """
        import numpy as np

        v = np.array(vec)
        norm = np.linalg.norm(v)

        if norm == 0:
            return vec

        return cast("list[float]", (v / norm).tolist())

    @staticmethod
    def combine_vectors(
        vec1: list[float],
        vec2: list[float],
        weight1: float = 0.5,
        weight2: float = 0.5,
    ) -> list[float]:
        """
        组合两个向量

        Args:
            vec1: 向量1
            vec2: 向量2
            weight1: 向量1权重
            weight2: 向量2权重

        Returns:
            组合后的向量
        """
        import numpy as np

        v1 = np.array(vec1)
        v2 = np.array(vec2)

        # 确保维度相同
        if len(v1) != len(v2):
            raise ValueError(f"Vector dimension mismatch: {len(v1)} != {len(v2)}")

        combined = weight1 * v1 + weight2 * v2
        return cast("list[float]", combined.tolist())


# =============================================================================
# 单例和便捷函数
# =============================================================================


_retrieval_embedder: RetrievalEmbedder | None = None


def get_retrieval_embedder() -> RetrievalEmbedder:
    """
    获取检索向量化器单例

    Returns:
        RetrievalEmbedder 实例
    """
    global _retrieval_embedder
    if _retrieval_embedder is None:
        _retrieval_embedder = RetrievalEmbedder()
    return _retrieval_embedder


def embed_query(
    query: str,
    field: Literal["title", "content", "both"] = "content",
) -> list[float] | tuple[list[float], list[float]]:
    """
    快速向量化查询

    Args:
        query: 查询文本
        field: 目标字段

    Returns:
        向量或向量元组
    """
    return get_retrieval_embedder().embed_query(query, field)


def cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
    """
    快速计算余弦相似度

    Args:
        vec1: 向量1
        vec2: 向量2

    Returns:
        余弦相似度
    """
    return RetrievalEmbedder.cosine_similarity(vec1, vec2)
