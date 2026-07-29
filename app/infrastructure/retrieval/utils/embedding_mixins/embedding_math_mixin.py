from __future__ import annotations

from app.infrastructure.retrieval.utils.embedding import (
    Any,
    Literal,
    Path,
    _root,
    cast,
    logger,
    logging,
    sys,
)

class EmbeddingMathMixin:
    """封装 RetrievalEmbedder 的单一职责方法。"""

    @staticmethod
    def cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
        """计算余弦相似度"""
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
        """计算欧几里得距离"""
        import numpy as np

        v1 = np.array(vec1)
        v2 = np.array(vec2)

        return float(np.linalg.norm(v1 - v2))

    @staticmethod
    def similarity_to_distance(similarity: float) -> float:
        """将相似度转换为距离"""
        return 1.0 - similarity

    @staticmethod
    def normalize_vector(vec: list[float]) -> list[float]:
        """L2 归一化向量"""
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
        """组合两个向量"""
        import numpy as np

        v1 = np.array(vec1)
        v2 = np.array(vec2)

        if len(v1) != len(v2):
            raise ValueError(f"Vector dimension mismatch: {len(v1)} != {len(v2)}")

        combined = weight1 * v1 + weight2 * v2
        return cast("list[float]", combined.tolist())
