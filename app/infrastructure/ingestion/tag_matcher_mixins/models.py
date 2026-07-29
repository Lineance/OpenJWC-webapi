from __future__ import annotations

from app.infrastructure.ingestion.tag_matcher import (
    Any,
    TAG_EMBEDDING_DIM,
    TagRepository,
    get_tag_repository,
    logger,
    logging,
    math,
    np,
)

class TagMatchingConfig:
    """标签匹配配置"""

    STRICT_THRESHOLD = 0.75

    RELAXED_THRESHOLD = 0.5

    MAX_TAGS_PER_ARTICLE = 5

    SIMILARITY_METHOD = "cosine"

    ENABLE_CACHE = True

    CACHE_TTL = 3600

class VectorSimilarity:
    """向量相似度计算器"""

    @staticmethod
    def cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
        """计算余弦相似度"""

        v1 = np.array(vec1)
        v2 = np.array(vec2)

        dot_product = np.dot(v1, v2)

        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        similarity = dot_product / (norm1 * norm2)

        similarity = max(-1.0, min(1.0, similarity))
        return float(similarity)

    @staticmethod
    def euclidean_distance(vec1: list[float], vec2: list[float]) -> float:
        """计算欧几里得距离"""
        v1 = np.array(vec1)
        v2 = np.array(vec2)

        distance = np.linalg.norm(v1 - v2)
        return float(distance)

    @staticmethod
    def euclidean_similarity(vec1: list[float], vec2: list[float]) -> float:
        """将欧几里得距离转换为相似度分数"""
        distance = VectorSimilarity.euclidean_distance(vec1, vec2)

        max_distance = math.sqrt(2)
        normalized_distance = min(distance / max_distance, 1.0)

        return 1.0 - normalized_distance

    @staticmethod
    def compute_similarity(vec1: list[float], vec2: list[float], method: str = "cosine") -> float:
        """计算向量相似度"""
        if method == "cosine":
            return VectorSimilarity.cosine_similarity(vec1, vec2)
        elif method == "euclidean":
            return VectorSimilarity.euclidean_similarity(vec1, vec2)
        else:
            raise ValueError(f"Unknown similarity method: {method}")
