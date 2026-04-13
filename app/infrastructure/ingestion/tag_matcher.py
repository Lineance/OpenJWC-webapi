"""
Tag Matcher - 标签向量匹配器

使用余弦相似度将文章内容向量与标签描述向量进行匹配，支持多标签匹配和严格阈值过滤。

Responsibilities:
    - 内容向量与标签向量相似度计算
    - 多标签匹配（严格模式）
    - 标签过滤和排序
    - 批量匹配优化
"""

import logging
import math
from typing import Any

import numpy as np

from app.infrastructure.storage.lancedb.tag_repository import TagRepository, get_tag_repository
from app.infrastructure.storage.lancedb.tag_schema import TAG_EMBEDDING_DIM

logger = logging.getLogger(__name__)


# =============================================================================
# 匹配配置
# =============================================================================


class TagMatchingConfig:
    """标签匹配配置"""

    # 严格匹配阈值 (0.75)
    STRICT_THRESHOLD = 0.75

    # 宽松匹配阈值
    RELAXED_THRESHOLD = 0.5

    # 最大返回标签数
    MAX_TAGS_PER_ARTICLE = 5

    # 相似度计算方法
    SIMILARITY_METHOD = "cosine"  # "cosine" 或 "euclidean"

    # 是否启用缓存
    ENABLE_CACHE = True

    # 缓存过期时间（秒）
    CACHE_TTL = 3600


# =============================================================================
# 向量相似度计算
# =============================================================================


class VectorSimilarity:
    """向量相似度计算器"""

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
        # 转换为 numpy 数组
        v1 = np.array(vec1)
        v2 = np.array(vec2)

        # 计算点积
        dot_product = np.dot(v1, v2)

        # 计算范数
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)

        # 避免除以零
        if norm1 == 0 or norm2 == 0:
            return 0.0

        # 计算余弦相似度
        similarity = dot_product / (norm1 * norm2)

        # 确保在合理范围内
        similarity = max(-1.0, min(1.0, similarity))
        return float(similarity)

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
        v1 = np.array(vec1)
        v2 = np.array(vec2)

        distance = np.linalg.norm(v1 - v2)
        return float(distance)

    @staticmethod
    def euclidean_similarity(vec1: list[float], vec2: list[float]) -> float:
        """
        将欧几里得距离转换为相似度分数

        Args:
            vec1: 向量1
            vec2: 向量2

        Returns:
            相似度分数 (0 到 1)
        """
        distance = VectorSimilarity.euclidean_distance(vec1, vec2)

        # 归一化：距离越大，相似度越小
        # 使用 sigmoid 函数转换
        max_distance = math.sqrt(2)  # 归一化向量的最大欧几里得距离
        normalized_distance = min(distance / max_distance, 1.0)

        return 1.0 - normalized_distance

    @staticmethod
    def compute_similarity(vec1: list[float], vec2: list[float], method: str = "cosine") -> float:
        """
        计算向量相似度

        Args:
            vec1: 向量1
            vec2: 向量2
            method: 计算方法 ("cosine" 或 "euclidean")

        Returns:
            相似度分数
        """
        if method == "cosine":
            return VectorSimilarity.cosine_similarity(vec1, vec2)
        elif method == "euclidean":
            return VectorSimilarity.euclidean_similarity(vec1, vec2)
        else:
            raise ValueError(f"Unknown similarity method: {method}")


# =============================================================================
# 标签匹配器
# =============================================================================


class TagMatcher:
    """
    标签向量匹配器

    使用余弦相似度将文章内容向量与标签描述向量进行匹配。
    支持严格阈值过滤和多标签返回。

    Features:
        - 余弦相似度计算
        - 严格/宽松匹配模式
        - 多标签匹配
        - 标签缓存优化
        - 批量匹配支持

    Usage:
        >>> matcher = TagMatcher(strict=True)
        >>> tags = matcher.match_tags(content_embedding)
        >>> # 或批量匹配
        >>> batch_tags = matcher.match_batch(content_embeddings)
    """

    def __init__(
        self,
        tag_repository: TagRepository | None = None,
        strict: bool = True,
        threshold: float | None = None,
        max_tags: int = TagMatchingConfig.MAX_TAGS_PER_ARTICLE,
        similarity_method: str = TagMatchingConfig.SIMILARITY_METHOD,
        enable_cache: bool = TagMatchingConfig.ENABLE_CACHE,
    ):
        """
        初始化标签匹配器

        Args:
            tag_repository: 标签仓库实例
            strict: 是否使用严格匹配模式
            threshold: 自定义阈值（覆盖 strict 设置）
            max_tags: 最大返回标签数
            similarity_method: 相似度计算方法
            enable_cache: 是否启用缓存
        """
        self._repo = tag_repository or get_tag_repository()
        self._strict = strict
        self._threshold = threshold or (
            TagMatchingConfig.STRICT_THRESHOLD if strict else TagMatchingConfig.RELAXED_THRESHOLD
        )
        self._max_tags = max_tags
        self._similarity_method = similarity_method
        self._enable_cache = enable_cache

        # 缓存标签向量
        self._tag_cache: list[tuple[str, list[float]]] | None = None
        self._cache_timestamp: float = 0.0

        logger.info(
            f"TagMatcher initialized: strict={strict}, threshold={self._threshold:.2f}, "
            f"max_tags={max_tags}, method={similarity_method}"
        )

    def _get_tag_embeddings(self) -> list[tuple[str, list[float]]]:
        """
        获取所有标签的向量表示

        Returns:
            (tag_id, embedding) 元组列表
        """
        # 检查缓存
        if self._enable_cache and self._tag_cache is not None:
            import time

            # 检查缓存是否过期
            current_time = time.time()
            if current_time - self._cache_timestamp < TagMatchingConfig.CACHE_TTL:
                logger.debug("Using cached tag embeddings")
                return self._tag_cache

        try:
            # 从仓库获取所有标签向量
            tag_embeddings = self._repo.get_all_embeddings()

            # 验证向量维度
            valid_embeddings = []
            for tag_id, embedding in tag_embeddings:
                if embedding and len(embedding) == TAG_EMBEDDING_DIM:
                    valid_embeddings.append((tag_id, embedding))
                else:
                    logger.warning(
                        f"Invalid embedding for tag {tag_id}: dimension={len(embedding) if embedding else 'empty'}"
                    )

            # 更新缓存
            if self._enable_cache:
                self._tag_cache = valid_embeddings
                import time

                self._cache_timestamp = time.time()
                logger.debug(f"Cached {len(valid_embeddings)} tag embeddings")

            return valid_embeddings
        except Exception as e:
            logger.error(f"Failed to get tag embeddings: {e}")
            return []

    def match_tags(self, content_embedding: list[float]) -> list[str]:
        """
        为单个内容向量匹配标签

        Args:
            content_embedding: 内容向量 (1024 维)

        Returns:
            匹配的标签 ID 列表
        """
        # 验证输入向量
        if not content_embedding or len(content_embedding) != TAG_EMBEDDING_DIM:
            logger.warning(
                f"Invalid content embedding dimension: {len(content_embedding) if content_embedding else 'empty'}"
            )
            return []

        try:
            # 获取所有标签向量
            tag_embeddings = self._get_tag_embeddings()
            if not tag_embeddings:
                logger.warning("No tag embeddings available")
                return []

            # 计算相似度
            similarities = []
            for tag_id, tag_embedding in tag_embeddings:
                similarity = VectorSimilarity.compute_similarity(
                    content_embedding, tag_embedding, self._similarity_method
                )

                # 应用阈值过滤
                if similarity >= self._threshold:
                    similarities.append((tag_id, similarity))

            # 按相似度降序排序
            similarities.sort(key=lambda x: x[1], reverse=True)

            # 返回前 N 个标签
            matched_tags = [tag_id for tag_id, _ in similarities[: self._max_tags]]

            logger.debug(
                f"Matched {len(matched_tags)} tags for content embedding "
                f"(threshold={self._threshold:.2f})"
            )

            return matched_tags

        except Exception as e:
            logger.error(f"Failed to match tags: {e}")
            return []

    def match_tags_with_scores(self, content_embedding: list[float]) -> list[tuple[str, float]]:
        """
        为单个内容向量匹配标签并返回相似度分数

        Args:
            content_embedding: 内容向量

        Returns:
            (tag_id, similarity_score) 元组列表
        """
        # 验证输入向量
        if not content_embedding or len(content_embedding) != TAG_EMBEDDING_DIM:
            logger.warning(
                f"Invalid content embedding dimension: {len(content_embedding) if content_embedding else 'empty'}"
            )
            return []

        try:
            # 获取所有标签向量
            tag_embeddings = self._get_tag_embeddings()
            if not tag_embeddings:
                logger.warning("No tag embeddings available")
                return []

            # 计算相似度
            similarities = []
            for tag_id, tag_embedding in tag_embeddings:
                similarity = VectorSimilarity.compute_similarity(
                    content_embedding, tag_embedding, self._similarity_method
                )

                # 应用阈值过滤
                if similarity >= self._threshold:
                    similarities.append((tag_id, similarity))

            # 按相似度降序排序
            similarities.sort(key=lambda x: x[1], reverse=True)

            # 返回前 N 个标签及分数
            return similarities[: self._max_tags]

        except Exception as e:
            logger.error(f"Failed to match tags with scores: {e}")
            return []

    def match_batch(self, content_embeddings: list[list[float]]) -> list[list[str]]:
        """
        批量匹配标签

        Args:
            content_embeddings: 内容向量列表

        Returns:
            每个内容向量的匹配标签 ID 列表
        """
        if not content_embeddings:
            return []

        # 验证所有输入向量
        valid_indices = []
        valid_embeddings = []
        for i, embedding in enumerate(content_embeddings):
            if embedding and len(embedding) == TAG_EMBEDDING_DIM:
                valid_indices.append(i)
                valid_embeddings.append(embedding)
            else:
                logger.warning(
                    f"Invalid embedding at index {i}: dimension={len(embedding) if embedding else 'empty'}"
                )

        if not valid_embeddings:
            return [[] for _ in range(len(content_embeddings))]

        try:
            # 获取所有标签向量
            tag_embeddings = self._get_tag_embeddings()
            if not tag_embeddings:
                logger.warning("No tag embeddings available")
                return [[] for _ in range(len(content_embeddings))]

            # 批量计算相似度
            all_matched_tags = []
            for embedding in valid_embeddings:
                matched_tags = []
                for tag_id, tag_embedding in tag_embeddings:
                    similarity = VectorSimilarity.compute_similarity(
                        embedding, tag_embedding, self._similarity_method
                    )

                    if similarity >= self._threshold:
                        matched_tags.append((tag_id, similarity))

                # 按相似度排序并取前 N 个
                matched_tags.sort(key=lambda x: x[1], reverse=True)
                tag_ids = [tag_id for tag_id, _ in matched_tags[: self._max_tags]]
                all_matched_tags.append(tag_ids)

            # 构建完整结果（包含无效输入的空结果）
            final_results: list[list[str]] = [[] for _ in range(len(content_embeddings))]
            for idx, tags in zip(valid_indices, all_matched_tags, strict=False):
                final_results[idx] = tags

            logger.debug(
                f"Batch matched tags for {len(valid_embeddings)} embeddings "
                f"(threshold={self._threshold:.2f})"
            )

            return final_results

        except Exception as e:
            logger.error(f"Failed to batch match tags: {e}")
            return [[] for _ in range(len(content_embeddings))]

    def match_batch_with_scores(
        self, content_embeddings: list[list[float]]
    ) -> list[list[tuple[str, float]]]:
        """
        批量匹配标签并返回相似度分数

        Args:
            content_embeddings: 内容向量列表

        Returns:
            每个内容向量的匹配标签及分数列表
        """
        if not content_embeddings:
            return []

        # 验证所有输入向量
        valid_indices = []
        valid_embeddings = []
        for i, embedding in enumerate(content_embeddings):
            if embedding and len(embedding) == TAG_EMBEDDING_DIM:
                valid_indices.append(i)
                valid_embeddings.append(embedding)
            else:
                logger.warning(
                    f"Invalid embedding at index {i}: dimension={len(embedding) if embedding else 'empty'}"
                )

        if not valid_embeddings:
            return [[] for _ in range(len(content_embeddings))]

        try:
            # 获取所有标签向量
            tag_embeddings = self._get_tag_embeddings()
            if not tag_embeddings:
                logger.warning("No tag embeddings available")
                return [[] for _ in range(len(content_embeddings))]

            # 批量计算相似度
            all_matched_tags = []
            for embedding in valid_embeddings:
                matched_tags = []
                for tag_id, tag_embedding in tag_embeddings:
                    similarity = VectorSimilarity.compute_similarity(
                        embedding, tag_embedding, self._similarity_method
                    )

                    if similarity >= self._threshold:
                        matched_tags.append((tag_id, similarity))

                # 按相似度排序并取前 N 个
                matched_tags.sort(key=lambda x: x[1], reverse=True)
                all_matched_tags.append(matched_tags[: self._max_tags])

            # 构建完整结果（包含无效输入的空结果）
            final_results: list[list[tuple[str, float]]] = [
                [] for _ in range(len(content_embeddings))
            ]
            for idx, tags in zip(valid_indices, all_matched_tags, strict=False):
                final_results[idx] = tags

            return final_results

        except Exception as e:
            logger.error(f"Failed to batch match tags with scores: {e}")
            return [[] for _ in range(len(content_embeddings))]

    def clear_cache(self) -> None:
        """清空标签向量缓存"""
        self._tag_cache = None
        self._cache_timestamp = 0.0
        logger.debug("Tag cache cleared")

    def update_config(
        self,
        strict: bool | None = None,
        threshold: float | None = None,
        max_tags: int | None = None,
        similarity_method: str | None = None,
    ) -> None:
        """
        更新匹配配置

        Args:
            strict: 是否使用严格匹配
            threshold: 相似度阈值
            max_tags: 最大标签数
            similarity_method: 相似度计算方法
        """
        if strict is not None:
            self._strict = strict
            if threshold is None:  # 仅当未显式指定阈值时更新
                self._threshold = (
                    TagMatchingConfig.STRICT_THRESHOLD
                    if strict
                    else TagMatchingConfig.RELAXED_THRESHOLD
                )

        if threshold is not None:
            self._threshold = threshold

        if max_tags is not None:
            self._max_tags = max_tags

        if similarity_method is not None:
            self._similarity_method = similarity_method

        logger.info(
            f"TagMatcher config updated: strict={self._strict}, threshold={self._threshold:.2f}, "
            f"max_tags={self._max_tags}, method={self._similarity_method}"
        )

    def get_config(self) -> dict[str, Any]:
        """获取当前配置"""
        return {
            "strict": self._strict,
            "threshold": self._threshold,
            "max_tags": self._max_tags,
            "similarity_method": self._similarity_method,
            "enable_cache": self._enable_cache,
        }


# =============================================================================
# 便捷函数
# =============================================================================


def get_tag_matcher(
    strict: bool = True,
    threshold: float | None = None,
    max_tags: int = TagMatchingConfig.MAX_TAGS_PER_ARTICLE,
) -> TagMatcher:
    """
    获取标签匹配器实例

    Args:
        strict: 是否使用严格匹配
        threshold: 自定义阈值
        max_tags: 最大标签数

    Returns:
        TagMatcher 实例
    """
    return TagMatcher(
        strict=strict,
        threshold=threshold,
        max_tags=max_tags,
    )


def match_content_tags(content_embedding: list[float], strict: bool = True) -> list[str]:
    """
    快速匹配内容标签

    Args:
        content_embedding: 内容向量
        strict: 是否使用严格匹配

    Returns:
        匹配的标签 ID 列表
    """
    matcher = get_tag_matcher(strict=strict)
    return matcher.match_tags(content_embedding)


def batch_match_content_tags(
    content_embeddings: list[list[float]], strict: bool = True
) -> list[list[str]]:
    """
    批量快速匹配内容标签

    Args:
        content_embeddings: 内容向量列表
        strict: 是否使用严格匹配

    Returns:
        每个内容向量的匹配标签列表
    """
    matcher = get_tag_matcher(strict=strict)
    return matcher.match_batch(content_embeddings)
