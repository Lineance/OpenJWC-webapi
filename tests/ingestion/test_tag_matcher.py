"""Tag Matcher 单元测试"""

from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest


class TestVectorSimilarity:
    """VectorSimilarity 测试"""

    def test_cosine_similarity_identical(self) -> None:
        """测试相同向量的余弦相似度"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec = [1.0, 2.0, 3.0]
        result = VectorSimilarity.cosine_similarity(vec, vec)
        assert result == pytest.approx(1.0, abs=0.0001)

    def test_cosine_similarity_opposite(self) -> None:
        """测试相反向量的余弦相似度"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec1 = [1.0, 2.0, 3.0]
        vec2 = [-1.0, -2.0, -3.0]
        result = VectorSimilarity.cosine_similarity(vec1, vec2)
        assert result == pytest.approx(-1.0, abs=0.0001)

    def test_cosine_similarity_zero_vector(self) -> None:
        """测试零向量"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec1 = [0.0, 0.0, 0.0]
        vec2 = [1.0, 2.0, 3.0]
        result = VectorSimilarity.cosine_similarity(vec1, vec2)
        assert result == 0.0

    def test_cosine_similarity_different(self) -> None:
        """测试不同向量"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec1 = [1.0, 0.0, 0.0]
        vec2 = [0.0, 1.0, 0.0]
        result = VectorSimilarity.cosine_similarity(vec1, vec2)
        assert result == pytest.approx(0.0, abs=0.0001)

    def test_euclidean_distance_identical(self) -> None:
        """测试相同向量的欧几里得距离"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec = [1.0, 2.0, 3.0]
        result = VectorSimilarity.euclidean_distance(vec, vec)
        assert result == pytest.approx(0.0, abs=0.0001)

    def test_euclidean_distance(self) -> None:
        """测试不同向量的欧几里得距离"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec1 = [0.0, 0.0, 0.0]
        vec2 = [3.0, 4.0, 0.0]
        result = VectorSimilarity.euclidean_distance(vec1, vec2)
        assert result == pytest.approx(5.0, abs=0.0001)

    def test_euclidean_similarity(self) -> None:
        """测试欧几里得相似度"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec1 = [1.0, 0.0, 0.0]
        vec2 = [0.0, 1.0, 0.0]
        result = VectorSimilarity.euclidean_similarity(vec1, vec2)
        assert 0 <= result <= 1

    def test_compute_similarity_cosine(self) -> None:
        """测试使用余弦方法计算相似度"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec = [1.0, 0.0, 0.0]
        result = VectorSimilarity.compute_similarity(vec, vec, method="cosine")
        assert result == pytest.approx(1.0, abs=0.0001)

    def test_compute_similarity_euclidean(self) -> None:
        """测试使用欧几里得方法计算相似度"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec = [1.0, 0.0, 0.0]
        result = VectorSimilarity.compute_similarity(vec, vec, method="euclidean")
        assert result == pytest.approx(1.0, abs=0.0001)

    def test_compute_similarity_invalid_method(self) -> None:
        """测试无效的相似度方法"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        with pytest.raises(ValueError, match="Unknown similarity method"):
            VectorSimilarity.compute_similarity([1.0], [1.0], method="invalid")


class TestTagMatchingConfig:
    """TagMatchingConfig 测试"""

    def test_default_threshold(self) -> None:
        """测试默认阈值"""
        from app.infrastructure.ingestion.tag_matcher import TagMatchingConfig

        assert TagMatchingConfig.STRICT_THRESHOLD == 0.75
        assert TagMatchingConfig.RELAXED_THRESHOLD == 0.5

    def test_max_tags_per_article(self) -> None:
        """测试最大标签数"""
        from app.infrastructure.ingestion.tag_matcher import TagMatchingConfig

        assert TagMatchingConfig.MAX_TAGS_PER_ARTICLE == 5

    def test_similarity_method(self) -> None:
        """测试相似度方法"""
        from app.infrastructure.ingestion.tag_matcher import TagMatchingConfig

        assert TagMatchingConfig.SIMILARITY_METHOD == "cosine"


class TestTagMatcherInit:
    """TagMatcher 初始化测试"""

    def test_matcher_init_strict(self) -> None:
        """测试严格模式初始化"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        matcher = TagMatcher(strict=True)
        assert matcher._strict is True
        assert matcher._threshold == 0.75

    def test_matcher_init_relaxed(self) -> None:
        """测试宽松模式初始化"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        matcher = TagMatcher(strict=False)
        assert matcher._strict is False
        assert matcher._threshold == 0.5

    def test_matcher_init_custom_threshold(self) -> None:
        """测试自定义阈值"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        matcher = TagMatcher(threshold=0.8)
        assert matcher._threshold == 0.8

    def test_matcher_init_custom_max_tags(self) -> None:
        """测试自定义最大标签数"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        matcher = TagMatcher(max_tags=10)
        assert matcher._max_tags == 10

    def test_matcher_init_custom_method(self) -> None:
        """测试自定义相似度方法"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        matcher = TagMatcher(similarity_method="euclidean")
        assert matcher._similarity_method == "euclidean"

    def test_matcher_init_with_repo(self) -> None:
        """测试使用自定义 repository"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        matcher = TagMatcher(tag_repository=mock_repo)
        assert matcher._repo is mock_repo


class TestTagMatcherGetEmbeddings:
    """TagMatcher _get_tag_embeddings 测试"""

    def test_get_tag_embeddings_no_cache(self) -> None:
        """测试获取标签向量（无缓存）"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        mock_repo.get_all_embeddings.return_value = [
            ("tag1", [0.1] * 1024),
            ("tag2", [0.2] * 1024),
        ]

        matcher = TagMatcher(tag_repository=mock_repo, enable_cache=False)
        embeddings = matcher._get_tag_embeddings()

        assert len(embeddings) == 2
        assert embeddings[0][0] == "tag1"

    def test_get_tag_embeddings_invalid_dimension(self) -> None:
        """测试无效向量维度"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        mock_repo.get_all_embeddings.return_value = [
            ("tag1", [0.1] * 100),  # Wrong dimension
            ("tag2", [0.2] * 1024),  # Correct dimension
        ]

        matcher = TagMatcher(tag_repository=mock_repo, enable_cache=False)
        embeddings = matcher._get_tag_embeddings()

        # Only valid dimension should be included
        assert len(embeddings) == 1
        assert embeddings[0][0] == "tag2"

    def test_get_tag_embeddings_empty(self) -> None:
        """测试无标签向量"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        mock_repo.get_all_embeddings.return_value = []

        matcher = TagMatcher(tag_repository=mock_repo, enable_cache=False)
        embeddings = matcher._get_tag_embeddings()

        assert embeddings == []


class TestTagMatcherMatchTags:
    """TagMatcher.match_tags 测试"""

    def test_match_tags_invalid_embedding(self) -> None:
        """测试无效内容向量"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        matcher = TagMatcher(tag_repository=mock_repo)

        result = matcher.match_tags([])  # Empty
        assert result == []

        result = matcher.match_tags([0.1] * 100)  # Wrong dimension
        assert result == []

    def test_match_tags_no_tags(self) -> None:
        """测试无标签可用"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        mock_repo.get_all_embeddings.return_value = []

        matcher = TagMatcher(tag_repository=mock_repo, enable_cache=False)
        result = matcher.match_tags([0.1] * 1024)

        assert result == []

    def test_match_tags_below_threshold(self) -> None:
        """测试所有标签都在阈值以下"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        mock_repo.get_all_embeddings.return_value = [
            ("tag1", [0.0] * 1024),  # Zero vector - no similarity
        ]

        matcher = TagMatcher(tag_repository=mock_repo, threshold=0.5, enable_cache=False)
        result = matcher.match_tags([0.1] * 1024)

        assert result == []

    def test_match_tags_above_threshold(self) -> None:
        """测试有标签在阈值以上"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        # Create vectors that are identical
        vec = [0.1] * 1024
        mock_repo.get_all_embeddings.return_value = [
            ("tag1", vec),
        ]

        matcher = TagMatcher(tag_repository=mock_repo, threshold=0.3, enable_cache=False)
        result = matcher.match_tags(vec)

        assert "tag1" in result

    def test_match_tags_respects_max_tags(self) -> None:
        """测试最大标签数限制"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        # Create many similar vectors
        base_vec = [0.1] * 1024
        mock_repo.get_all_embeddings.return_value = [
            (f"tag{i}", base_vec) for i in range(10)
        ]

        matcher = TagMatcher(tag_repository=mock_repo, threshold=0.3, max_tags=3, enable_cache=False)
        result = matcher.match_tags(base_vec)

        assert len(result) <= 3
