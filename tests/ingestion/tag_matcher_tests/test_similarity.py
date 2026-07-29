"""Tag Matcher 单元测试"""

from unittest.mock import MagicMock

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
