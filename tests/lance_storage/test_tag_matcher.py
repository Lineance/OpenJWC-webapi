"""
测试标签匹配功能

测试 TagMatcher 的标签匹配功能。
"""

from app.infrastructure.ingestion.tag_matcher import (
    TagMatcher,
    TagMatchingConfig,
    VectorSimilarity,
)


class TestTagMatchingConfig:
    """标签匹配配置测试"""

    def test_config_constants(self) -> None:
        """测试配置常量"""
        assert TagMatchingConfig.STRICT_THRESHOLD == 0.75
        assert TagMatchingConfig.RELAXED_THRESHOLD == 0.5
        assert TagMatchingConfig.MAX_TAGS_PER_ARTICLE == 5


class TestVectorSimilarity:
    """向量相似度测试"""

    def test_cosine_similarity_same_vector(self) -> None:
        """测试相同向量的余弦相似度"""
        vec = [1.0, 0.0, 0.0]
        sim = VectorSimilarity.cosine_similarity(vec, vec)
        assert abs(sim - 1.0) < 1e-6

    def test_cosine_similarity_opposite_vectors(self) -> None:
        """测试相反向量的余弦相似度"""
        vec1 = [1.0, 0.0]
        vec2 = [-1.0, 0.0]
        sim = VectorSimilarity.cosine_similarity(vec1, vec2)
        assert abs(sim - (-1.0)) < 1e-6

    def test_cosine_similarity_orthogonal_vectors(self) -> None:
        """测试正交向量的余弦相似度"""
        vec1 = [1.0, 0.0]
        vec2 = [0.0, 1.0]
        sim = VectorSimilarity.cosine_similarity(vec1, vec2)
        assert abs(sim - 0.0) < 1e-6


class TestTagMatcher:
    """TagMatcher测试类"""

    def test_tag_matcher_initialization(self) -> None:
        """测试TagMatcher初始化"""
        matcher = TagMatcher()
        assert matcher is not None

    def test_match_tags_no_embeddings(self) -> None:
        """测试无嵌入时的标签匹配"""
        matcher = TagMatcher()
        # 当没有标签嵌入时，应该返回空列表
        result = matcher.match_tags([0.1] * 1024)
        assert isinstance(result, list)
