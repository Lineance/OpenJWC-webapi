"""测试标签系统"""

class TestVectorSimilarityIntegration:
    """向量相似度集成测试"""

    def test_cosine_similarity_integration(self) -> None:
        """测试余弦相似度计算"""
        from app.infrastructure.ingestion.tag_matcher import VectorSimilarity

        vec1 = [1.0, 0.0, 0.0]
        vec2 = [0.5, 0.5, 0.0]
        sim = VectorSimilarity.cosine_similarity(vec1, vec2)

        assert 0 < sim < 1

class TestTagMatchingIntegration:
    """标签匹配集成测试"""

    def test_tag_matcher_with_mock(self) -> None:
        """测试TagMatcher与mock仓库"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        matcher = TagMatcher()

        result = matcher.match_tags([0.0] * 1024)
        assert isinstance(result, list)
