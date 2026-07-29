"""测试标签匹配功能"""

from app.infrastructure.ingestion.tag_matcher import (
    TagMatcher,
    TagMatchingConfig,
    VectorSimilarity,
)

class TestTagMatcher:
    """TagMatcher测试类"""

    def test_tag_matcher_initialization(self) -> None:
        """测试TagMatcher初始化"""
        matcher = TagMatcher()
        assert matcher is not None

    def test_match_tags_no_embeddings(self) -> None:
        """测试无嵌入时的标签匹配"""
        matcher = TagMatcher()

        result = matcher.match_tags([0.1] * 1024)
        assert isinstance(result, list)
