"""Tag Matcher 单元测试"""

from unittest.mock import MagicMock

import pytest

class TestTagMatcherMatchTags:
    """TagMatcher.match_tags 测试"""

    def test_match_tags_invalid_embedding(self) -> None:
        """测试无效内容向量"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()
        matcher = TagMatcher(tag_repository=mock_repo)

        result = matcher.match_tags([])
        assert result == []

        result = matcher.match_tags([0.1] * 100)
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
            ("tag1", [0.0] * 1024),
        ]

        matcher = TagMatcher(tag_repository=mock_repo, threshold=0.5, enable_cache=False)
        result = matcher.match_tags([0.1] * 1024)

        assert result == []

    def test_match_tags_above_threshold(self) -> None:
        """测试有标签在阈值以上"""
        from app.infrastructure.ingestion.tag_matcher import TagMatcher

        mock_repo = MagicMock()

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

        base_vec = [0.1] * 1024
        mock_repo.get_all_embeddings.return_value = [
            (f"tag{i}", base_vec) for i in range(10)
        ]

        matcher = TagMatcher(tag_repository=mock_repo, threshold=0.3, max_tags=3, enable_cache=False)
        result = matcher.match_tags(base_vec)

        assert len(result) <= 3
