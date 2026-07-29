"""Tag Matcher 单元测试"""

from unittest.mock import MagicMock

import pytest

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
            ("tag1", [0.1] * 100),
            ("tag2", [0.2] * 1024),
        ]

        matcher = TagMatcher(tag_repository=mock_repo, enable_cache=False)
        embeddings = matcher._get_tag_embeddings()

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
