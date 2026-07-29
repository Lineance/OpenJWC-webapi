"""Retrieval Embedding Utilities 单元测试"""

from unittest.mock import MagicMock, patch

import pytest

class TestSimilarityToDistance:
    """相似度转距离测试"""

    def test_similarity_to_distance(self) -> None:
        """测试相似度转距离"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        result = RetrievalEmbedder.similarity_to_distance(0.5)
        assert result == 0.5

    def test_similarity_to_distance_one(self) -> None:
        """测试相似度为1"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        result = RetrievalEmbedder.similarity_to_distance(1.0)
        assert result == 0.0

    def test_similarity_to_distance_zero(self) -> None:
        """测试相似度为0"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        result = RetrievalEmbedder.similarity_to_distance(0.0)
        assert result == 1.0

class TestNormalizeVector:
    """向量归一化测试"""

    def test_normalize_vector_unit(self) -> None:
        """测试单位向量归一化"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec = [1.0, 0.0, 0.0]
        result = RetrievalEmbedder.normalize_vector(vec)
        assert result == pytest.approx([1.0, 0.0, 0.0], abs=0.0001)

    def test_normalize_vector_zero(self) -> None:
        """测试零向量"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec = [0.0, 0.0, 0.0]
        result = RetrievalEmbedder.normalize_vector(vec)
        assert result == [0.0, 0.0, 0.0]
