"""Retrieval Embedding Utilities 单元测试"""

from unittest.mock import MagicMock, patch

import pytest

class TestCombineVectors:
    """向量组合测试"""

    def test_combine_vectors_equal_weight(self) -> None:
        """测试等权重组合"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec1 = [1.0, 0.0]
        vec2 = [0.0, 1.0]
        result = RetrievalEmbedder.combine_vectors(vec1, vec2, 0.5, 0.5)

        expected = [0.5, 0.5]
        assert result == pytest.approx(expected, abs=0.0001)

    def test_combine_vectors_different_weight(self) -> None:
        """测试不同权重组合"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec1 = [2.0, 0.0]
        vec2 = [0.0, 2.0]
        result = RetrievalEmbedder.combine_vectors(vec1, vec2, 0.75, 0.25)

        expected = [1.5, 0.5]
        assert result == pytest.approx(expected, abs=0.0001)

    def test_combine_vectors_dimension_mismatch(self) -> None:
        """测试维度不匹配"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec1 = [1.0, 2.0, 3.0]
        vec2 = [0.0, 1.0]

        with pytest.raises(ValueError, match="dimension mismatch"):
            RetrievalEmbedder.combine_vectors(vec1, vec2)

class TestConvenienceFunctions:
    """便捷函数测试"""

    def test_cosine_similarity_function(self) -> None:
        """测试便捷余弦相似度函数"""
        from app.infrastructure.retrieval.utils.embedding import cosine_similarity

        vec = [1.0, 0.0]
        result = cosine_similarity(vec, vec)
        assert result == pytest.approx(1.0, abs=0.0001)
