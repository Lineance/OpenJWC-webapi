"""Retrieval Embedding Utilities 单元测试"""

from unittest.mock import MagicMock, patch

import pytest

class TestCosineSimilarity:
    """余弦相似度测试"""

    def test_cosine_similarity_identical(self) -> None:
        """测试相同向量"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec = [1.0, 2.0, 3.0]
        result = RetrievalEmbedder.cosine_similarity(vec, vec)
        assert result == pytest.approx(1.0, abs=0.0001)

    def test_cosine_similarity_opposite(self) -> None:
        """测试相反向量"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec1 = [1.0, 2.0, 3.0]
        vec2 = [-1.0, -2.0, -3.0]
        result = RetrievalEmbedder.cosine_similarity(vec1, vec2)
        assert result == pytest.approx(-1.0, abs=0.0001)

    def test_cosine_similarity_zero_vector(self) -> None:
        """测试零向量"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec1 = [0.0, 0.0, 0.0]
        vec2 = [1.0, 2.0, 3.0]
        result = RetrievalEmbedder.cosine_similarity(vec1, vec2)
        assert result == 0.0

    def test_cosine_similarity_orthogonal(self) -> None:
        """测试正交向量"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec1 = [1.0, 0.0, 0.0]
        vec2 = [0.0, 1.0, 0.0]
        result = RetrievalEmbedder.cosine_similarity(vec1, vec2)
        assert result == pytest.approx(0.0, abs=0.0001)

class TestEuclideanDistance:
    """欧几里得距离测试"""

    def test_euclidean_distance_identical(self) -> None:
        """测试相同向量"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec = [1.0, 2.0, 3.0]
        result = RetrievalEmbedder.euclidean_distance(vec, vec)
        assert result == pytest.approx(0.0, abs=0.0001)

    def test_euclidean_distance_3d(self) -> None:
        """测试3D向量距离"""
        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        vec1 = [0.0, 0.0, 0.0]
        vec2 = [3.0, 4.0, 0.0]
        result = RetrievalEmbedder.euclidean_distance(vec1, vec2)
        assert result == pytest.approx(5.0, abs=0.0001)
