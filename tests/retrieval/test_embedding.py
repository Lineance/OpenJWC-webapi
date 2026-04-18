"""Retrieval Embedding Utilities 单元测试"""

from unittest.mock import MagicMock, patch

import pytest


class TestRetrievalEmbedderInit:
    """RetrievalEmbedder 初始化测试"""

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embedder_init(self, mock_get_embedder: MagicMock) -> None:
        """测试检索向量化器初始化"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()

        assert embedder._embedder is mock_embedder
        assert embedder._model_info == {"title": 384, "content": 1024}

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embedder_init_with_custom_embedder(self, mock_get_embedder: MagicMock) -> None:
        """测试使用自定义向量化器初始化"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        custom_embedder = MagicMock()
        embedder = RetrievalEmbedder(embedder=custom_embedder)

        assert embedder._embedder is custom_embedder


class TestEmbedQuery:
    """embed_query 测试"""

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_query_empty(self, mock_get_embedder: MagicMock) -> None:
        """测试空查询返回零向量"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        result = embedder.embed_query("", field="content")

        assert result == [0.0] * 1024

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_query_empty_title(self, mock_get_embedder: MagicMock) -> None:
        """测试空查询返回标题零向量"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        result = embedder.embed_query("", field="title")

        assert result == [0.0] * 384

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_query_empty_both(self, mock_get_embedder: MagicMock) -> None:
        """测试空查询返回两个零向量"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        title_vec, content_vec = embedder.embed_query("", field="both")

        assert title_vec == [0.0] * 384
        assert content_vec == [0.0] * 1024

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_query_title_field(self, mock_get_embedder: MagicMock) -> None:
        """测试标题字段查询"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_embedder.embed_titles.return_value = [[0.1] * 384]
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        result = embedder.embed_query("test query", field="title")

        assert result == [0.1] * 384
        mock_embedder.embed_titles.assert_called_once()

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_query_content_field(self, mock_get_embedder: MagicMock) -> None:
        """测试内容字段查询"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024, "content_model": "bge"}
        mock_embedder.embed_contents.return_value = [[0.2] * 1024]
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        result = embedder.embed_query("test query", field="content")

        assert result == [0.2] * 1024
        # Should add BGE prefix
        mock_embedder.embed_contents.assert_called_once()

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_query_both_fields(self, mock_get_embedder: MagicMock) -> None:
        """测试两个字段查询"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_embedder.embed_titles.return_value = [[0.1] * 384]
        mock_embedder.embed_contents.return_value = [[0.2] * 1024]
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        title_vec, content_vec = embedder.embed_query("test", field="both")

        assert title_vec == [0.1] * 384
        assert content_vec == [0.2] * 1024


class TestEmbedQueries:
    """embed_queries 批量查询测试"""

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_queries_empty(self, mock_get_embedder: MagicMock) -> None:
        """测试空查询列表"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        result = embedder.embed_queries([])

        assert result == []
        mock_embedder.embed_titles.assert_not_called()

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_queries_title(self, mock_get_embedder: MagicMock) -> None:
        """测试批量标题查询"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_embedder.embed_titles.return_value = [[0.1] * 384, [0.2] * 384]
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        result = embedder.embed_queries(["query1", "query2"], field="title")

        assert len(result) == 2
        assert result[0] == [0.1] * 384

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_queries_content(self, mock_get_embedder: MagicMock) -> None:
        """测试批量内容查询"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024, "content_model": "bge"}
        mock_embedder.embed_contents.return_value = [[0.1] * 1024]
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        result = embedder.embed_queries(["query"], field="content")

        assert len(result) == 1


class TestEmbedHybridQuery:
    """embed_hybrid_query 测试"""

    @patch("app.infrastructure.retrieval.utils.embedding.get_embedder")
    def test_embed_hybrid_query(self, mock_get_embedder: MagicMock) -> None:
        """测试混合查询"""
        mock_embedder = MagicMock()
        mock_embedder.get_dimensions.return_value = {"title": 384, "content": 1024}
        mock_embedder.embed_titles.return_value = [[0.1] * 384]
        mock_embedder.embed_contents.return_value = [[0.2] * 1024]
        mock_get_embedder.return_value = mock_embedder

        from app.infrastructure.retrieval.utils.embedding import RetrievalEmbedder

        embedder = RetrievalEmbedder()
        title_vec, content_vec = embedder.embed_hybrid_query("test query")

        assert title_vec == [0.1] * 384
        assert content_vec == [0.2] * 1024


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

