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
