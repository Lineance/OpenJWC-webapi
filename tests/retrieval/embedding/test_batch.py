"""Retrieval Embedding Utilities 单元测试"""

from unittest.mock import MagicMock, patch

import pytest

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
