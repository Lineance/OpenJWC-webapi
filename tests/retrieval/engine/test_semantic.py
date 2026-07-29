"""Retrieval Engine 单元测试"""

from unittest.mock import MagicMock

import pytest

class TestRetrievalEngineSemanticSearch:
    """RetrievalEngine semantic_search 方法测试"""

    def test_semantic_search_returns_results(self) -> None:
        """测试语义搜索返回结果"""
        mock_store = MagicMock()
        mock_store.vector_search.return_value = [
            {"news_id": "1", "title": "测试", "title_embedding": [0.1] * 384},
        ]
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384
        mock_embedder.cosine_similarity.return_value = 0.85

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.semantic_search("测试")

        assert result["search_type"] == "semantic"
        assert "field" in result

    def test_semantic_search_content_field(self) -> None:
        """测试语义搜索内容字段"""
        mock_store = MagicMock()
        mock_store.vector_search.return_value = [
            {"news_id": "1", "content": "测试", "content_embedding": [0.1] * 1024},
        ]
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = ([0.1] * 384, [0.1] * 1024)
        mock_embedder.cosine_similarity.return_value = 0.85

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.semantic_search("测试", field="content")

        assert result["field"] == "content"

    def test_semantic_search_with_threshold(self) -> None:
        """测试带阈值的语义搜索"""
        mock_store = MagicMock()
        mock_store.vector_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.semantic_search("测试", similarity_threshold=0.8)

        assert result["similarity_threshold"] == 0.8

class TestRetrievalEngineKeywordSearch:
    """RetrievalEngine keyword_search 方法测试"""

    def test_keyword_search_returns_results(self) -> None:
        """测试关键词搜索返回结果"""
        mock_store = MagicMock()
        mock_store.fulltext_search.return_value = [
            {"news_id": "1", "title": "测试文章"},
        ]

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        result = engine.keyword_search("测试")

        assert result["search_type"] == "keyword"
        assert len(result["results"]) > 0

    def test_keyword_search_match_any(self) -> None:
        """测试任意匹配模式"""
        mock_store = MagicMock()
        mock_store.fulltext_search.return_value = [
            {"news_id": "1", "title": "测试文章", "content_text": "内容"},
        ]

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        result = engine.keyword_search("测试 内容", match_type="any")

        assert result["match_type"] == "any"

    def test_keyword_search_match_all(self) -> None:
        """测试全部匹配模式"""
        mock_store = MagicMock()
        mock_store.fulltext_search.return_value = [
            {"news_id": "1", "title": "测试", "content_text": "文章"},
        ]

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        result = engine.keyword_search("测试 文章", match_type="all")

        assert result["match_type"] == "all"

    def test_keyword_search_match_phrase(self) -> None:
        """测试短语匹配模式"""
        mock_store = MagicMock()
        mock_store.fulltext_search.return_value = [
            {"news_id": "1", "title": "测试文章", "content_text": "这是一篇测试文章"},
        ]

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        result = engine.keyword_search("测试文章", match_type="phrase")

        assert result["match_type"] == "phrase"

    def test_keyword_search_default_fields(self) -> None:
        """测试默认搜索字段"""
        mock_store = MagicMock()
        mock_store.fulltext_search.return_value = []

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        result = engine.keyword_search("测试")

        assert result["fields"] == ["title", "content_text"]
