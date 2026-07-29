"""Retrieval Engine 单元测试"""

from unittest.mock import MagicMock

import pytest

class TestRetrievalEngineVectorSearch:
    """RetrievalEngine _vector_search 方法测试"""

    def test_vector_search_with_precomputed_vector(self) -> None:
        """测试使用预计算向量的向量搜索"""
        mock_store = MagicMock()
        mock_store.vector_search.return_value = [{"news_id": "1"}]
        mock_embedder = MagicMock()

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)

        query_obj = MagicMock()
        query_obj.vector_query = [0.1] * 384
        query_obj.vector_field = "content_embedding"
        query_obj.limit = 10
        query_obj.build_where_clause.return_value = None

        results = engine._vector_search(query_obj)

        assert len(results) == 1
        mock_store.vector_search.assert_called_once()

    def test_vector_search_with_keyword(self) -> None:
        """测试使用关键词的向量搜索"""
        mock_store = MagicMock()
        mock_store.vector_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)

        query_obj = MagicMock()
        query_obj.vector_query = None
        query_obj.keyword = "测试"
        query_obj.vector_field = "title_embedding"
        query_obj.limit = 10
        query_obj.build_where_clause.return_value = None

        engine._vector_search(query_obj)

        mock_store.vector_search.assert_called_once()

    def test_vector_search_with_tuple_vector(self) -> None:
        """测试返回元组向量时取第一个"""
        mock_store = MagicMock()
        mock_store.vector_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = ([0.1] * 384, [0.2] * 1024)

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)

        query_obj = MagicMock()
        query_obj.vector_query = None
        query_obj.keyword = "测试"
        query_obj.vector_field = "title_embedding"
        query_obj.limit = 10
        query_obj.build_where_clause.return_value = None

        engine._vector_search(query_obj)

class TestRetrievalEngineFulltextSearch:
    """RetrievalEngine _fulltext_search 方法测试"""

    def test_fulltext_search_with_keyword(self) -> None:
        """测试带关键词的全文搜索"""
        mock_store = MagicMock()
        mock_store.fulltext_search.return_value = [{"news_id": "1"}]
        mock_embedder = MagicMock()

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)

        query_obj = MagicMock()
        query_obj.keyword = "测试"
        query_obj.search_fields = ["title", "content"]
        query_obj.limit = 10
        query_obj.build_where_clause.return_value = None

        results = engine._fulltext_search(query_obj)

        assert len(results) == 1
        mock_store.fulltext_search.assert_called_once()

    def test_fulltext_search_empty_keyword(self) -> None:
        """测试空关键词返回空"""
        mock_store = MagicMock()
        mock_embedder = MagicMock()

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)

        query_obj = MagicMock()
        query_obj.keyword = ""
        query_obj.search_fields = ["title"]
        query_obj.limit = 10
        query_obj.build_where_clause.return_value = None

        results = engine._fulltext_search(query_obj)

        assert results == []
        mock_store.fulltext_search.assert_not_called()
