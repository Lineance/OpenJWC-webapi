"""Retrieval Engine 单元测试"""

from unittest.mock import MagicMock

import pytest

class TestRetrievalEngineInit:
    """RetrievalEngine 初始化测试"""

    def test_engine_init_with_mocked_store(self) -> None:
        """测试使用 mock store 初始化引擎"""
        mock_store = MagicMock()
        mock_store.hybrid_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)

        assert engine._store is mock_store
        assert engine._embedder is mock_embedder

    def test_engine_init_checks_store_type(self) -> None:
        """测试引擎验证 store 类型"""
        from app.infrastructure.retrieval.engine import RetrievalEngine

        with pytest.raises(TypeError, match="Expected LanceStore"):
            RetrievalEngine(store="invalid_store")

    def test_engine_init_checks_store_methods(self) -> None:
        """测试引擎验证 store 方法"""
        from app.infrastructure.retrieval.engine import RetrievalEngine

        mock_store = MagicMock()
        del mock_store.hybrid_search

        with pytest.raises(TypeError, match="not a LanceStore"):
            RetrievalEngine(store=mock_store)

class TestRetrievalEngineSearch:
    """RetrievalEngine search 方法测试"""

    def test_search_returns_structure(self) -> None:
        """测试搜索返回正确结构"""
        mock_store = MagicMock()
        mock_store.hybrid_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.search("测试")

        assert "query" in result
        assert "search_type" in result
        assert "total" in result
        assert "results" in result
        assert result["query"] == "测试"

    def test_search_with_limit(self) -> None:
        """测试带 limit 的搜索"""
        mock_store = MagicMock()
        mock_store.hybrid_search.return_value = [
            {"news_id": "1", "title": "测试1"},
            {"news_id": "2", "title": "测试2"},
        ]
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.search("测试", limit=1)

        assert result["limit"] == 1
        assert len(result["results"]) == 1

    def test_search_with_offset(self) -> None:
        """测试带 offset 的搜索"""
        mock_store = MagicMock()
        mock_store.hybrid_search.return_value = [
            {"news_id": "1", "title": "测试1"},
            {"news_id": "2", "title": "测试2"},
            {"news_id": "3", "title": "测试3"},
        ]
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.search("测试", limit=10, offset=1)

        assert result["offset"] == 1

    def test_search_vector_type(self) -> None:
        """测试向量搜索类型"""
        mock_store = MagicMock()
        mock_store.vector_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.search("测试", search_type="vector")

        assert result["search_type"] == "vector"
        mock_store.vector_search.assert_called_once()

    def test_search_fulltext_type(self) -> None:
        """测试全文搜索类型"""
        mock_store = MagicMock()
        mock_store.fulltext_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.search("测试", search_type="fulltext")

        assert result["search_type"] == "fulltext"
        mock_store.fulltext_search.assert_called_once()

    def test_search_hybrid_type(self) -> None:
        """测试混合搜索类型"""
        mock_store = MagicMock()
        mock_store.hybrid_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.search("测试", search_type="hybrid")

        assert result["search_type"] == "hybrid"
        mock_store.hybrid_search.assert_called_once()

    def test_search_invalid_limit(self) -> None:
        """测试无效 limit 参数"""
        mock_store = MagicMock()
        mock_store.hybrid_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)

        with pytest.raises(ValueError, match="limit must be between"):
            engine.search("test", limit=0)

    def test_search_invalid_offset(self) -> None:
        """测试无效 offset 参数"""
        mock_store = MagicMock()
        mock_store.hybrid_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)

        with pytest.raises(ValueError, match="offset must be >="):
            engine.search("test", offset=-1)
