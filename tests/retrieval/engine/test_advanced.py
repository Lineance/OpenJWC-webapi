"""Retrieval Engine 单元测试"""

from unittest.mock import MagicMock

import pytest

class TestRetrievalEngineAdvancedSearch:
    """RetrievalEngine advanced_search 方法测试"""

    def test_advanced_search_returns_results(self) -> None:
        """测试高级搜索返回结果"""
        mock_store = MagicMock()
        mock_store.hybrid_search.return_value = [
            {
                "news_id": "1",
                "title": "测试",
                "title_embedding": [0.1] * 384,
                "content_embedding": [0.1] * 1024,
            }
        ]
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = ([0.1] * 384, [0.1] * 1024)
        mock_embedder.cosine_similarity.return_value = 0.8

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.advanced_search(
            "测试",
            vector_weight=0.6,
            keyword_weight=0.4,
        )

        assert "vector_weight" in result or result["search_type"] == "advanced"

    def test_advanced_search_weights(self) -> None:
        """测试高级搜索权重"""
        mock_store = MagicMock()
        mock_store.hybrid_search.return_value = []
        mock_embedder = MagicMock()
        mock_embedder.embed_query.return_value = [0.1] * 384

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=mock_embedder)
        result = engine.advanced_search(
            "测试",
            vector_weight=0.7,
            keyword_weight=0.3,
            title_weight=0.4,
            content_weight=0.6,
        )

        assert result["weights"]["vector"] == 0.7
        assert result["weights"]["keyword"] == 0.3

class TestRetrievalEngineGetDocument:
    """RetrievalEngine get_document 方法测试"""

    def test_get_document_found(self) -> None:
        """测试获取已存在的文档"""
        mock_store = MagicMock()
        mock_store.table.search.return_value.where.return_value.limit.return_value.to_list.return_value = [
            {"news_id": "1", "title": "测试"}
        ]

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        doc = engine.get_document("1")

        assert doc is not None
        assert doc["news_id"] == "1"

    def test_get_document_not_found(self) -> None:
        """测试获取不存在的文档"""
        mock_store = MagicMock()
        mock_store.table.search.return_value.where.return_value.limit.return_value.to_list.return_value = []

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        doc = engine.get_document("nonexistent")

        assert doc is None

    def test_get_document_error(self) -> None:
        """测试获取文档出错"""
        mock_store = MagicMock()
        mock_store.table.search.side_effect = Exception("DB error")

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        doc = engine.get_document("1")

        assert doc is None
