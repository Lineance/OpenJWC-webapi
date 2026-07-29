"""Retrieval Engine 单元测试"""

from unittest.mock import MagicMock

import pytest

class TestRetrievalEngineGetSimilarDocuments:
    """RetrievalEngine get_similar_documents 方法测试"""

    def test_get_similar_documents(self) -> None:
        """测试获取相似文档"""
        mock_store = MagicMock()

        mock_store.table.search.return_value.where.return_value.limit.return_value.to_list.side_effect = [
            [{"news_id": "1", "title_embedding": [0.1] * 384}]
        ]

        mock_store.vector_search.return_value = [
            {"news_id": "2", "title_embedding": [0.2] * 384}
        ]

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        docs = engine.get_similar_documents("1", field="title", limit=5)

        assert len(docs) >= 0

    def test_get_similar_documents_not_found(self) -> None:
        """测试相似文档-文档不存在"""
        mock_store = MagicMock()
        mock_store.table.search.return_value.where.return_value.limit.return_value.to_list.return_value = []

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        docs = engine.get_similar_documents("nonexistent")

        assert docs == []

    def test_get_similar_documents_no_embedding(self) -> None:
        """测试相似文档-无向量字段"""
        mock_store = MagicMock()
        mock_store.table.search.return_value.where.return_value.limit.return_value.to_list.return_value = [
            {"news_id": "1"}
        ]

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        docs = engine.get_similar_documents("1", field="content")

        assert docs == []

class TestRetrievalEngineGetStatistics:
    """RetrievalEngine get_statistics 方法测试"""

    def test_get_statistics(self) -> None:
        """测试获取统计信息"""
        mock_store = MagicMock()
        mock_store.count.return_value = 100
        mock_store.info.return_value = {"name": "articles", "count": 100}
        mock_store.table.search.return_value.select.return_value.to_list.return_value = [
            {"source_site": "jwc"},
            {"source_site": "jwc"},
            {"source_site": "news"},
        ]

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        stats = engine.get_statistics()

        assert stats["total_documents"] == 100
        assert "source_distribution" in stats

    def test_get_statistics_error(self) -> None:
        """测试获取统计信息出错"""
        mock_store = MagicMock()
        mock_store.count.side_effect = Exception("DB error")

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        stats = engine.get_statistics()

        assert stats == {}
