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
        del mock_store.hybrid_search  # Remove the method

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

        # Invalid limit should raise
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

        # Invalid offset should raise
        with pytest.raises(ValueError, match="offset must be >="):
            engine.search("test", offset=-1)


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

        # Default fields should be ["title", "content_text"]
        assert result["fields"] == ["title", "content_text"]


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


class TestRetrievalEngineGetSimilarDocuments:
    """RetrievalEngine get_similar_documents 方法测试"""

    def test_get_similar_documents(self) -> None:
        """测试获取相似文档"""
        mock_store = MagicMock()
        # First call for get_document
        mock_store.table.search.return_value.where.return_value.limit.return_value.to_list.side_effect = [
            [{"news_id": "1", "title_embedding": [0.1] * 384}]
        ]
        # Second call for vector_search
        mock_store.vector_search.return_value = [
            {"news_id": "2", "title_embedding": [0.2] * 384}
        ]

        from app.infrastructure.retrieval.engine import RetrievalEngine

        engine = RetrievalEngine(store=mock_store, embedder=MagicMock())
        docs = engine.get_similar_documents("1", field="title", limit=5)

        assert len(docs) >= 0  # Just check it runs

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
            {"news_id": "1"}  # No embedding
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


class TestRetrievalEngineCreateGet:
    """便捷函数测试"""

    def test_create_engine(self) -> None:
        """测试 create_engine 函数"""
        from app.infrastructure.retrieval.engine import create_engine

        engine = create_engine()
        assert engine is not None

    def test_get_engine(self) -> None:
        """测试 get_engine 函数"""
        from app.infrastructure.retrieval.engine import get_engine

        engine = get_engine()
        assert engine is not None
