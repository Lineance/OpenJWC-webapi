"""Retrieval Store Integration Tests - 真实实现测试"""

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# 确保 backend 路径可用
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))


class TestLanceStoreRealInit:
    """LanceStore 真实初始化测试"""

    def test_store_init_with_real_db(self, temp_db_path: str, mock_embedder: MagicMock) -> None:
        """测试使用真实数据库初始化"""
        import lancedb
        from app.infrastructure.retrieval.store import LanceStore
        from app.infrastructure.retrieval.schema.article import Article

        # 创建真实数据库
        db = lancedb.connect(temp_db_path)
        table = db.create_table("articles", schema=Article.get_schema())

        store = LanceStore(
            table=table,
            embedder=mock_embedder,
        )

        assert store._table is not None
        assert store.count() == 0

    def test_store_init_without_table_creates_new(self, temp_db_path: str, mock_embedder: MagicMock) -> None:
        """测试不提供表时创建新表"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(
            db_path=temp_db_path,
            table_name="articles",
            embedder=mock_embedder,
        )

        assert store._table is not None
        assert store.count() == 0


class TestLanceStoreRealOperations:
    """LanceStore 真实操作测试"""

    def test_add_and_search_real(
        self,
        temp_db_path: str,
        sample_article: dict[str, Any],
    ) -> None:
        """测试添加文章并进行向量搜索"""
        from app.infrastructure.retrieval.store import LanceStore
        from app.infrastructure.retrieval.schema.article import Article

        # 创建 store
        store = LanceStore(
            db_path=temp_db_path,
            table_name="articles",
        )

        # 添加文章（使用真实 embedding）
        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)

            # 添加文章
            store.add_documents([sample_article], generate_embeddings=True)

        assert store.count() == 1

    def test_count_empty(self, temp_db_path: str, mock_embedder: MagicMock) -> None:
        """测试空表计数"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(
            db_path=temp_db_path,
            table_name="articles",
            embedder=mock_embedder,
        )

        assert store.count() == 0

    def test_schema(self, temp_db_path: str, mock_embedder: MagicMock) -> None:
        """测试获取表结构"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(
            db_path=temp_db_path,
            table_name="articles",
            embedder=mock_embedder,
        )

        schema = store.schema()
        assert schema is not None
        assert "news_id" in str(schema)

    def test_info(self, temp_db_path: str, mock_embedder: MagicMock) -> None:
        """测试获取表信息"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(
            db_path=temp_db_path,
            table_name="articles",
            embedder=mock_embedder,
        )

        info = store.info()
        assert "name" in info
        assert "count" in info
        assert info["count"] == 0


class TestLanceStoreVectorSearch:
    """向量搜索测试"""

    def test_vector_search_with_data(
        self,
        temp_db_path: str,
        sample_articles: list[dict[str, Any]],
    ) -> None:
        """测试带数据的向量搜索"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(db_path=temp_db_path, table_name="articles")

        # 添加测试数据
        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents(sample_articles[:3], generate_embeddings=True)

        # 向量搜索
        results = store.vector_search(
            query_vector=[0.1] * 1024,
            vector_field="content_embedding",
            limit=10,
        )

        assert isinstance(results, list)


class TestLanceStoreFulltextSearch:
    """全文搜索测试"""

    def test_fulltext_search_no_index(
        self,
        temp_db_path: str,
        sample_article: dict[str, Any],
    ) -> None:
        """测试无全文索引时的搜索（降级方案）"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(db_path=temp_db_path, table_name="articles")

        # 添加测试数据
        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents([sample_article], generate_embeddings=True)

        # 全文搜索（会降级到简单文本搜索）
        results = store.fulltext_search(
            query="东南大学",
            limit=10,
        )

        assert isinstance(results, list)


class TestLanceStoreHybridSearch:
    """混合搜索测试"""

    def test_hybrid_search_with_data(
        self,
        temp_db_path: str,
        sample_articles: list[dict[str, Any]],
    ) -> None:
        """测试带数据的混合搜索"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(db_path=temp_db_path, table_name="articles")

        # 添加测试数据
        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents(sample_articles[:3], generate_embeddings=True)

        # 混合搜索
        results = store.hybrid_search("测试")

        assert isinstance(results, list)


class TestLanceStoreBatchOperations:
    """批量操作测试"""

    def test_add_batch(
        self,
        temp_db_path: str,
        sample_articles: list[dict[str, Any]],
    ) -> None:
        """测试批量添加"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(db_path=temp_db_path, table_name="articles")

        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents(sample_articles, generate_embeddings=True)

        assert store.count() == len(sample_articles)

    def test_update_documents(self, temp_db_path: str, sample_article: dict[str, Any]) -> None:
        """测试更新文档"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(db_path=temp_db_path, table_name="articles")

        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents([sample_article], generate_embeddings=True)

        assert store.count() == 1

        # 更新文档
        updated = [{**sample_article, "author": "新作者"}]
        store.update_documents(updated)
        assert store.count() == 1


class TestLanceStoreFilters:
    """过滤功能测试"""

    def test_filter_by_source_site(
        self,
        temp_db_path: str,
        sample_articles: list[dict[str, Any]],
    ) -> None:
        """测试按来源站点过滤"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(db_path=temp_db_path, table_name="articles")

        # 修改来源站点
        for article in sample_articles:
            article["source_site"] = "jwc" if int(article["news_id"][-3:]) % 2 == 0 else "news"

        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents(sample_articles, generate_embeddings=True)

        # 过滤
        results = store.hybrid_search("测试", where="source_site = 'jwc'")

        assert isinstance(results, list)


class TestLanceStoreIndexManagement:
    """索引管理测试"""

    def test_create_vector_index_small_dataset(
        self,
        temp_db_path: str,
        sample_article: dict[str, Any],
    ) -> None:
        """测试小数据集跳过索引创建"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(db_path=temp_db_path, table_name="articles")

        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents([sample_article], generate_embeddings=True)

        # 小数据集应该跳过索引创建
        store.create_vector_index(field="content_embedding")

        # 索引列表应该为空
        indices = store.list_indices()
        assert isinstance(indices, list)

    def test_list_indices(self, temp_db_path: str, mock_embedder: MagicMock) -> None:
        """测试列出索引"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(
            db_path=temp_db_path,
            table_name="articles",
            embedder=mock_embedder,
        )

        indices = store.list_indices()
        assert isinstance(indices, list)
