"""Retrieval Store Integration Tests - 真实实现测试"""

import sys

from pathlib import Path

from typing import Any

from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

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

        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents(sample_articles[:3], generate_embeddings=True)

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

        updated = [{**sample_article, "author": "新作者"}]
        store.update_documents(updated)
        assert store.count() == 1
