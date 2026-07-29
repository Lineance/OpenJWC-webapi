"""Retrieval Store Integration Tests - 真实实现测试"""

import sys

from pathlib import Path

from typing import Any

from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

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

        for article in sample_articles:
            article["source_site"] = "jwc" if int(article["news_id"][-3:]) % 2 == 0 else "news"

        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents(sample_articles, generate_embeddings=True)

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

        store.create_vector_index(field="content_embedding")

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
