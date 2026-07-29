"""Retrieval Store Integration Tests - 真实实现测试"""

import sys

from pathlib import Path

from typing import Any

from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

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

        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents(sample_articles[:3], generate_embeddings=True)

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

        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)
            store.add_documents([sample_article], generate_embeddings=True)

        results = store.fulltext_search(
            query="东南大学",
            limit=10,
        )

        assert isinstance(results, list)
