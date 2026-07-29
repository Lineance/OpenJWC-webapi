"""Retrieval Store Integration Tests - 真实实现测试"""

import sys

from pathlib import Path

from typing import Any

from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

class TestLanceStoreRealInit:
    """LanceStore 真实初始化测试"""

    def test_store_init_with_real_db(self, temp_db_path: str, mock_embedder: MagicMock) -> None:
        """测试使用真实数据库初始化"""
        import lancedb
        from app.infrastructure.retrieval.store import LanceStore
        from app.infrastructure.retrieval.schema.article import Article

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

        store = LanceStore(
            db_path=temp_db_path,
            table_name="articles",
        )

        with patch.object(store._embedder, "embed_query") as mock_embed:
            mock_embed.return_value = ([0.1] * 384, [0.1] * 1024)

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
