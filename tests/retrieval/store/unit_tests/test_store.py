"""Retrieval Store 单元测试"""

from unittest.mock import MagicMock, patch

import pytest

class TestLanceStoreInit:
    """LanceStore 初始化测试"""

    def test_store_init_with_table(self) -> None:
        """测试使用表对象初始化"""
        mock_table = MagicMock()
        mock_repo = MagicMock()
        mock_embedder = MagicMock()

        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(
            table=mock_table,
            repository=mock_repo,
            embedder=mock_embedder,
        )

        assert store._table is mock_table
        assert store._repository is mock_repo

    def test_store_table_property(self) -> None:
        """测试 table 属性"""
        mock_table = MagicMock()

        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(table=mock_table)
        assert store.table is mock_table

    def test_store_table_not_initialized(self) -> None:
        """测试表未初始化时抛出错误"""
        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore.__new__(LanceStore)
        store._table = None

        with pytest.raises(ValueError, match="not initialized"):
            _ = store.table

    def test_store_count(self) -> None:
        """测试 count 方法"""
        mock_table = MagicMock()
        mock_table.count_rows.return_value = 100

        from app.infrastructure.retrieval.store import LanceStore

        store = LanceStore(table=mock_table)
        assert store.count() == 100
