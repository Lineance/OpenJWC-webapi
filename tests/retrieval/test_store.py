"""Retrieval Store 单元测试"""

from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock

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


class TestCreateStore:
    """create_store 工厂函数测试"""

    def test_create_store_returns_lance_store(self) -> None:
        """测试 create_store 返回 LanceStore"""
        with patch("app.infrastructure.retrieval.store.LanceStore") as mock_store_class:
            mock_store = MagicMock()
            mock_store_class.return_value = mock_store

            from app.infrastructure.retrieval.store import create_store

            store = create_store("/path/to/db", "articles")

            mock_store_class.assert_called_once()


class TestGetStore:
    """get_store 工厂函数测试"""

    def test_get_store_returns_cached_instance(self) -> None:
        """测试 get_store 返回缓存实例"""
        with patch("app.infrastructure.retrieval.store.create_store") as mock_create:
            mock_store = MagicMock()
            mock_create.return_value = mock_store

            from app.infrastructure.retrieval.store import get_store

            store1 = get_store()
            store2 = get_store()

            # 两次调用返回同一实例
            assert store1 is store2

