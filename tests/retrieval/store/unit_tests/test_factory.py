"""Retrieval Store 单元测试"""

from unittest.mock import MagicMock, patch

import pytest

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

            assert store1 is store2
