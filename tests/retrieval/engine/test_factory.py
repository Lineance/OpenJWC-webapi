"""Retrieval Engine 单元测试"""

from unittest.mock import MagicMock

import pytest

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
