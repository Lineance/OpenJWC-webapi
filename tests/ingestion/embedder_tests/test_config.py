"""Ingestion Embedder 单元测试"""

from unittest.mock import MagicMock, patch

import numpy as np

import pytest

pytest.importorskip("sentence_transformers")

class TestEmbedderModelConfig:
    """嵌入器模型配置测试"""

    def test_title_embedding_dimension(self) -> None:
        """测试标题嵌入维度"""
        from app.infrastructure.ingestion.embedder.local_embedder import (
            TITLE_EMBEDDING_DIM,
        )

        assert TITLE_EMBEDDING_DIM == 384

    def test_content_embedding_dimension(self) -> None:
        """测试正文嵌入维度"""
        from app.infrastructure.ingestion.embedder.local_embedder import (
            CONTENT_EMBEDDING_DIM,
        )

        assert CONTENT_EMBEDDING_DIM == 1024

    def test_bge_query_prefix(self) -> None:
        """测试 BGE 查询前缀"""
        from app.infrastructure.ingestion.embedder.local_embedder import (
            BGE_QUERY_PREFIX,
        )

        assert "检索" in BGE_QUERY_PREFIX

class TestEmbedderUtilityFunctions:
    """嵌入器工具函数测试"""

    def test_is_model_cached_locally_true(self) -> None:
        """测试模型已缓存"""
        with patch(
            "app.infrastructure.ingestion.embedder.local_embedder.try_to_load_from_cache"
        ) as mock_cache:
            mock_cache.return_value = "/path/to/cached/model"

            from app.infrastructure.ingestion.embedder.local_embedder import (
                _is_model_cached_locally,
            )

            result = _is_model_cached_locally("test/model")

            assert result is True

    def test_is_model_cached_locally_false(self) -> None:
        """测试模型未缓存"""
        with patch(
            "app.infrastructure.ingestion.embedder.local_embedder.try_to_load_from_cache"
        ) as mock_cache:
            mock_cache.return_value = "_CACHED_NO_EXIST"

            from app.infrastructure.ingestion.embedder.local_embedder import (
                _is_model_cached_locally,
            )

            result = _is_model_cached_locally("test/model")

            assert result is False

    def test_is_model_cached_locally_exception(self) -> None:
        """测试缓存检查异常"""
        with patch(
            "app.infrastructure.ingestion.embedder.local_embedder.try_to_load_from_cache"
        ) as mock_cache:
            mock_cache.side_effect = Exception("Cache error")

            from app.infrastructure.ingestion.embedder.local_embedder import (
                _is_model_cached_locally,
            )

            result = _is_model_cached_locally("test/model")

            assert result is False
