"""Ingestion Embedder 单元测试"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import numpy as np


class TestEmbedderModelConfig:
    """嵌入器模型配置测试"""

    def test_title_embedding_dimension(self) -> None:
        """测试标题嵌入维度"""
        from app.infrastructure.ingestion.embedder import TITLE_EMBEDDING_DIM

        assert TITLE_EMBEDDING_DIM == 384

    def test_content_embedding_dimension(self) -> None:
        """测试正文嵌入维度"""
        from app.infrastructure.ingestion.embedder import CONTENT_EMBEDDING_DIM

        assert CONTENT_EMBEDDING_DIM == 1024

    def test_bge_query_prefix(self) -> None:
        """测试 BGE 查询前缀"""
        from app.infrastructure.ingestion.embedder import BGE_QUERY_PREFIX

        assert "检索" in BGE_QUERY_PREFIX


class TestEmbedderUtilityFunctions:
    """嵌入器工具函数测试"""

    def test_is_model_cached_locally_true(self) -> None:
        """测试模型已缓存"""
        with patch("app.infrastructure.ingestion.embedder.try_to_load_from_cache") as mock_cache:
            mock_cache.return_value = "/path/to/cached/model"

            from app.infrastructure.ingestion.embedder import _is_model_cached_locally

            result = _is_model_cached_locally("test/model")

            assert result is True

    def test_is_model_cached_locally_false(self) -> None:
        """测试模型未缓存"""
        with patch("app.infrastructure.ingestion.embedder.try_to_load_from_cache") as mock_cache:
            mock_cache.return_value = "_CACHED_NO_EXIST"

            from app.infrastructure.ingestion.embedder import _is_model_cached_locally

            result = _is_model_cached_locally("test/model")

            assert result is False

    def test_is_model_cached_locally_exception(self) -> None:
        """测试缓存检查异常"""
        with patch("app.infrastructure.ingestion.embedder.try_to_load_from_cache") as mock_cache:
            mock_cache.side_effect = Exception("Cache error")

            from app.infrastructure.ingestion.embedder import _is_model_cached_locally

            result = _is_model_cached_locally("test/model")

            assert result is False


class TestEmbedderEmbedTitles:
    """embed_titles 方法测试"""

    def test_embed_titles_returns_correct_dimension(self) -> None:
        """测试标题嵌入返回正确维度"""
        from app.infrastructure.ingestion.embedder import Embedder, TITLE_EMBEDDING_DIM

        embedder = Embedder.__new__(Embedder)
        embedder.title_model = MagicMock()
        embedder.title_model.encode.return_value = np.array([[0.1] * TITLE_EMBEDDING_DIM])
        embedder._initialized = True

        result = embedder.embed_titles(["测试标题"])

        assert len(result) == 1
        assert len(result[0]) == TITLE_EMBEDDING_DIM

    def test_embed_titles_empty_input(self) -> None:
        """测试空输入"""
        from app.infrastructure.ingestion.embedder import Embedder

        embedder = Embedder.__new__(Embedder)
        embedder._initialized = True

        result = embedder.embed_titles([])

        assert result == []

    def test_embed_titles_multiple(self) -> None:
        """测试多个标题"""
        from app.infrastructure.ingestion.embedder import Embedder, TITLE_EMBEDDING_DIM

        embedder = Embedder.__new__(Embedder)
        embedder.title_model = MagicMock()
        embedder.title_model.encode.return_value = np.array([[0.1] * TITLE_EMBEDDING_DIM] * 3)
        embedder._initialized = True

        result = embedder.embed_titles(["标题1", "标题2", "标题3"])

        assert len(result) == 3


class TestEmbedderEmbedContents:
    """embed_contents 方法测试"""

    def test_embed_contents_returns_correct_dimension(self) -> None:
        """测试正文嵌入返回正确维度"""
        from app.infrastructure.ingestion.embedder import Embedder, CONTENT_EMBEDDING_DIM

        embedder = Embedder.__new__(Embedder)
        embedder.content_model = MagicMock()
        embedder.content_model.encode.return_value = np.array([[0.1] * CONTENT_EMBEDDING_DIM])
        embedder._initialized = True

        result = embedder.embed_contents(["测试内容"])

        assert len(result) == 1
        assert len(result[0]) == CONTENT_EMBEDDING_DIM

    def test_embed_contents_empty_input(self) -> None:
        """测试空输入"""
        from app.infrastructure.ingestion.embedder import Embedder

        embedder = Embedder.__new__(Embedder)
        embedder._initialized = True

        result = embedder.embed_contents([])

        assert result == []


class TestEmbedderEmbedBatch:
    """embed_batch 方法测试"""

    def test_embed_batch_combines_results(self) -> None:
        """测试批量嵌入组合结果"""
        from app.infrastructure.ingestion.embedder import Embedder, TITLE_EMBEDDING_DIM, CONTENT_EMBEDDING_DIM

        embedder = Embedder.__new__(Embedder)
        embedder.title_model = MagicMock()
        embedder.title_model.encode.return_value = np.array([[0.1] * TITLE_EMBEDDING_DIM])
        embedder.content_model = MagicMock()
        embedder.content_model.encode.return_value = np.array([[0.2] * CONTENT_EMBEDDING_DIM])
        embedder._initialized = True

        titles = ["标题"]
        contents = ["内容"]
        title_vecs, content_vecs = embedder.embed_batch(titles, contents)

        assert len(title_vecs) == 1
        assert len(content_vecs) == 1
        assert len(title_vecs[0]) == TITLE_EMBEDDING_DIM
        assert len(content_vecs[0]) == CONTENT_EMBEDDING_DIM

