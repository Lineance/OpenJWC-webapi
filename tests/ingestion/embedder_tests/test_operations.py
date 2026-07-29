"""Ingestion Embedder 单元测试"""

from unittest.mock import MagicMock, patch

import numpy as np

import pytest

pytest.importorskip("sentence_transformers")

class TestEmbedderEmbedTitles:
    """embed_titles 方法测试"""

    def test_embed_titles_returns_correct_dimension(self) -> None:
        """测试标题嵌入返回正确维度"""
        from app.infrastructure.ingestion.embedder.local_embedder import (
            TITLE_EMBEDDING_DIM,
            Embedder,
        )

        embedder = Embedder.__new__(Embedder)
        embedder.title_model = MagicMock()
        embedder.title_model.encode.return_value = np.array(
            [[0.1] * TITLE_EMBEDDING_DIM]
        )
        embedder._initialized = True

        result = embedder.embed_titles(["测试标题"])

        assert len(result) == 1
        assert len(result[0]) == TITLE_EMBEDDING_DIM

    def test_embed_titles_empty_input(self) -> None:
        """测试空输入"""
        from app.infrastructure.ingestion.embedder.local_embedder import Embedder

        embedder = Embedder.__new__(Embedder)
        embedder._initialized = True

        result = embedder.embed_titles([])

        assert result == []

    def test_embed_titles_multiple(self) -> None:
        """测试多个标题"""
        from app.infrastructure.ingestion.embedder.local_embedder import (
            TITLE_EMBEDDING_DIM,
            Embedder,
        )

        embedder = Embedder.__new__(Embedder)
        embedder.title_model = MagicMock()
        embedder.title_model.encode.return_value = np.array(
            [[0.1] * TITLE_EMBEDDING_DIM] * 3
        )
        embedder._initialized = True

        result = embedder.embed_titles(["标题1", "标题2", "标题3"])

        assert len(result) == 3

class TestEmbedderEmbedContents:
    """embed_contents 方法测试"""

    def test_embed_contents_returns_correct_dimension(self) -> None:
        """测试正文嵌入返回正确维度"""
        from app.infrastructure.ingestion.embedder.local_embedder import (
            CONTENT_EMBEDDING_DIM,
            Embedder,
        )

        embedder = Embedder.__new__(Embedder)
        embedder.content_model = MagicMock()
        embedder.content_model.encode.return_value = np.array(
            [[0.1] * CONTENT_EMBEDDING_DIM]
        )
        embedder._initialized = True

        result = embedder.embed_contents(["测试内容"])

        assert len(result) == 1
        assert len(result[0]) == CONTENT_EMBEDDING_DIM

    def test_embed_contents_empty_input(self) -> None:
        """测试空输入"""
        from app.infrastructure.ingestion.embedder.local_embedder import Embedder

        embedder = Embedder.__new__(Embedder)
        embedder._initialized = True

        result = embedder.embed_contents([])

        assert result == []
