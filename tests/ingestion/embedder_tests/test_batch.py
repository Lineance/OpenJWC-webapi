"""Ingestion Embedder 单元测试"""

from unittest.mock import MagicMock, patch

import numpy as np

import pytest

pytest.importorskip("sentence_transformers")

class TestEmbedderEmbedBatch:
    """embed_batch 方法测试"""

    def test_embed_batch_combines_results(self) -> None:
        """测试批量嵌入组合结果"""
        from app.infrastructure.ingestion.embedder.local_embedder import (
            CONTENT_EMBEDDING_DIM,
            TITLE_EMBEDDING_DIM,
            Embedder,
        )

        embedder = Embedder.__new__(Embedder)
        embedder.title_model = MagicMock()
        embedder.title_model.encode.return_value = np.array(
            [[0.1] * TITLE_EMBEDDING_DIM]
        )
        embedder.content_model = MagicMock()
        embedder.content_model.encode.return_value = np.array(
            [[0.2] * CONTENT_EMBEDDING_DIM]
        )
        embedder._initialized = True

        titles = ["标题"]
        contents = ["内容"]
        title_vecs, content_vecs = embedder.embed_batch(titles, contents)

        assert len(title_vecs) == 1
        assert len(content_vecs) == 1
        assert len(title_vecs[0]) == TITLE_EMBEDDING_DIM
        assert len(content_vecs[0]) == CONTENT_EMBEDDING_DIM
