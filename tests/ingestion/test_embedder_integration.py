"""Embedder Real Integration Tests - 使用真实模型"""

import pytest


@pytest.fixture(autouse=True)
def reset_embedder_singleton():
    """在每个测试前后重置 Embedder 单例"""
    from app.infrastructure.ingestion.embedder import Embedder

    # 重置单例
    Embedder.reset()
    yield
    # 测试后重置
    Embedder.reset()


class TestEmbedderRealModel:
    """使用真实嵌入模型的集成测试"""

    def test_embed_titles_real_model(self) -> None:
        """测试使用真实模型进行标题嵌入"""
        from app.infrastructure.ingestion.embedder import Embedder

        embedder = Embedder()
        result = embedder.embed_titles(["东南大学", "计算机学院"])

        assert len(result) == 2
        assert len(result[0]) == 384  # Title embedding dim

    def test_embed_contents_real_model(self) -> None:
        """测试使用真实模型进行内容嵌入"""
        from app.infrastructure.ingestion.embedder import Embedder

        embedder = Embedder()
        result = embedder.embed_contents(["这是测试内容", "另一条内容"])

        assert len(result) == 2
        assert len(result[0]) == 1024  # Content embedding dim

    def test_embed_query_real_model(self) -> None:
        """测试使用真实模型进行查询嵌入"""
        from app.infrastructure.ingestion.embedder import Embedder

        embedder = Embedder()
        result = embedder.embed_query("东南大学计算机学院")

        assert len(result) == 1024  # Content embedding dim

    def test_embed_batch_real_model(self) -> None:
        """测试批量嵌入"""
        from app.infrastructure.ingestion.embedder import Embedder

        embedder = Embedder()
        titles = [f"测试标题{i}" for i in range(5)]
        contents = [f"测试内容{i}" for i in range(5)]

        title_vectors = embedder.embed_titles(titles)
        content_vectors = embedder.embed_contents(contents)

        assert len(title_vectors) == 5
        assert len(content_vectors) == 5
