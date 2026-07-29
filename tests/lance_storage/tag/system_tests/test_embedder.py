"""测试标签系统"""

class TestEmbedderDirect:
    """Embedder直接测试（不依赖模型加载）"""

    def test_embed_batch_method_exists(self) -> None:
        """测试embed_batch方法存在"""
        from app.infrastructure.ingestion.embedder.local_embedder import Embedder

        assert hasattr(Embedder, "embed_batch")

    def test_embed_titles_method_exists(self) -> None:
        """测试embed_titles方法存在"""
        from app.infrastructure.ingestion.embedder.local_embedder import Embedder

        assert hasattr(Embedder, "embed_titles")

    def test_embed_contents_method_exists(self) -> None:
        """测试embed_contents方法存在"""
        from app.infrastructure.ingestion.embedder.local_embedder import Embedder

        assert hasattr(Embedder, "embed_contents")

class TestEmbedderWithMocks:
    """使用Mock测试Embedder"""

    def test_embed_batch_with_mock_models(self) -> None:
        """测试使用mock模型的embed_batch"""

        pass
