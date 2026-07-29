"""测试标签系统"""

class TestTagInitializer:
    """标签初始化器测试"""

    def test_tag_initializer_initialization(self) -> None:
        """测试标签初始化器初始化"""
        from app.infrastructure.ingestion.tag_initializer import TagInitializer

        initializer = TagInitializer()
        assert initializer is not None
