"""Tag Initializer 单元测试"""

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.ingestion.tag_initializer import TagConfigLoader, TagInitializer


class TestTagConfigLoader:
    """TagConfigLoader 测试"""

    def test_load_config_success(self, tmp_path: Any) -> None:
        """测试成功加载配置"""
        config_file = tmp_path / "tags.yaml"
        config_file.write_text("""
tags:
  - id: tag1
    name: Test Tag
    description: Test description
    category: test
""")

        loader = TagConfigLoader()
        config = loader.load_config(str(config_file))

        assert "tags" in config
        assert len(config["tags"]) == 1

    def test_load_config_file_not_found(self) -> None:
        """测试配置文件不存在"""
        loader = TagConfigLoader()

        with pytest.raises(Exception):
            loader.load_config("/nonexistent/path.yaml")

    def test_parse_tags_mixed(self) -> None:
        """测试解析混合标签配置"""
        loader = TagConfigLoader()
        config = {
            "tags": [
                {"id": "tag1", "name": "标签1", "description": "测试"}
            ],
            "manual_tags": [
                {"id": "tag2", "name": "标签2", "description": "手动标签"}
            ]
        }

        tags = loader.parse_tags(config)

        assert len(tags) == 2
        assert tags[0]["id"] == "tag1"
        assert tags[1]["id"] == "tag2"

    def test_parse_tags_empty(self) -> None:
        """测试解析空配置"""
        loader = TagConfigLoader()
        tags = loader.parse_tags({})

        assert tags == []

    def test_parse_tags_invalid_format(self) -> None:
        """测试解析无效格式"""
        loader = TagConfigLoader()
        tags = loader.parse_tags({"tags": "not a list"})

        assert tags == []

    def test_parse_tags_only_auto(self) -> None:
        """测试只解析自动标签"""
        loader = TagConfigLoader()
        config = {
            "tags": [
                {"id": "tag1", "name": "标签1"},
                {"id": "tag2", "name": "标签2"}
            ]
        }

        tags = loader.parse_tags(config)
        assert len(tags) == 2

    def test_parse_tags_only_manual(self) -> None:
        """测试只解析手动标签"""
        loader = TagConfigLoader()
        config = {
            "manual_tags": [
                {"id": "tag1", "name": "标签1"}
            ]
        }

        tags = loader.parse_tags(config)
        assert len(tags) == 1

    def test_parse_tags_filters_invalid(self) -> None:
        """测试过滤无效标签"""
        loader = TagConfigLoader()
        config = {
            "tags": [
                {"id": "tag1", "name": "标签1"},
                "not a dict",
                None,
                {"id": "tag2", "name": "标签2"}
            ]
        }

        tags = loader.parse_tags(config)
        assert len(tags) == 2


class TestTagInitializerInit:
    """TagInitializer 初始化测试"""

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_initializer_init(self, mock_get_repo: MagicMock, mock_get_embedder: MagicMock) -> None:
        """测试初始化器初始化"""
        mock_repo = MagicMock()
        mock_embedder = MagicMock()
        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer(
            config_path="test.yaml",
            clear_existing=True,
            create_indices=True
        )

        assert initializer.config_path == "test.yaml"
        assert initializer.clear_existing is True
        assert initializer.create_indices is True


class TestTagInitializerRun:
    """TagInitializer run 方法测试"""

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    @patch.object(TagConfigLoader, "load_config")
    @patch.object(TagConfigLoader, "parse_tags")
    def test_run_no_tags(
        self,
        mock_parse: MagicMock,
        mock_load: MagicMock,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试没有标签时运行失败"""
        mock_load.return_value = {}
        mock_parse.return_value = []

        mock_repo = MagicMock()
        mock_embedder = MagicMock()
        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        result = initializer.run()

        assert result is False

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    @patch.object(TagConfigLoader, "load_config")
    @patch.object(TagConfigLoader, "parse_tags")
    def test_run_success(
        self,
        mock_parse: MagicMock,
        mock_load: MagicMock,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试成功运行"""
        mock_load.return_value = {}
        mock_parse.return_value = [
            {"id": "tag1", "name": "标签1", "description": "测试", "category": "test"}
        ]

        mock_repo = MagicMock()
        mock_repo.count.return_value = 1
        mock_repo.add_batch.return_value = 1
        mock_repo.create_indices.return_value = True

        mock_embedder = MagicMock()
        mock_embedder.embed_contents.return_value = [[0.1] * 1024]

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        result = initializer.run()

        assert result is True
        mock_repo.add_batch.assert_called_once()

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    @patch.object(TagConfigLoader, "load_config")
    @patch.object(TagConfigLoader, "parse_tags")
    def test_run_with_clear_existing(
        self,
        mock_parse: MagicMock,
        mock_load: MagicMock,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试带清空选项的运行"""
        mock_load.return_value = {}
        mock_parse.return_value = [
            {"id": "tag1", "name": "标签1", "description": "测试", "category": "test"}
        ]

        mock_repo = MagicMock()
        mock_repo.count.side_effect = [10, 1]  # 10个现有标签，然后1个新标签
        mock_repo.clear_all.return_value = True
        mock_repo.add_batch.return_value = 1
        mock_repo.create_indices.return_value = True

        mock_embedder = MagicMock()
        mock_embedder.embed_contents.return_value = [[0.1] * 1024]

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer(clear_existing=True)
        result = initializer.run()

        assert result is True
        mock_repo.clear_all.assert_called_once()


class TestTagInitializerHelpers:
    """TagInitializer 辅助方法测试"""

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_clear_existing_tags(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试清空现有标签"""
        mock_repo = MagicMock()
        mock_repo.count.return_value = 5
        mock_repo.clear_all.return_value = True

        mock_embedder = MagicMock()

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        result = initializer._clear_existing_tags()

        assert result is True
        mock_repo.clear_all.assert_called_once()

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_clear_existing_no_tags(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试清空时无现有标签"""
        mock_repo = MagicMock()
        mock_repo.count.return_value = 0

        mock_embedder = MagicMock()

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        result = initializer._clear_existing_tags()

        assert result is True
        mock_repo.clear_all.assert_not_called()

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_generate_tag_embeddings(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试生成标签嵌入"""
        mock_repo = MagicMock()
        mock_embedder = MagicMock()
        mock_embedder.embed_contents.return_value = [[0.1] * 1024]

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        tag_defs = [
            {"id": "tag1", "name": "标签1", "description": "测试", "category": "test"}
        ]

        records = initializer._generate_tag_embeddings(tag_defs)

        assert len(records) == 1
        assert records[0].tag_id == "tag1"
        assert records[0].name == "标签1"
        mock_embedder.embed_contents.assert_called_once()

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_generate_tag_embeddings_partial_failure(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试生成嵌入部分失败"""
        mock_repo = MagicMock()
        mock_embedder = MagicMock()
        # First call succeeds, second call fails
        mock_embedder.embed_contents.side_effect = [
            [[0.1] * 1024],
            Exception("embed error")
        ]

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        tag_defs = [
            {"id": "tag1", "name": "标签1", "description": "测试", "category": "test"},
            {"id": "tag2", "name": "标签2", "description": "测试2", "category": "test"}
        ]

        records = initializer._generate_tag_embeddings(tag_defs)

        # One succeeded, one failed
        assert len(records) == 1
        assert records[0].tag_id == "tag1"

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_save_tags_empty(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试保存空标签列表"""
        mock_repo = MagicMock()
        mock_embedder = MagicMock()

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        count = initializer._save_tags([])

        assert count == 0
        mock_repo.add_batch.assert_not_called()

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_save_tags_success(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试保存标签成功"""
        mock_repo = MagicMock()
        mock_repo.add_batch.return_value = 2

        mock_embedder = MagicMock()

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()

        mock_record = MagicMock()
        count = initializer._save_tags([mock_record, mock_record])

        assert count == 2

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_create_indices(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试创建索引"""
        mock_repo = MagicMock()
        mock_repo.create_indices.return_value = True

        mock_embedder = MagicMock()

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        result = initializer._create_indices()

        assert result is True
        mock_repo.create_indices.assert_called_once()

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_verify_initialization_pass(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试验证通过"""
        mock_repo = MagicMock()
        mock_repo.count.return_value = 5

        mock_embedder = MagicMock()

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        result = initializer._verify_initialization(5)

        assert result is True

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_verify_initialization_fail(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试验证失败"""
        mock_repo = MagicMock()
        mock_repo.count.return_value = 3

        mock_embedder = MagicMock()

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        result = initializer._verify_initialization(5)

        assert result is False

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_get_statistics(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试获取统计信息"""
        mock_repo = MagicMock()
        mock_repo.count.return_value = 10
        mock_repo.count_by_category.return_value = {"test": 5, "general": 5}

        mock_embedder = MagicMock()

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        stats = initializer.get_statistics()

        assert stats["total_tags"] == 10
        assert stats["categories"]["test"] == 5

    @patch("app.infrastructure.ingestion.tag_initializer.get_embedder")
    @patch("app.infrastructure.ingestion.tag_initializer.get_tag_repository")
    def test_get_statistics_error(
        self,
        mock_get_repo: MagicMock,
        mock_get_embedder: MagicMock
    ) -> None:
        """测试获取统计信息出错"""
        mock_repo = MagicMock()
        mock_repo.count.side_effect = Exception("db error")

        mock_embedder = MagicMock()

        mock_get_repo.return_value = mock_repo
        mock_get_embedder.return_value = mock_embedder

        initializer = TagInitializer()
        stats = initializer.get_statistics()

        assert stats == {}

