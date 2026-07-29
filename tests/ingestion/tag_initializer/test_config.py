"""Tag Initializer 单元测试"""

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
