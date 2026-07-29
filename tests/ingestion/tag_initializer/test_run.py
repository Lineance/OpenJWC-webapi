"""Tag Initializer 单元测试"""

from typing import Any

from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.ingestion.tag_initializer import TagConfigLoader, TagInitializer

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
        mock_repo.count.side_effect = [10, 1]
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
