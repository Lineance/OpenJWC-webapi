"""Tag Initializer 单元测试"""

from typing import Any

from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.ingestion.tag_initializer import TagConfigLoader, TagInitializer

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
