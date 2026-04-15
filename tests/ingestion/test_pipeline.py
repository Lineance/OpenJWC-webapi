"""Ingestion Pipeline 单元测试"""

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.ingestion.pipeline import (
    PipelineResult,
    ProcessResult,
)


class TestProcessResult:
    """ProcessResult 数据类测试"""

    def test_process_result_defaults(self) -> None:
        """测试默认值"""
        result = ProcessResult()

        assert result.news_id is None
        assert result.url is None
        assert result.status == "unknown"
        assert result.message == ""

    def test_process_result_with_values(self) -> None:
        """测试带值的构造"""
        result = ProcessResult(
            news_id="test_001",
            url="https://example.com/test",
            status="success",
            message="",
        )

        assert result.news_id == "test_001"
        assert result.url == "https://example.com/test"
        assert result.status == "success"


class TestPipelineResult:
    """PipelineResult 数据类测试"""

    def test_pipeline_result_defaults(self) -> None:
        """测试默认值"""
        result = PipelineResult()

        assert result.total == 0
        assert result.success == 0
        assert result.invalid == 0
        assert result.duplicate == 0
        assert result.error == 0
        assert result.results == []
        assert result.elapsed_seconds == 0.0

    def test_add_result_success(self) -> None:
        """测试添加成功结果"""
        result = PipelineResult()
        result.add_result(ProcessResult(news_id="t1", status="success"))

        assert result.total == 1
        assert result.success == 1
        assert result.invalid == 0
        assert result.duplicate == 0
        assert result.error == 0

    def test_add_result_invalid(self) -> None:
        """测试添加无效结果"""
        result = PipelineResult()
        result.add_result(
            ProcessResult(news_id="t1", status="invalid", message="Missing title")
        )

        assert result.total == 1
        assert result.success == 0
        assert result.invalid == 1

    def test_add_result_duplicate(self) -> None:
        """测试添加重复结果"""
        result = PipelineResult()
        result.add_result(ProcessResult(news_id="t1", status="duplicate"))

        assert result.total == 1
        assert result.duplicate == 1

    def test_add_result_error(self) -> None:
        """测试添加错误结果"""
        result = PipelineResult()
        result.add_result(
            ProcessResult(news_id="t1", status="error", message="DB error")
        )

        assert result.total == 1
        assert result.error == 1

    def test_summary(self) -> None:
        """测试摘要生成"""
        result = PipelineResult()
        result.total = 10
        result.success = 5
        result.invalid = 2
        result.duplicate = 2
        result.error = 1
        result.elapsed_seconds = 1.5

        summary = result.summary()

        assert "total=10" in summary
        assert "success=5" in summary
        assert "invalid=2" in summary
        assert "duplicate=2" in summary
        assert "error=1" in summary
        assert "1.50s" in summary


class TestPipelineNormalization:
    """管道数据标准化测试"""

    def test_normalize_minimal_document(self) -> None:
        """测试最小文档标准化"""
        with (
            patch("app.infrastructure.ingestion.pipeline.get_embedder"),
            patch("app.infrastructure.ingestion.pipeline.get_article_repository"),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            from app.infrastructure.ingestion.pipeline import IngestionPipeline

            pipeline = IngestionPipeline(
                skip_validation=True,
                skip_dedup=True,
                skip_embedding=True,
                skip_tag_matching=True,
            )

            doc = {
                "news_id": "test_001",
                "title": "测试",
                "url": "https://example.com/test",
            }

            normalized = pipeline._normalize(doc)

            assert normalized["news_id"] == "test_001"
            assert normalized["title"] == "测试"
            assert normalized["url"] == "https://example.com/test"

    def test_normalize_extracts_title_from_content(self) -> None:
        """测试从内容中提取标题"""
        with (
            patch("app.infrastructure.ingestion.pipeline.get_embedder"),
            patch("app.infrastructure.ingestion.pipeline.get_article_repository"),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            from app.infrastructure.ingestion.pipeline import IngestionPipeline

            pipeline = IngestionPipeline(
                skip_validation=True,
                skip_dedup=True,
                skip_embedding=True,
                skip_tag_matching=True,
            )

            doc = {
                "news_id": "test_001",
                "url": "https://example.com/test",
                "content_markdown": "# 提取的标题\n\n正文内容",
            }

            normalized = pipeline._normalize(doc)

            assert "提取的标题" in normalized["title"]

    def test_normalize_datetime_conversion(self) -> None:
        """测试日期时间转换"""
        with (
            patch("app.infrastructure.ingestion.pipeline.get_embedder"),
            patch("app.infrastructure.ingestion.pipeline.get_article_repository"),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            from app.infrastructure.ingestion.pipeline import IngestionPipeline

            pipeline = IngestionPipeline(
                skip_validation=True,
                skip_dedup=True,
                skip_embedding=True,
                skip_tag_matching=True,
            )

            doc = {
                "news_id": "test_001",
                "title": "测试",
                "url": "https://example.com/test",
                "publish_date": "2024-05-20T10:00:00Z",
            }

            normalized = pipeline._normalize(doc)

            assert isinstance(normalized["publish_date"], datetime)

    def test_normalize_preserves_attachments(self) -> None:
        """测试附件字段在标准化后不会丢失"""
        with (
            patch("app.infrastructure.ingestion.pipeline.get_embedder"),
            patch("app.infrastructure.ingestion.pipeline.get_article_repository"),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            from app.infrastructure.ingestion.pipeline import IngestionPipeline
            from app.infrastructure.storage.lancedb.schema import ArticleFields

            pipeline = IngestionPipeline(
                skip_validation=True,
                skip_dedup=True,
                skip_embedding=True,
                skip_tag_matching=True,
            )

            doc = {
                "news_id": "test_001",
                "title": "测试",
                "url": "https://example.com/test",
                "content_markdown": "正文",
                ArticleFields.ATTACHMENTS: [
                    "https://example.com/a.pdf",
                    "https://example.com/b.pdf",
                ],
            }

            normalized = pipeline._normalize(doc)

            assert normalized[ArticleFields.ATTACHMENTS] == [
                "https://example.com/a.pdf",
                "https://example.com/b.pdf",
            ]


class TestPipelineValidation:
    """管道数据验证测试"""

    def test_validate_missing_required_field(self) -> None:
        """测试缺少必填字段"""
        with (
            patch("app.infrastructure.ingestion.pipeline.get_embedder"),
            patch("app.infrastructure.ingestion.pipeline.get_article_repository"),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            from app.infrastructure.ingestion.pipeline import IngestionPipeline

            pipeline = IngestionPipeline(
                skip_dedup=True,
                skip_embedding=True,
                skip_tag_matching=True,
            )

            doc = {
                "news_id": "test_001",
                # 缺少 title 和 url
            }

            result = pipeline.process_one(doc)

            assert result.status == "invalid"
            assert (
                "Missing required field" in result.message
                or "title" in result.message.lower()
            )

    def test_validate_invalid_url(self) -> None:
        """测试无效 URL"""
        with (
            patch("app.infrastructure.ingestion.pipeline.get_embedder"),
            patch("app.infrastructure.ingestion.pipeline.get_article_repository"),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            from app.infrastructure.ingestion.pipeline import IngestionPipeline

            pipeline = IngestionPipeline(
                skip_dedup=True,
                skip_embedding=True,
                skip_tag_matching=True,
            )

            doc = {
                "news_id": "test_001",
                "title": "测试",
                "url": "not-a-valid-url",
            }

            result = pipeline.process_one(doc)

            assert result.status == "invalid"


class TestPipelineProcessOne:
    """process_one 方法测试"""

    def test_process_one_skip_validation(self) -> None:
        """测试跳过验证"""
        mock_repo = MagicMock()
        mock_repo.add_one.return_value = True

        with (
            patch(
                "app.infrastructure.ingestion.pipeline.get_embedder"
            ) as mock_get_embedder,
            patch(
                "app.infrastructure.ingestion.pipeline.get_article_repository",
                return_value=mock_repo,
            ),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            mock_embedder = MagicMock()
            mock_embedder.embed_batch.return_value = ([0.1] * 384, [0.1] * 1024)
            mock_get_embedder.return_value = mock_embedder

            from app.infrastructure.ingestion.pipeline import IngestionPipeline

            pipeline = IngestionPipeline(
                skip_validation=True,
                skip_dedup=True,
                skip_tag_matching=True,
            )

            doc = {
                "news_id": "test_001",
                "title": "测试",
                "url": "https://example.com/test",
            }

            result = pipeline.process_one(doc)

            # 跳过验证后，直接处理
            assert result.news_id == "test_001"

    def test_process_one_write_failure(self) -> None:
        """测试写入失败"""
        mock_repo = MagicMock()
        mock_repo.add_one.return_value = False  # 写入失败

        with (
            patch(
                "app.infrastructure.ingestion.pipeline.get_embedder"
            ) as mock_get_embedder,
            patch(
                "app.infrastructure.ingestion.pipeline.get_article_repository",
                return_value=mock_repo,
            ),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            mock_embedder = MagicMock()
            mock_embedder.embed_batch.return_value = ([0.1] * 384, [0.1] * 1024)
            mock_get_embedder.return_value = mock_embedder

            from app.infrastructure.ingestion.pipeline import IngestionPipeline

            pipeline = IngestionPipeline(
                skip_validation=True,
                skip_dedup=True,
                skip_tag_matching=True,
            )

            doc = {
                "news_id": "test_001",
                "title": "测试",
                "url": "https://example.com/test",
            }

            result = pipeline.process_one(doc)

            assert result.status == "error"


class TestPipelineProcessBatch:
    """process_batch 方法测试"""

    def test_process_batch_empty_list(self) -> None:
        """测试空列表处理"""
        with (
            patch("app.infrastructure.ingestion.pipeline.get_embedder"),
            patch("app.infrastructure.ingestion.pipeline.get_article_repository"),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            from app.infrastructure.ingestion.pipeline import IngestionPipeline

            pipeline = IngestionPipeline(
                skip_validation=True,
                skip_dedup=True,
                skip_embedding=True,
                skip_tag_matching=True,
            )

            result = pipeline.process_batch([])

            assert result.total == 0
            assert result.success == 0

    def test_process_batch_validation_failure(self) -> None:
        """测试批量验证失败"""
        mock_repo = MagicMock()

        with (
            patch("app.infrastructure.ingestion.pipeline.get_embedder"),
            patch(
                "app.infrastructure.ingestion.pipeline.get_article_repository",
                return_value=mock_repo,
            ),
            patch("app.infrastructure.ingestion.pipeline.get_tag_matcher"),
        ):
            from app.infrastructure.ingestion.pipeline import IngestionPipeline

            pipeline = IngestionPipeline(
                skip_dedup=True,
                skip_embedding=True,
                skip_tag_matching=True,
            )

            docs = [
                {"news_id": "test_001"},  # 缺少必填字段
            ]

            result = pipeline.process_batch(docs)

            assert result.total == 1
            assert result.invalid == 1
