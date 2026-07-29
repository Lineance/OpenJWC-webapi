"""Ingestion Pipeline 单元测试"""

from datetime import datetime

from unittest.mock import MagicMock, patch

from app.infrastructure.ingestion.pipeline import (
    PipelineResult,
    ProcessResult,
)

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
