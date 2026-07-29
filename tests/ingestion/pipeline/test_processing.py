"""Ingestion Pipeline 单元测试"""

from datetime import datetime

from unittest.mock import MagicMock, patch

from app.infrastructure.ingestion.pipeline import (
    PipelineResult,
    ProcessResult,
)

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

            assert result.news_id == "test_001"

    def test_process_one_write_failure(self) -> None:
        """测试写入失败"""
        mock_repo = MagicMock()
        mock_repo.add_one.return_value = False

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
                {"news_id": "test_001"},
            ]

            result = pipeline.process_batch(docs)

            assert result.total == 1
            assert result.invalid == 1
