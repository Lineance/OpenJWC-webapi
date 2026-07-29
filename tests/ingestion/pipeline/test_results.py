"""Ingestion Pipeline 单元测试"""

from datetime import datetime

from unittest.mock import MagicMock, patch

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
