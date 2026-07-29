"""Ingestion Validators 单元测试"""

from app.infrastructure.ingestion.validators import (
    ContentValidator,
    DocumentValidator,
    URLValidator,
    ValidationResult,
)

class TestDocumentValidator:
    """文档验证器测试"""

    def test_validate_valid_document(self) -> None:
        validator = DocumentValidator()
        doc = {
            "news_id": "test_001",
            "title": "测试标题",
            "url": "https://jwc.seu.edu.cn/test",
            "content_text": "这是正文内容，至少有一定长度。",
        }

        result = validator.validate(doc)

        assert result.is_valid is True

    def test_validate_missing_required_field(self) -> None:
        validator = DocumentValidator()
        doc = {
            "news_id": "test_001",

        }

        result = validator.validate(doc)

        assert result.is_valid is False
        assert any("Missing required field" in err for err in result.errors)

    def test_validate_empty_required_field(self) -> None:
        validator = DocumentValidator()
        doc = {
            "news_id": "test_001",
            "title": "   ",
            "url": "https://jwc.seu.edu.cn/test",
        }

        result = validator.validate(doc)

        assert result.is_valid is False
        assert any("empty" in err.lower() for err in result.errors)

    def test_validate_invalid_news_id_format(self) -> None:
        validator = DocumentValidator()
        doc = {
            "news_id": "test@#$%",
            "title": "标题",
            "url": "https://jwc.seu.edu.cn/test",
            "content_text": "正文内容",
        }

        result = validator.validate(doc)

        assert any("special characters" in w for w in result.warnings)

class TestValidationResult:
    """验证结果测试"""

    def test_add_error_sets_invalid(self) -> None:
        result = ValidationResult()
        assert result.is_valid is True

        result.add_error("Test error")

        assert result.is_valid is False
        assert "Test error" in result.errors

    def test_add_warning_preserves_valid(self) -> None:
        result = ValidationResult()
        result.add_warning("Test warning")

        assert result.is_valid is True
        assert "Test warning" in result.warnings

    def test_merge_combines_errors_and_warnings(self) -> None:
        result1 = ValidationResult()
        result1.add_error("Error 1")
        result1.add_warning("Warning 1")

        result2 = ValidationResult()
        result2.add_error("Error 2")
        result2.add_warning("Warning 2")

        result1.merge(result2)

        assert result1.is_valid is False
        assert len(result1.errors) == 2
        assert len(result1.warnings) == 2
