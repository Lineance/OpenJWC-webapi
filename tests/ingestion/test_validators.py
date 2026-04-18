"""Ingestion Validators 单元测试"""



from app.infrastructure.ingestion.validators import (
    ContentValidator,
    DocumentValidator,
    URLValidator,
    ValidationResult,
)


class TestURLValidator:
    """URL 验证器测试"""

    def test_validate_valid_url(self) -> None:
        validator = URLValidator()
        result = validator.validate("https://jwc.seu.edu.cn/jwxx/1001.htm")

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_empty_url(self) -> None:
        validator = URLValidator()
        result = validator.validate("")

        assert result.is_valid is False
        assert "URL is empty" in result.errors

    def test_validate_invalid_scheme(self) -> None:
        validator = URLValidator()
        result = validator.validate("ftp://example.com/file")

        assert result.is_valid is False
        assert any("Invalid scheme" in err for err in result.errors)

    def test_validate_missing_domain(self) -> None:
        validator = URLValidator()
        result = validator.validate("https:///path")

        assert result.is_valid is False
        assert any("must have a domain" in err for err in result.errors)


class TestContentValidator:
    """内容验证器测试"""

    def test_validate_valid_content(self) -> None:
        validator = ContentValidator()
        result = validator.validate("这是一段有效的正文内容。")

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_empty_content(self) -> None:
        validator = ContentValidator()
        result = validator.validate("")

        assert result.is_valid is False
        assert any("empty" in err for err in result.errors)

    def test_validate_content_too_short(self) -> None:
        validator = ContentValidator(min_length=50)
        result = validator.validate("太短")

        assert result.is_valid is False
        assert any("too short" in err for err in result.errors)

    def test_validate_whitespace_only(self) -> None:
        validator = ContentValidator()
        result = validator.validate("   \n\t  ")

        assert result.is_valid is False
        assert any("whitespace" in err for err in result.errors)


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
            # 缺少 title 和 url
        }

        result = validator.validate(doc)

        assert result.is_valid is False
        assert any("Missing required field" in err for err in result.errors)

    def test_validate_empty_required_field(self) -> None:
        validator = DocumentValidator()
        doc = {
            "news_id": "test_001",
            "title": "   ",  # 空白标题
            "url": "https://jwc.seu.edu.cn/test",
        }

        result = validator.validate(doc)

        assert result.is_valid is False
        assert any("empty" in err.lower() for err in result.errors)

    def test_validate_invalid_news_id_format(self) -> None:
        validator = DocumentValidator()
        doc = {
            "news_id": "test@#$%",  # 包含特殊字符
            "title": "标题",
            "url": "https://jwc.seu.edu.cn/test",
            "content_text": "正文内容",
        }

        result = validator.validate(doc)

        # news_id 有特殊字符应该产生警告
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
