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
