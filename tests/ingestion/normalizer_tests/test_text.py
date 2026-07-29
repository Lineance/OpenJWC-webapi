"""Ingestion Normalizers 单元测试"""

from datetime import datetime, timezone

from app.infrastructure.ingestion.normalizers import (
    extract_first_sentence,
    normalize_content,
    normalize_datetime,
    normalize_whitespace,
    markdown_to_text,
    strip_html,
    truncate_text,
)

class TestNormalizeWhitespace:
    """空白字符标准化测试"""

    def test_normalize_multiple_spaces(self) -> None:
        result = normalize_whitespace("多个    空格")
        assert result == "多个 空格"

    def test_normalize_tabs_and_newlines(self) -> None:
        result = normalize_whitespace("文字\t\n\t文字")
        assert result == "文字 文字"

    def test_normalize_strips_whitespace(self) -> None:
        result = normalize_whitespace("  前后空格  ")
        assert result == "前后空格"

class TestTruncateText:
    """文本截断测试"""

    def test_truncate_short_text_unchanged(self) -> None:
        text = "短文本"
        result = truncate_text(text, 100)
        assert result == text

    def test_truncate_long_text_with_suffix(self) -> None:
        text = "这是一段很长的文本内容"
        result = truncate_text(text, 10)
        assert len(result) <= 13
        assert result.endswith("...")

    def test_truncate_exact_length_unchanged(self) -> None:
        text = "正好十个字"
        result = truncate_text(text, 10)
        assert result == text
