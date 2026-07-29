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

class TestExtractFirstSentence:
    """第一句提取测试"""

    def test_extract_from_markdown_heading(self) -> None:
        text = "# 文章标题\n\n这是正文的第一句话。"
        result = extract_first_sentence(text, is_markdown=True)

        assert "文章标题" in result

    def test_extract_chinese_sentence(self) -> None:
        text = "这是第一句话。这是第二句话。"
        result = extract_first_sentence(text, is_markdown=False)

        assert "第一句话" in result

    def test_extract_english_sentence(self) -> None:
        text = "First sentence. Second sentence."
        result = extract_first_sentence(text, is_markdown=False)

        assert "First sentence" in result

    def test_truncate_long_title(self) -> None:
        text = "# " + "很长" * 100 + "\n\n正文"
        result = extract_first_sentence(text, is_markdown=True, max_title_length=20)

        assert len(result) <= 23

class TestNormalizeContent:
    """综合内容标准化测试"""

    def test_normalize_markdown_content(self) -> None:
        md = "# 标题\n\n**加粗**和[链接](url)"
        result = normalize_content(md, is_markdown=True)

        assert "标题" in result
        assert "加粗" in result

    def test_normalize_html_content(self) -> None:
        html = "<p>段落<b>加粗</b></p>"
        result = normalize_content(html, is_markdown=False)

        assert "段落" in result
        assert "加粗" in result

    def test_normalize_empty_content(self) -> None:
        result = normalize_content("")
        assert result == ""

    def test_normalize_with_max_length(self) -> None:
        text = "这是很长的内容。" * 100
        result = normalize_content(text, is_markdown=False, max_length=50)

        assert len(result) <= 53
