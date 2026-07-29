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

class TestMarkdownToText:
    """Markdown 转文本测试"""

    def test_convert_heading(self) -> None:
        result = markdown_to_text("# 一级标题")
        assert "一级标题" in result

    def test_convert_bold_text(self) -> None:
        result = markdown_to_text("**加粗文字** 和 普通文字")
        assert "加粗文字" in result
        assert "普通文字" in result

    def test_convert_link(self) -> None:
        result = markdown_to_text("[链接文字](https://example.com)")
        assert "链接文字" in result

    def test_empty_input(self) -> None:
        result = markdown_to_text("")
        assert result == ""

class TestStripHtml:
    """HTML 标签移除测试"""

    def test_strip_simple_tags(self) -> None:
        result = strip_html("<p>段落文字</p>")
        assert "段落文字" in result

    def test_strip_nested_tags(self) -> None:
        result = strip_html("<div><p>嵌套<span>文字</span></p></div>")

        assert "嵌套" in result and "文字" in result

    def test_empty_input(self) -> None:
        result = strip_html("")
        assert result == ""
