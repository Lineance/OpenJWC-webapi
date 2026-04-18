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
        # BeautifulSoup 在标签间添加空格
        assert "嵌套" in result and "文字" in result

    def test_empty_input(self) -> None:
        result = strip_html("")
        assert result == ""


class TestNormalizeDatetime:
    """日期时间标准化测试"""

    def test_parse_iso_format(self) -> None:
        result = normalize_datetime("2024-05-20T10:30:00")

        assert result is not None
        assert result.year == 2024
        assert result.month == 5
        assert result.day == 20

    def test_parse_chinese_format(self) -> None:
        result = normalize_datetime("2024年5月20日")

        assert result is not None
        assert result.year == 2024
        assert result.month == 5
        assert result.day == 20

    def test_parse_slash_format(self) -> None:
        result = normalize_datetime("2024/05/20")

        assert result is not None
        assert result.year == 2024
        assert result.month == 5
        assert result.day == 20

    def test_parse_datetime_object(self) -> None:
        dt = datetime(2024, 5, 20, 10, 30, 0, tzinfo=timezone.utc)
        result = normalize_datetime(dt)

        assert result == dt

    def test_parse_none_returns_none(self) -> None:
        result = normalize_datetime(None)
        assert result is None

    def test_parse_invalid_string_returns_none(self) -> None:
        result = normalize_datetime("not a date")
        assert result is None


class TestTruncateText:
    """文本截断测试"""

    def test_truncate_short_text_unchanged(self) -> None:
        text = "短文本"
        result = truncate_text(text, 100)
        assert result == text

    def test_truncate_long_text_with_suffix(self) -> None:
        text = "这是一段很长的文本内容"
        result = truncate_text(text, 10)
        assert len(result) <= 13  # 10 + len("...")
        assert result.endswith("...")

    def test_truncate_exact_length_unchanged(self) -> None:
        text = "正好十个字"
        result = truncate_text(text, 10)
        assert result == text


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

        assert len(result) <= 23  # 20 + "..."


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

        assert len(result) <= 53  # 50 + "..."
