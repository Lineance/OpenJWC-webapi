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
