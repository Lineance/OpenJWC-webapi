"""SQLGuard 安全校验测试。"""

import pytest
from app.infrastructure.storage.lancedb.guard import SQLGuard


def test_validate_where_allows_double_dash_inside_string_literal() -> None:
    guard = SQLGuard()
    clause = "title = 'This is a -- great news'"

    assert guard.validate_where(clause) is True


def test_validate_where_allows_iso_datetime_string() -> None:
    guard = SQLGuard()
    clause = "publish_date = '2024-01-01T12:00:00+08:00'"

    assert guard.validate_where(clause) is True


def test_validate_where_blocks_statement_separator_outside_literal() -> None:
    guard = SQLGuard()
    clause = "news_id = 'n1'; DROP TABLE articles"

    with pytest.raises(ValueError, match="SQL injection pattern detected"):
        guard.validate_where(clause)


def test_validate_where_blocks_copy_statement_keyword() -> None:
    guard = SQLGuard()
    clause = "title = 'safe' AND COPY = TRUE"

    with pytest.raises(ValueError, match="SQL injection pattern detected"):
        guard.validate_where(clause)


def test_validate_where_blocks_execute_statement_keyword() -> None:
    guard = SQLGuard()
    clause = "title = 'safe' OR EXECUTE = TRUE"

    with pytest.raises(ValueError, match="SQL injection pattern detected"):
        guard.validate_where(clause)
