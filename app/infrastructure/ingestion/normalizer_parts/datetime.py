from __future__ import annotations

import html
import logging
import re
import unicodedata
from datetime import UTC, datetime, timezone
from typing import Literal

from bs4 import BeautifulSoup
from markdown import markdown

logger = logging.getLogger(__name__)

def normalize_datetime(
    date_input: str | datetime | None,
    default_tz: timezone = UTC,
) -> datetime | None:
    """标准化日期时间为 ISO8601 格式 (UTC 时区)"""
    if date_input is None:
        return None

    if isinstance(date_input, datetime):

        if date_input.tzinfo is None:
            return date_input.replace(tzinfo=default_tz)
        return date_input

    if not isinstance(date_input, str):
        return None

    date_str = date_input.strip()
    if not date_str:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
    ]

    date_str = re.sub(r"(\d{4})年(\d{1,2})月(\d{1,2})日", r"\1-\2-\3", date_str)
    date_str = re.sub(r"(\d{1,2})时(\d{1,2})分(\d{1,2})秒?", r"\1:\2:\3", date_str)
    date_str = re.sub(r"(\d{1,2})时(\d{1,2})分", r"\1:\2:00", date_str)

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=default_tz)
            return dt
        except ValueError:
            continue

    logger.warning(f"Failed to parse datetime: {date_input}")
    return None

def format_datetime(dt: datetime | None, fmt: str = "%Y-%m-%dT%H:%M:%S%z") -> str:
    """格式化日期时间为字符串"""
    if dt is None:
        return ""
    return dt.strftime(fmt)
