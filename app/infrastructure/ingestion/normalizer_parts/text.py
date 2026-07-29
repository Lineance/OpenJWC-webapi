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

def normalize_unicode(text: str, form: Literal["NFC", "NFD", "NFKC", "NFKD"] = "NFC") -> str:
    """Unicode 规范化"""
    if not text:
        return ""
    return unicodedata.normalize(form, text)

def normalize_whitespace(text: str) -> str:
    """规范化空白字符"""
    if not text:
        return ""

    text = re.sub(r"\s+", " ", text)
    return text.strip()

def normalize_newlines(text: str) -> str:
    """规范化换行符为 Unix 风格 (\n)"""
    if not text:
        return ""

    text = text.replace("\r\n", "\n")

    text = text.replace("\r", "\n")
    return text

def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """截断文本到指定长度"""
    if not text or len(text) <= max_length:
        return text or ""

    return text[: max_length - len(suffix)] + suffix
