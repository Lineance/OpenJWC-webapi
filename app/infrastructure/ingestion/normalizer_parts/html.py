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

from app.infrastructure.ingestion.normalizer_parts.text import normalize_whitespace

def strip_html(html_content: str) -> str:
    """移除 HTML 标签，提取纯文本"""
    if not html_content:
        return ""

    try:
        soup = BeautifulSoup(html_content, "html.parser")

        for element in soup(["script", "style"]):
            element.decompose()

        text = soup.get_text(separator=" ")
        return normalize_whitespace(text)
    except Exception as e:
        logger.warning(f"Failed to strip HTML: {e}")

        return re.sub(r"<[^>]+>", " ", html_content)

def unescape_html(text: str) -> str:
    """解码 HTML 实体"""
    if not text:
        return ""
    return html.unescape(text)
