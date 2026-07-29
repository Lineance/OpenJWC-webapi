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

from app.infrastructure.ingestion.normalizer_parts.html import strip_html, unescape_html
from app.infrastructure.ingestion.normalizer_parts.markdown import markdown_to_text, strip_markdown_simple
from app.infrastructure.ingestion.normalizer_parts.text import normalize_newlines, normalize_unicode, normalize_whitespace, truncate_text

def extract_first_sentence(
    text: str,
    is_markdown: bool = True,
    max_title_length: int = 100,
) -> str:
    """从文本中提取第一句作为标题"""
    if not text:
        return ""

    if is_markdown:

        title_match = re.search(r"^#+\s*(.+?)$", text, re.MULTILINE)
        if title_match:
            title = title_match.group(1).strip()
            if title:

                title = strip_markdown_simple(title)
                title = normalize_whitespace(title)

                if len(title) > max_title_length:
                    return truncate_text(title, max_title_length)
                return title

    content = markdown_to_text(text) if is_markdown else strip_html(text)

    content = unescape_html(content)

    content = normalize_whitespace(content)

    if not content:
        return ""

    sentence_delimiters = r"[。！？?!\.…]+"

    match = re.search(sentence_delimiters, content)

    first_sentence = content[: match.end()] if match else content

    first_sentence = normalize_whitespace(first_sentence)

    if len(first_sentence) > max_title_length:

        for delimiter in ["。", "！", "？", "!", "?", ".", "，", ",", "；", ";"]:
            idx = first_sentence.rfind(delimiter, 0, max_title_length)
            if idx != -1:
                return first_sentence[: idx + len(delimiter)]

        return first_sentence[:max_title_length] + "..."

    return first_sentence

def normalize_content(
    content: str,
    is_markdown: bool = True,
    max_length: int | None = None,
) -> str:
    """综合内容标准化"""
    if not content:
        return ""

    text = normalize_unicode(content)

    text = normalize_newlines(text)

    text = markdown_to_text(text) if is_markdown else strip_html(text)

    text = unescape_html(text)

    text = normalize_whitespace(text)

    if max_length:
        text = truncate_text(text, max_length)

    return text
