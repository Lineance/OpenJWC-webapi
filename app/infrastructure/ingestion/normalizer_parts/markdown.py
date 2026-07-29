from __future__ import annotations

from typing import Any

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

def markdown_to_text(md_content: str) -> str:
    """将 Markdown 内容转换为纯文本"""
    if not md_content:
        return ""

    try:

        html_content = markdown(md_content, extensions=["tables", "fenced_code"])

        soup = BeautifulSoup(html_content, "html.parser")

        for element in soup(["script", "style", "code", "pre"]):
            element.decompose()

        text = soup.get_text(separator=" ")

        text = normalize_whitespace(text)

        return text
    except Exception as e:
        logger.warning(f"Failed to convert markdown to text: {e}")

        return strip_markdown_simple(md_content)

def strip_markdown_simple(md_content: str) -> str:
    """简单的 Markdown 标记清理 (正则方式)"""
    if not md_content:
        return ""

    text = md_content

    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)

    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)

    text = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)

    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)
    text = re.sub(r"__([^_]+)__", r"\1", text)
    text = re.sub(r"_([^_]+)_", r"\1", text)

    text = re.sub(r"```[\s\S]*?```", "", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)

    text = re.sub(r"^>\s*", "", text, flags=re.MULTILINE)

    text = re.sub(r"^[\*\-\+]\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\d+\.\s+", "", text, flags=re.MULTILINE)

    text = re.sub(r"^[-*_]{3,}$", "", text, flags=re.MULTILINE)

    return normalize_whitespace(text)

def normalize_markdown(markdown: str) -> str:
    """规范化 markdown 格式，修复常见的格式错误"""
    if not markdown:
        return markdown

    markdown = re.sub(r'\*\*[ \t\r\n]*\*\*', '', markdown)

    markdown = re.sub(r'!\[\]([^\[\n]*)\[', r'![]\1 [', markdown)

    markdown = re.sub(r'(?<!\*)\*{4,}(?!\*)', '', markdown)

    markdown = re.sub(r'\*{4,}', '**', markdown)

    markdown = re.sub(r'\*\*([^*]+)\*\*(\|)', r'\1\2', markdown)
    markdown = re.sub(r'\*\*([^*]+)\*\*(\s*$)', r'\1\2', markdown)

    markdown = re.sub(r'(\|\s*){4,}', '', markdown)

    def fix_separator(match: Any) -> Any:
        content = match.group(0)
        if not content.startswith('|'):
            content = '|' + content
        if not content.rstrip().endswith('|'):
            content = content.rstrip() + '|'
        return content
    markdown = re.sub(r'^(?=.*\|)(?=.*-)[^\n]+$', fix_separator, markdown, flags=re.MULTILINE)

    markdown = re.sub(r'^(?!.*\|.*$)[-:\s]+$', '', markdown, flags=re.MULTILINE)

    lines = markdown.split('\n')
    result_lines = []
    in_table = False
    prev_ended_with_text = False
    prev_line_was_image = False

    for line in lines:
        stripped = line.strip()

        is_table_row = '|' in stripped and not re.match(r'^[\s|:-]+$', stripped)
        is_image_line = stripped.startswith('![](') or stripped.startswith('![')

        if is_table_row:

            result_lines.append(line)
            in_table = True
        elif is_image_line:

            result_lines.append(line)
            in_table = False
        else:
            if not stripped:

                result_lines.append(line)
                in_table = False
            elif stripped.startswith('#') or stripped.startswith('- [') or stripped.startswith('```'):

                result_lines.append(line)
                in_table = False
            elif prev_ended_with_text and not stripped.startswith('|'):

                result_lines.append('')
                result_lines.append(line)
                in_table = False
            else:
                result_lines.append(line)
                in_table = False

        prev_ended_with_text = bool(stripped) and not stripped.startswith('|') and not is_image_line
        prev_line_was_image = is_image_line

    return '\n'.join(result_lines)
