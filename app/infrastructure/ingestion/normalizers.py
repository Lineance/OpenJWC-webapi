"""Data Normalizers - 数据标准化工具"""

import html
import logging
import re
import unicodedata
from datetime import UTC, datetime, timezone
from typing import Literal

from bs4 import BeautifulSoup
from markdown import markdown

logger = logging.getLogger(__name__)

from app.infrastructure.ingestion.normalizer_parts.markdown import (
    markdown_to_text,
    strip_markdown_simple,
    normalize_markdown,
)

from app.infrastructure.ingestion.normalizer_parts.html import (
    strip_html,
    unescape_html,
)

from app.infrastructure.ingestion.normalizer_parts.datetime import (
    normalize_datetime,
    format_datetime,
)

from app.infrastructure.ingestion.normalizer_parts.text import (
    normalize_unicode,
    normalize_whitespace,
    normalize_newlines,
    truncate_text,
)

from app.infrastructure.ingestion.normalizer_parts.content import (
    extract_first_sentence,
    normalize_content,
)
