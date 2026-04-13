"""
Data Normalizers - 数据标准化工具

提供文本和数据的标准化处理，包括:
- Markdown 转纯文本
- 日期时间标准化
- HTML 标签清理
- Unicode 规范化

Responsibilities:
    - DateTime format standardization (ISO8601)
    - HTML tag stripping
    - Unicode normalization
    - Markdown to plain text conversion
"""

import html
import logging
import re
import unicodedata
from datetime import UTC, datetime, timezone
from typing import Literal

from bs4 import BeautifulSoup
from markdown import markdown

logger = logging.getLogger(__name__)


# =============================================================================
# Markdown 处理
# =============================================================================


def markdown_to_text(md_content: str) -> str:
    """
    将 Markdown 内容转换为纯文本

    处理流程:
    1. Markdown → HTML
    2. HTML → 纯文本 (BeautifulSoup)
    3. 清理多余空白

    Args:
        md_content: Markdown 格式的内容

    Returns:
        提取的纯文本内容

    Example:
        >>> markdown_to_text("# 标题\\n**加粗** 和 [链接](url)")
        '标题 加粗 和 链接'
    """
    if not md_content:
        return ""

    try:
        # Markdown → HTML
        html_content = markdown(md_content, extensions=["tables", "fenced_code"])

        # HTML → 纯文本
        soup = BeautifulSoup(html_content, "html.parser")

        # 移除脚本和样式
        for element in soup(["script", "style", "code", "pre"]):
            element.decompose()

        # 获取文本
        text = soup.get_text(separator=" ")

        # 清理空白
        text = normalize_whitespace(text)

        return text
    except Exception as e:
        logger.warning(f"Failed to convert markdown to text: {e}")
        # 降级处理：简单的正则清理
        return strip_markdown_simple(md_content)


def strip_markdown_simple(md_content: str) -> str:
    """
    简单的 Markdown 标记清理 (正则方式)

    用于 markdown 库失败时的降级处理

    Args:
        md_content: Markdown 内容

    Returns:
        清理后的文本
    """
    if not md_content:
        return ""

    text = md_content

    # 移除图片
    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)

    # 移除链接，保留文字
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)

    # 移除标题标记
    text = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)

    # 移除强调标记
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)
    text = re.sub(r"__([^_]+)__", r"\1", text)
    text = re.sub(r"_([^_]+)_", r"\1", text)

    # 移除代码块
    text = re.sub(r"```[\s\S]*?```", "", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)

    # 移除引用标记
    text = re.sub(r"^>\s*", "", text, flags=re.MULTILINE)

    # 移除列表标记
    text = re.sub(r"^[\*\-\+]\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\d+\.\s+", "", text, flags=re.MULTILINE)

    # 移除水平线
    text = re.sub(r"^[-*_]{3,}$", "", text, flags=re.MULTILINE)

    return normalize_whitespace(text)


def normalize_markdown(markdown: str) -> str:
    """
    规范化 markdown 格式，修复常见的格式错误

    处理流程:
    1. 去除 HTML <strong> 标签（包括空标签）
    2. 修复 PDF 图标和链接相邻问题
    3. 修复多余的星号
    4. 修复换行：段落内单个换行转为硬换行（不干扰表格）

    Args:
        markdown: 原始 markdown 内容

    Returns:
        规范化后的 markdown 内容
    """
    if not markdown:
        return markdown

    # 去除空的 ** 模式（来自 HTML <strong><br></strong> 转换）
    markdown = re.sub(r'\*\*[ \t\r\n]*\*\*', '', markdown)

    # 修复 PDF 图标 + 链接相邻: ![](url)[name](link) → ![](url) [name](link)
    # 使用 [^\[\n]* 避免跨行匹配
    markdown = re.sub(r'!\[\]([^\[\n]*)\[', r'![]\1 [', markdown)

    # 删除多余的连续星号: **8****月30日** → **8月30日**
    # 第一步：删除不在 ** 包围中的 ****
    markdown = re.sub(r'(?<!\*)\*{4,}(?!\*)', '', markdown)
    # 第二步：处理剩余的多个星号
    markdown = re.sub(r'\*{4,}', '**', markdown)

    # 修复表格表头中的粗体标记: **序号**| → 序号|
    # 粗体标记直接跟着表格分隔符，导致表格解析失败
    # 处理两种情况：末尾有|的和末尾没有|的
    markdown = re.sub(r'\*\*([^*]+)\*\*(\|)', r'\1\2', markdown)
    markdown = re.sub(r'\*\*([^*]+)\*\*(\s*$)', r'\1\2', markdown)

    # 删除连续的四个竖线（带空格）
    markdown = re.sub(r'(\|\s*){4,}', '', markdown)

    # 修复表格分隔行：确保首尾有 |（在删除standalone ---之前运行）
    # 只匹配包含 - 和 | 的分隔行
    def fix_separator(match):
        content = match.group(0)
        if not content.startswith('|'):
            content = '|' + content
        if not content.rstrip().endswith('|'):
            content = content.rstrip() + '|'
        return content
    markdown = re.sub(r'^(?=.*\|)(?=.*-)[^\n]+$', fix_separator, markdown, flags=re.MULTILINE)

    # 删除单独一行的 --- 分隔线（不包含 | 的）
    markdown = re.sub(r'^(?!.*\|.*$)[-:\s]+$', '', markdown, flags=re.MULTILINE)

    # 修复换行：段落内单个换行转为硬换行
    # 使用状态机追踪是否在表格行内
    lines = markdown.split('\n')
    result_lines = []
    in_table = False
    prev_ended_with_text = False
    prev_line_was_image = False

    for line in lines:
        stripped = line.strip()
        # 表格行检测：包含 | 但不是分隔行
        is_table_row = '|' in stripped and not re.match(r'^[\s|:-]+$', stripped)
        is_image_line = stripped.startswith('![](') or stripped.startswith('![')

        if is_table_row:
            # 表格行保持原样
            result_lines.append(line)
            in_table = True
        elif is_image_line:
            # 图片行单独成行，不合并
            result_lines.append(line)
            in_table = False
        else:
            if not stripped:
                # 空行，重置状态
                result_lines.append(line)
                in_table = False
            elif stripped.startswith('#') or stripped.startswith('- [') or stripped.startswith('```'):
                # 标题、列表项、代码块保持原样
                result_lines.append(line)
                in_table = False
            elif prev_ended_with_text and not stripped.startswith('|'):
                # 上一行是非空文本，当前行非空且不是表格行 → 添加空行分隔
                result_lines.append('')
                result_lines.append(line)
                in_table = False
            else:
                result_lines.append(line)
                in_table = False

        prev_ended_with_text = bool(stripped) and not stripped.startswith('|') and not is_image_line
        prev_line_was_image = is_image_line

    return '\n'.join(result_lines)


# =============================================================================
# HTML 处理
# =============================================================================


def strip_html(html_content: str) -> str:
    """
    移除 HTML 标签，提取纯文本

    Args:
        html_content: HTML 内容

    Returns:
        纯文本内容
    """
    if not html_content:
        return ""

    try:
        soup = BeautifulSoup(html_content, "html.parser")

        # 移除脚本和样式
        for element in soup(["script", "style"]):
            element.decompose()

        text = soup.get_text(separator=" ")
        return normalize_whitespace(text)
    except Exception as e:
        logger.warning(f"Failed to strip HTML: {e}")
        # 降级处理
        return re.sub(r"<[^>]+>", " ", html_content)


def unescape_html(text: str) -> str:
    """
    解码 HTML 实体

    Args:
        text: 包含 HTML 实体的文本

    Returns:
        解码后的文本

    Example:
        >>> unescape_html("& < >")
        '& < >'
    """
    if not text:
        return ""
    return html.unescape(text)


# =============================================================================
# 日期时间处理
# =============================================================================


def normalize_datetime(
    date_input: str | datetime | None,
    default_tz: timezone = UTC,
) -> datetime | None:
    """
    标准化日期时间为 ISO8601 格式 (UTC 时区)

    支持的输入格式:
    - ISO8601: "2024-05-20T10:30:00"
    - 中文格式: "2024年5月20日"
    - 斜杠格式: "2024/05/20 10:30"
    - 日期对象: datetime 实例

    Args:
        date_input: 日期输入 (字符串或 datetime)
        default_tz: 默认时区

    Returns:
        标准化的 datetime 对象，无法解析返回 None
    """
    if date_input is None:
        return None

    if isinstance(date_input, datetime):
        # 确保有时区信息
        if date_input.tzinfo is None:
            return date_input.replace(tzinfo=default_tz)
        return date_input

    if not isinstance(date_input, str):
        return None

    date_str = date_input.strip()
    if not date_str:
        return None

    # 尝试多种格式解析
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",  # ISO8601 with timezone
        "%Y-%m-%dT%H:%M:%S.%f%z",  # ISO8601 with microseconds
        "%Y-%m-%dT%H:%M:%S",  # ISO8601
        "%Y-%m-%dT%H:%M",  # ISO8601 short
        "%Y-%m-%d %H:%M:%S",  # Standard
        "%Y-%m-%d %H:%M",  # Standard short
        "%Y-%m-%d",  # Date only
        "%Y/%m/%d %H:%M:%S",  # Slash format
        "%Y/%m/%d %H:%M",  # Slash format short
        "%Y/%m/%d",  # Slash date only
        "%d/%m/%Y",  # European
        "%m/%d/%Y",  # US
    ]

    # 预处理中文日期格式
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
    """
    格式化日期时间为字符串

    Args:
        dt: datetime 对象
        fmt: 输出格式

    Returns:
        格式化的字符串
    """
    if dt is None:
        return ""
    return dt.strftime(fmt)


# =============================================================================
# 文本标准化
# =============================================================================


def normalize_unicode(text: str, form: Literal["NFC", "NFD", "NFKC", "NFKD"] = "NFC") -> str:
    """
    Unicode 规范化

    Args:
        text: 输入文本
        form: 规范化形式 (NFC, NFD, NFKC, NFKD)

    Returns:
        规范化后的文本
    """
    if not text:
        return ""
    return unicodedata.normalize(form, text)


def normalize_whitespace(text: str) -> str:
    """
    规范化空白字符

    - 将多个空白字符合并为单个空格
    - 移除首尾空白

    Args:
        text: 输入文本

    Returns:
        规范化后的文本
    """
    if not text:
        return ""
    # 将所有空白字符替换为空格
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_newlines(text: str) -> str:
    """
    规范化换行符为 Unix 风格 (\\n)

    Args:
        text: 输入文本

    Returns:
        规范化后的文本
    """
    if not text:
        return ""
    # Windows → Unix
    text = text.replace("\r\n", "\n")
    # Old Mac → Unix
    text = text.replace("\r", "\n")
    return text


def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """
    截断文本到指定长度

    Args:
        text: 输入文本
        max_length: 最大长度
        suffix: 截断后缀

    Returns:
        截断后的文本
    """
    if not text or len(text) <= max_length:
        return text or ""

    return text[: max_length - len(suffix)] + suffix


# =============================================================================
# 综合标准化
# =============================================================================


def extract_first_sentence(
    text: str,
    is_markdown: bool = True,
    max_title_length: int = 100,
) -> str:
    """
    从文本中提取第一句作为标题

    处理流程:
    1. 如果是Markdown，先尝试提取标题（#开头的行）
    2. 如果没有标题，转换为纯文本后提取第一句
    3. 使用句子分隔符分割文本
    4. 提取第一句，如果太长则截断

    Args:
        text: 原始文本（Markdown或纯文本）
        is_markdown: 是否为Markdown格式
        max_title_length: 最大标题长度

    Returns:
        提取的第一句文本
    """
    if not text:
        return ""

    # 如果是Markdown，先尝试提取标题
    if is_markdown:
        # 查找第一个标题（#开头的行）
        title_match = re.search(r"^#+\s*(.+?)$", text, re.MULTILINE)
        if title_match:
            title = title_match.group(1).strip()
            if title:
                # 清理标题中的markdown标记
                title = strip_markdown_simple(title)
                title = normalize_whitespace(title)
                # 如果标题太长，截断
                if len(title) > max_title_length:
                    return truncate_text(title, max_title_length)
                return title

    # 如果没有找到标题，或者不是Markdown，转换为纯文本
    content = markdown_to_text(text) if is_markdown else strip_html(text)

    # HTML实体解码
    content = unescape_html(content)

    # 空白规范化
    content = normalize_whitespace(content)

    if not content:
        return ""

    # 中文句子分隔符：句号、问号、感叹号、省略号
    sentence_delimiters = r"[。！？?!\.…]+"

    # 查找第一个句子分隔符
    match = re.search(sentence_delimiters, content)

    # 提取到第一个分隔符之前的内容；未匹配时使用整个文本
    first_sentence = content[: match.end()] if match else content

    # 清理空白
    first_sentence = normalize_whitespace(first_sentence)

    # 如果句子太长，截断
    if len(first_sentence) > max_title_length:
        # 尝试在标点处截断
        for delimiter in ["。", "！", "？", "!", "?", ".", "，", ",", "；", ";"]:
            idx = first_sentence.rfind(delimiter, 0, max_title_length)
            if idx != -1:
                return first_sentence[: idx + len(delimiter)]

        # 没有找到合适的截断点，直接截断
        return first_sentence[:max_title_length] + "..."

    return first_sentence


def normalize_content(
    content: str,
    is_markdown: bool = True,
    max_length: int | None = None,
) -> str:
    """
    综合内容标准化

    处理流程:
    1. Unicode 规范化
    2. Markdown/HTML 转纯文本
    3. HTML 实体解码
    4. 空白规范化
    5. 可选长度截断

    Args:
        content: 原始内容
        is_markdown: 是否为 Markdown 格式
        max_length: 最大长度限制

    Returns:
        标准化后的纯文本
    """
    if not content:
        return ""

    # Unicode 规范化
    text = normalize_unicode(content)

    # 换行规范化
    text = normalize_newlines(text)

    # 转纯文本
    text = markdown_to_text(text) if is_markdown else strip_html(text)

    # HTML 实体解码
    text = unescape_html(text)

    # 空白规范化
    text = normalize_whitespace(text)

    # 长度截断
    if max_length:
        text = truncate_text(text, max_length)

    return text
