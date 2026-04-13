import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from crawl4ai import CacheMode, LLMConfig
from crawl4ai.content_filter_strategy import (
    BM25ContentFilter,
    LLMContentFilter,
    PruningContentFilter,
)
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator


class TablePreservingMarkdownGenerator(DefaultMarkdownGenerator):
    """
    扩展的 Markdown 生成器，当表格包含 rowspan 或 colspan 时保留原始 HTML。
    表格内容会清理多余的 style、class 等属性。
    """

    def generate(self, html: str, source_url: str = "", **kwargs) -> str:
        """
        生成 Markdown，若 HTML 中表格包含 rowspan 或 colspan：
        - 表格部分保留为清理后的 HTML
        - 其余内容转换为 Markdown

        Args:
            html: HTML 内容
            source_url: 源页面 URL，用于提取图片的 base URL
        """
        if not html:
            return ""

        # 从 source_url 提取 base URL
        base_url = self._extract_base_url(source_url) if source_url else ""

        soup = BeautifulSoup(html, "html.parser")

        # 将 wp_pdf_player iframe 转换为 markdown 链接
        self._convert_wp_pdf_iframe(soup, base_url)

        # 检查是否有复杂表格（包含 rowspan 或 colspan）
        complex_tables = self._find_complex_tables(soup)

        if not complex_tables:
            # 没有复杂表格，使用默认行为
            result = self.generate_markdown(str(soup), **kwargs)
            if hasattr(result, 'markdown'):
                markdown = result.markdown
            else:
                markdown = str(result)
            # 转换相对图片 URL 为绝对 URL
            return self._convert_image_urls(markdown, base_url)

        # 有复杂表格，需要混合处理
        return self._process_with_complex_tables(soup, base_url=base_url, **kwargs)

    def _extract_base_url(self, url: str) -> str:
        """从 URL 中提取 base URL (protocol + host)。"""
        if not url:
            return ""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            return ""

    def _convert_image_urls(self, markdown: str, base_url: str = "") -> str:
        """将 markdown 中的相对图片 URL 转换为绝对 URL。"""
        if not markdown:
            return markdown

        # 匹配 ![...](path) 或 ![](path) 格式的图片
        def replace_image_url(match):
            alt_text = match.group(1) if match.group(1) else ""
            path = match.group(2)
            # 如果是相对路径且是上传目录，转换为绝对路径
            if path.startswith("/_upload/") and base_url:
                return f'![{alt_text}]({base_url}{path})'
            return match.group(0)

        # 替换图片 URL
        pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
        return re.sub(pattern, replace_image_url, markdown)

    def _convert_wp_pdf_iframe(self, soup: BeautifulSoup, base_url: str = "") -> None:
        """
        将 wp_pdf_player iframe 转换为 markdown 链接。

        格式: <iframe class="wp_pdf_player" src="/_js/.../viewer.html?file=/_upload/.../xxx.pdf">
        """
        iframes = soup.find_all("iframe")
        for iframe in iframes:
            # 检查 class 属性是否包含 wp_pdf_player
            iframe_class = iframe.get("class", [])
            if not any("wp_pdf_player" in c for c in iframe_class):
                continue
            src = iframe.get("src", "")
            if "viewer.html" not in src:
                continue
            # 提取 file 参数中的 PDF URL
            match = re.search(r'file=([^&"]+)', src)
            if match:
                pdf_url = match.group(1)
                # URL 解码
                from urllib.parse import unquote
                pdf_url = unquote(pdf_url)
                # 转换为绝对 URL
                if pdf_url.startswith("/"):
                    pdf_url = base_url + pdf_url
                # 优先使用 title 属性作为链接文本
                filename = iframe.get("title", "") or pdf_url.split("/")[-1].replace(".pdf", "")
                if not filename:
                    filename = "PDF文档"
                # 替换为 markdown 链接
                new_tag = soup.new_tag("a", href=pdf_url)
                new_tag.string = filename
                iframe.replace_with(new_tag)

    def _find_complex_tables(self, soup: BeautifulSoup) -> list:
        """找出所有包含 rowspan 或 colspan 的表格。"""
        complex_tables = []
        for table in soup.find_all("table"):
            for td in table.find_all(["td", "th"]):
                if td.has_attr("rowspan") or td.has_attr("colspan"):
                    complex_tables.append(table)
                    break
        return complex_tables

    def _clean_table(self, table: BeautifulSoup) -> str:
        """清理表格，只保留 rowspan, colspan, valign, align, href, src, alt, title 属性。"""
        clean_table = BeautifulSoup(str(table), "html.parser").find("table")
        if not clean_table:
            return str(table)

        # 只保留这些属性（注意：不保留 valign 因为 React 不识别 vAlign）
        allowed_attrs = {"rowspan", "colspan", "align", "href", "src", "alt", "title"}

        # 要删除的图标图片
        icon_patterns = ("icon_pdf.gif", "icon_xls.gif", "icon_doc.gif")

        # 处理 table 标签本身
        for attr in list(clean_table.attrs):
            if attr not in allowed_attrs:
                del clean_table[attr]

        # 处理所有子元素
        for tag in clean_table.find_all(True):
            # 删除图标图片
            if tag.name == "img":
                src = tag.get("src", "")
                if any(src.endswith(icon) for icon in icon_patterns):
                    tag.decompose()
                    continue

            attrs_to_remove = [attr for attr in list(tag.attrs) if attr not in allowed_attrs]
            for attr in attrs_to_remove:
                del tag[attr]

        return str(clean_table)

    def _process_with_complex_tables(self, soup: BeautifulSoup, base_url: str = "", **kwargs) -> str:
        """
        处理包含复杂表格的 HTML：
        1. 用占位符替换复杂表格
        2. 将替换后的 HTML 转为 Markdown
        3. 用清理后的表格 HTML 替换占位符
        """
        complex_tables = self._find_complex_tables(soup)

        # 准备一个副本用于处理
        work_soup = BeautifulSoup(str(soup), "html.parser")

        # 将 wp_pdf_player iframe 转换为 markdown 链接
        self._convert_wp_pdf_iframe(work_soup, base_url)

        # 用占位符替换复杂表格
        table_replacements = []
        for i, table in enumerate(complex_tables):
            placeholder = f"__TABLE_PLACEHOLDER_{i}__"
            cleaned_html = self._clean_table(table)
            table_replacements.append((placeholder, cleaned_html))
            # 在工作副本中替换
            new_soup = BeautifulSoup(str(work_soup), "html.parser")
            for t in new_soup.find_all("table"):
                if t == table:
                    t.replace_with(BeautifulSoup(placeholder, "html.parser"))
            work_soup = new_soup

        # 将替换后的 HTML 转为 Markdown
        result = self.generate_markdown(str(work_soup), **kwargs)
        if hasattr(result, 'markdown'):
            markdown = result.markdown
        else:
            markdown = str(result)

        # 用清理后的表格 HTML 替换占位符
        for placeholder, cleaned_html in table_replacements:
            markdown = markdown.replace(placeholder, cleaned_html)

        # 转换相对图片 URL 为绝对 URL
        return self._convert_image_urls(markdown, base_url)


def normalize_cache_mode(value: Any, logger: logging.Logger) -> Any:
    if not isinstance(value, str):
        return value

    key = value.upper()
    if hasattr(CacheMode, key):
        return getattr(CacheMode, key)

    logger.warning("Invalid cache_mode override: %s", value)
    return value


def build_content_filter(config: Any, logger: logging.Logger) -> Any:
    if not isinstance(config, dict):
        return config

    filter_type = str(config.get("type", "")).strip().lower()
    params = dict(config.get("params", {}))

    if filter_type in {"", "none"}:
        return None
    if filter_type == "pruning":
        return PruningContentFilter(**params)
    if filter_type == "bm25":
        return BM25ContentFilter(**params)
    if filter_type == "llm":
        llm_cfg = params.pop("llm_config", None)
        if isinstance(llm_cfg, dict):
            params["llm_config"] = LLMConfig(**llm_cfg)
        return LLMContentFilter(**params)

    logger.warning("Unsupported content_filter type: %s", filter_type)
    return None


def build_markdown_generator(config: Any, logger: logging.Logger) -> Any:
    if not isinstance(config, dict):
        return config

    generator_type = str(config.get("type", "default")).strip().lower()
    if generator_type not in {"default", "defaultmarkdowngenerator", "table_preserving"}:
        logger.warning("Unsupported markdown_generator type: %s", generator_type)
        return None

    kwargs: dict[str, Any] = {}
    if "content_source" in config:
        kwargs["content_source"] = config["content_source"]
    if isinstance(config.get("options"), dict):
        kwargs["options"] = config["options"]
    if "content_filter" in config:
        kwargs["content_filter"] = build_content_filter(config["content_filter"], logger)

    # 使用支持复杂表格的生成器
    return TablePreservingMarkdownGenerator(**kwargs)


def normalize_crawler_overrides(
    overrides: dict[str, Any], logger: logging.Logger
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in overrides.items():
        if key == "cache_mode":
            normalized[key] = normalize_cache_mode(value, logger)
            continue
        if key == "markdown_generator":
            normalized[key] = build_markdown_generator(value, logger)
            continue

        normalized[key] = value

    return normalized