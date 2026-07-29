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
    """扩展的 Markdown 生成器，当表格包含 rowspan 或 colspan 时保留原始 HTML。"""

    def generate(self, html: str, source_url: str = "", **kwargs: Any) -> str:
        """生成 Markdown，若 HTML 中表格包含 rowspan 或 colspan："""
        if not html:
            return ""

        base_url = self._extract_base_url(source_url) if source_url else ""

        soup = BeautifulSoup(html, "html.parser")

        self._convert_wp_pdf_iframe(soup, base_url)

        complex_tables = self._find_complex_tables(soup)

        if not complex_tables:

            result = self.generate_markdown(str(soup), **kwargs)
            if hasattr(result, 'markdown'):
                markdown = result.markdown
            else:
                markdown = str(result)

            return self._convert_image_urls(markdown, base_url)

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

        def replace_image_url(match: Any) -> Any:
            alt_text = match.group(1) if match.group(1) else ""
            path = match.group(2)

            if path.startswith("/_upload/") and base_url:
                return f'![{alt_text}]({base_url}{path})'
            return match.group(0)

        pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
        return re.sub(pattern, replace_image_url, markdown)

    def _convert_wp_pdf_iframe(self, soup: BeautifulSoup, base_url: str = "") -> None:
        """将 wp_pdf_player iframe 转换为 markdown 链接。"""
        iframes = soup.find_all("iframe")
        for iframe in iframes:

            iframe_class = iframe.get("class", [])
            if not any("wp_pdf_player" in c for c in iframe_class):
                continue
            src = iframe.get("src", "")
            if "viewer.html" not in src:
                continue

            match = re.search(r'file=([^&"]+)', src)
            if match:
                pdf_url = match.group(1)

                from urllib.parse import unquote
                pdf_url = unquote(pdf_url)

                if pdf_url.startswith("/"):
                    pdf_url = base_url + pdf_url

                filename = iframe.get("title", "") or pdf_url.split("/")[-1].replace(".pdf", "")
                if not filename:
                    filename = "PDF文档"

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

        allowed_attrs = {"rowspan", "colspan", "align", "href", "src", "alt", "title"}

        icon_patterns = ("icon_pdf.gif", "icon_xls.gif", "icon_doc.gif")

        for attr in list(clean_table.attrs):
            if attr not in allowed_attrs:
                del clean_table[attr]

        for tag in clean_table.find_all(True):

            if tag.name == "img":
                src = tag.get("src", "")
                if any(src.endswith(icon) for icon in icon_patterns):
                    tag.decompose()
                    continue

            attrs_to_remove = [attr for attr in list(tag.attrs) if attr not in allowed_attrs]
            for attr in attrs_to_remove:
                del tag[attr]

        return str(clean_table)

    def _process_with_complex_tables(self, soup: BeautifulSoup, base_url: str = "", **kwargs: Any) -> str:
        """处理包含复杂表格的 HTML："""
        complex_tables = self._find_complex_tables(soup)

        work_soup = BeautifulSoup(str(soup), "html.parser")

        self._convert_wp_pdf_iframe(work_soup, base_url)

        table_replacements = []
        for i, table in enumerate(complex_tables):
            placeholder = f"__TABLE_PLACEHOLDER_{i}__"
            cleaned_html = self._clean_table(table)
            table_replacements.append((placeholder, cleaned_html))

            new_soup = BeautifulSoup(str(work_soup), "html.parser")
            for t in new_soup.find_all("table"):
                if t == table:
                    t.replace_with(BeautifulSoup(placeholder, "html.parser"))
            work_soup = new_soup

        result = self.generate_markdown(str(work_soup), **kwargs)
        if hasattr(result, 'markdown'):
            markdown = result.markdown
        else:
            markdown = str(result)

        for placeholder, cleaned_html in table_replacements:
            markdown = markdown.replace(placeholder, cleaned_html)

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
