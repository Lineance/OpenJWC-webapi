from __future__ import annotations

from app.infrastructure.crawler.python_crawler.article_url_crawler import (
    Any,
    AsyncWebCrawler,
    BFSDeepCrawlStrategy,
    BeautifulSoup,
    BrowserConfig,
    CrawlerRunConfig,
    Path,
    datetime,
    importlib,
    logging,
    normalize_crawler_overrides,
    re,
    yaml,
)

class ArticleFormatMixin:
    """封装 ArticleUrlCrawler 的单一职责方法。"""

    def _format_result(self, result: Any) -> dict[str, Any]:
        if isinstance(result, dict):
            defaults = {
                "success": False,
                "url": "",
                "title": "",
                "publish_date": "",
                "author": "",
                "content": "",
                "markdown": "",
                "error": result.get("error", "Unknown internal error"),
                "metadata": {
                    "crawled_at": datetime.now().isoformat(),
                    "word_count": 0,
                    "is_pdf": False,
                    "depth": 0,
                },
                "pdf_size": 0,
            }
            for key, value in defaults.items():
                result.setdefault(key, value)
            return result

        markdown_raw = ""
        markdown_fit = ""
        markdown_citations = ""
        markdown_refs = ""

        markdown_obj = getattr(result, "markdown", None)
        if markdown_obj:
            markdown_raw = getattr(markdown_obj, "raw_markdown", "") or ""
            markdown_fit = getattr(markdown_obj, "fit_markdown", "") or ""
            markdown_citations = getattr(markdown_obj, "markdown_with_citations", "") or ""
            markdown_refs = getattr(markdown_obj, "references_markdown", "") or ""

        if not markdown_raw and hasattr(result, "markdown_v2") and result.markdown_v2:
            markdown_raw = getattr(result.markdown_v2, "raw_markdown", "") or ""
        if not markdown_raw and isinstance(markdown_obj, str):
            markdown_raw = markdown_obj

        url = getattr(result, "url", "")
        success = getattr(result, "success", False)
        error_msg = getattr(result, "error_message", None)

        markdown_result = getattr(result, "markdown", "") or ""
        content_result = getattr(result, "content", "") or ""

        pre_extracted_title = getattr(result, "_pre_extracted_title", "") or ""
        pre_extracted_date = getattr(result, "_pre_extracted_date", "") or ""

        formatted = {
            "success": success,
            "url": url,
            "title": pre_extracted_title,
            "publish_date": pre_extracted_date,
            "author": "",
            "content": content_result,
            "markdown": markdown_result,
            "raw_markdown": markdown_raw,
            "fit_markdown": markdown_fit,
            "markdown_with_citations": markdown_citations,
            "references_markdown": markdown_refs,
            "metadata": {
                "crawled_at": datetime.now().isoformat(),
                "word_count": getattr(result, "word_count", 0) or 0,
                "is_pdf": getattr(result, "pdf", None) is not None
                or (url and url.lower().endswith(".pdf")),
                "depth": getattr(result, "depth", 0) or 0,
                "cache_status": getattr(result, "cache_status", ""),
            },
            "pdf_size": len(result.pdf) if getattr(result, "pdf", None) else 0,
        }

        if not success:
            formatted["error"] = error_msg or "Crawl failed without specific error message"

        raw_html = getattr(result, "html", "") or ""
        if raw_html and not pre_extracted_title and not pre_extracted_date:
            metadata = self._extract_metadata(
                raw_html,
                title_selectors=[
                    ".Article_Title",
                    ".News-title",
                    "h1",
                    ".article-title",
                    "[class*=title]",
                ],
                date_selectors=[".Article_PublishDate", ".publish-date", ".date", "time"],
                author_selectors=[".author", ".Article_Author", ".writer"],
            )

            if not formatted.get("title") and metadata.get("title"):
                formatted["title"] = metadata.get("title")

            if metadata.get("date"):
                formatted["publish_date"] = metadata.get("date")

            if metadata.get("author"):
                formatted["author"] = metadata.get("author")

        if formatted.get("title") or formatted.get("publish_date"):
            markdown_text = str(formatted.get("markdown", ""))
            cleaned_markdown = self._clean_markdown_content(
                markdown_text,
                title=formatted.get("title", ""),
                date=formatted.get("publish_date", ""),
            )
            formatted["markdown"] = cleaned_markdown
            formatted["content"] = cleaned_markdown

        if not formatted.get("title") and formatted.get("markdown"):
            markdown_text = str(formatted.get("markdown", ""))
            title_from_content = self._extract_title_from_content(markdown_text)
            if title_from_content:
                formatted["title"] = title_from_content
            else:

                sentences = re.split(r'[。.]', markdown_text)
                if sentences:
                    first_sentence = sentences[0].strip()

                    if first_sentence and not first_sentence.startswith("!["):

                        if len(first_sentence) > 100:
                            first_sentence = first_sentence[:100] + "..."
                        formatted["title"] = first_sentence

        if not formatted.get("publish_date") and formatted.get("markdown"):
            markdown_text = str(formatted.get("markdown", ""))

            date_match = re.search(r'(\d{1,2}年)?\d{1,2}月\d{1,2}[日号]', markdown_text)
            if date_match:
                formatted["publish_date"] = date_match.group(0)

        return formatted
