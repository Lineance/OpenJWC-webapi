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

class ArticleCrawlMixin:
    """封装 ArticleUrlCrawler 的单一职责方法。"""

    async def crawl_article(
        self,
        url: str,
        run_config: CrawlerRunConfig,
    ) -> dict[str, Any]:
        if not self._crawler_instance:
            await self.start()

        crawler = self._crawler_instance
        if crawler is None:
            raise RuntimeError("Crawler instance is not initialized")

        css_selector = getattr(run_config, "css_selector", None)

        if css_selector:

            meta_only_config = CrawlerRunConfig(
                page_timeout=getattr(run_config, "page_timeout", 30000),
                cache_mode=getattr(run_config, "cache_mode", None),
                check_cache_freshness=getattr(run_config, "check_cache_freshness", False),
            )
            result_with_meta = await crawler.arun(url=url, config=meta_only_config)

            inner = result_with_meta._results[0] if hasattr(result_with_meta, "_results") else result_with_meta
            full_html = inner.html if hasattr(inner, "html") else ""

            title = ""
            publish_date = ""

            if full_html:
                metadata = self._extract_metadata(
                    full_html,
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
                title = metadata.get("title", "")
                publish_date = metadata.get("date", "")

            content_only_config = CrawlerRunConfig(
                css_selector=css_selector,
                page_timeout=getattr(run_config, "page_timeout", 30000),
                cache_mode=getattr(run_config, "cache_mode", None),
                check_cache_freshness=getattr(run_config, "check_cache_freshness", False),
            )
            result_content = await crawler.arun(url=url, config=content_only_config)

            combined_result = result_content._results[0] if hasattr(result_content, "_results") else result_content

            raw_html = getattr(combined_result, "html", "") or ""
            md_generator = getattr(run_config, "markdown_generator", None)
            generated_markdown = ""
            if raw_html and md_generator:
                try:

                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(raw_html, "html.parser")
                    selected = soup.select_one(css_selector)
                    if selected:
                        content_html = str(selected)
                    else:
                        content_html = raw_html

                    generated_markdown = md_generator.generate(content_html, source_url=url)
                    if hasattr(generated_markdown, 'markdown'):
                        generated_markdown = generated_markdown.markdown
                except Exception as e:
                    self.logger.warning(f"Failed to generate markdown: {e}")

            if hasattr(combined_result, "html") and full_html:

                combined_result.html = full_html

            if title or publish_date:
                combined_result._pre_extracted_title = title
                combined_result._pre_extracted_date = publish_date

            formatted = self._format_result(combined_result)

            if generated_markdown:
                formatted["markdown"] = generated_markdown

            return formatted
        else:

            res = await crawler.arun(url=url, config=run_config)
            return self._format_result(res)

    async def crawl_articles(
        self,
        urls: list[str],
        run_config: CrawlerRunConfig,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for url in urls:
            try:
                results.append(await self.crawl_article(url=url, run_config=run_config))
            except Exception as exc:
                self.logger.error("Failed to crawl article %s: %s", url, exc)
                results.append(
                    {
                        "success": False,
                        "url": url,
                        "title": "",
                        "publish_date": "",
                        "author": "",
                        "error": str(exc),
                        "markdown": "",
                        "metadata": {
                            "crawled_at": datetime.now().isoformat(),
                            "word_count": 0,
                            "is_pdf": False,
                            "depth": 0,
                        },
                        "pdf_size": 0,
                    }
                )
        return results
