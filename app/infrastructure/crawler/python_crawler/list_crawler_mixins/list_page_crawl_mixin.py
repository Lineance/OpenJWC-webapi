from __future__ import annotations

from app.infrastructure.crawler.python_crawler.list_incremental_crawler import (
    Any,
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    Path,
    importlib,
    json,
    logging,
    md5,
    normalize_crawler_overrides,
    re,
    urlparse,
    yaml,
)

class ListPageCrawlMixin:
    """封装 ListIncrementalCrawler 的单一职责方法。"""

    async def crawl_list_incremental(
        self,
        list_url: str,
        max_pages: int = 31,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        state_file_path: str | Path | None = None,
        run_config: CrawlerRunConfig | None = None,
        initialize: bool = True,
    ) -> list[str]:
        if initialize:
            self._init_configs()
            await self.start()
        elif self._crawler_instance is None:
            await self.start()

        include_patterns = include_patterns or []
        exclude_patterns = exclude_patterns or []

        effective_state_file = (
            self._resolve_path(str(state_file_path)) if state_file_path else self.state_file
        )

        if run_config is None:
            run_config = self.crawler_config.clone() if self.crawler_config else CrawlerRunConfig()

        crawler = self._crawler_instance
        if crawler is None:
            raise RuntimeError("Crawler instance is not initialized")

        domain = urlparse(list_url).netloc
        discovered_links: set[str] = set()

        for page_num in range(1, max_pages + 1):
            page_url = self._build_list_page_url(list_url, page_num)
            result = await crawler.arun(url=page_url, config=run_config)

            if not result.success:
                if page_num == 1:
                    self.logger.warning("Failed to crawl first list page: %s", page_url)
                break

            page_links = result.links.get("internal", []) if hasattr(result, "links") else []
            if not page_links and page_num > 1:
                break

            before_count = len(discovered_links)
            for link_obj in page_links:
                href = link_obj.get("href") if isinstance(link_obj, dict) else ""
                if not isinstance(href, str):
                    continue
                full_url = self._normalize_link(href, domain)
                if not full_url:
                    continue
                if self._is_allowed(full_url, include_patterns, exclude_patterns):
                    discovered_links.add(full_url)

            if len(discovered_links) == before_count and page_num > 1:
                break

        seen_urls = self._load_state(effective_state_file)
        incremental_urls = sorted(url for url in discovered_links if url not in seen_urls)
        self._save_state(seen_urls | discovered_links, effective_state_file)

        self.logger.info(
            "List crawl done: discovered=%d incremental=%d state_file=%s",
            len(discovered_links),
            len(incremental_urls),
            effective_state_file,
        )
        return incremental_urls
