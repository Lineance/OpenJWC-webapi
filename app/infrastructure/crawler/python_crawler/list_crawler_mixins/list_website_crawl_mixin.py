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

class ListWebsiteCrawlMixin:
    """封装 ListIncrementalCrawler 的单一职责方法。"""

    async def crawl_website_incremental(
        self,
        website_name: str,
        max_pages: int | None = None,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        list_crawler_overrides: dict[str, Any] | None = None,
        article_crawler_overrides: dict[str, Any] | None = None,
        browser_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._init_configs()
        website_cfg = self.load_website_config(website_name).get("website", {})
        start_urls = website_cfg.get("start_urls", [])
        list_cfg = website_cfg.get("list_incremental", {})
        overrides = website_cfg.get("overrides", {})

        list_crawler_cfg = dict(overrides.get("list_crawler", overrides.get("crawler", {})))
        article_crawler_cfg = dict(overrides.get("article_crawler", overrides.get("crawler", {})))
        browser_cfg_overrides = dict(overrides.get("browser", {}))

        if list_crawler_overrides:
            list_crawler_cfg.update(list_crawler_overrides)
        if article_crawler_overrides:
            article_crawler_cfg.update(article_crawler_overrides)
        if browser_overrides:
            browser_cfg_overrides.update(browser_overrides)

        if not start_urls:
            return {
                "website": website_name,
                "source": website_cfg.get("name"),
                "start_urls": [],
                "lists": [],
                "incremental_urls": [],
                "article_overrides": {
                    "crawler": article_crawler_cfg,
                    "browser": browser_cfg_overrides,
                },
            }

        max_pages_value = max_pages if max_pages is not None else int(list_cfg.get("max_pages", 31))
        include_value = (
            include_patterns if include_patterns else list_cfg.get("include_patterns", [])
        )
        exclude_value = (
            exclude_patterns if exclude_patterns else list_cfg.get("exclude_patterns", [])
        )

        if list_cfg.get("cache_base_directory"):
            self.cache_base_directory = self._resolve_path(list_cfg["cache_base_directory"])
            self.cache_base_directory.mkdir(parents=True, exist_ok=True)

        base_state = self._resolve_path(list_cfg.get("state_file", str(self.state_file)))
        base_state.parent.mkdir(parents=True, exist_ok=True)

        browser_config = self.browser_config.clone() if self.browser_config else BrowserConfig()
        browser_config = self._merge_browser_configs(browser_config, browser_cfg_overrides)
        self.browser_config = browser_config

        if self._crawler_instance is not None:
            await self.close()
        await self.start()

        run_config = self.crawler_config.clone() if self.crawler_config else CrawlerRunConfig()
        run_config = self._merge_crawler_configs(run_config, list_crawler_cfg)

        all_incremental: set[str] = set()
        per_list: list[dict[str, Any]] = []
        for list_url in start_urls:
            list_state_file = self._state_file_for_list_url(base_state, list_url)
            incremental_urls = await self.crawl_list_incremental(
                list_url=list_url,
                max_pages=max_pages_value,
                include_patterns=include_value,
                exclude_patterns=exclude_value,
                state_file_path=list_state_file,
                run_config=run_config.clone(),
                initialize=False,
            )
            all_incremental.update(incremental_urls)
            per_list.append(
                {
                    "list_url": list_url,
                    "incremental_count": len(incremental_urls),
                    "state_file": str(list_state_file),
                    "incremental_urls": incremental_urls,
                }
            )

        return {
            "website": website_name,
            "source": website_cfg.get("name"),
            "start_urls": start_urls,
            "lists": per_list,
            "incremental_urls": sorted(all_incremental),
            "article_overrides": {
                "crawler": article_crawler_cfg,
                "browser": browser_cfg_overrides,
            },
        }
