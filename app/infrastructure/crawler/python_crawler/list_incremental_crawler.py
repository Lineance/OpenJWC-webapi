import importlib
import json
import logging
import re
from hashlib import md5
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

if __package__:
    _crawl4ai_config_utils = importlib.import_module(".crawl4ai_config_utils", __package__)
else:
    _crawl4ai_config_utils = importlib.import_module("crawl4ai_config_utils")

normalize_crawler_overrides = _crawl4ai_config_utils.normalize_crawler_overrides

from .list_crawler_mixins.list_config_mixin import ListConfigMixin
from .list_crawler_mixins.list_state_mixin import ListStateMixin
from .list_crawler_mixins.list_page_crawl_mixin import ListPageCrawlMixin
from .list_crawler_mixins.list_website_crawl_mixin import ListWebsiteCrawlMixin

class ListIncrementalCrawler(ListConfigMixin, ListStateMixin, ListPageCrawlMixin, ListWebsiteCrawlMixin):

    def __init__(
        self,
        config_dir: str | None = None,
        cache_base_directory: str | None = None,
        state_file: str | None = None,
    ) -> None:
        self.base_script_path = Path(__file__).resolve().parent
        if config_dir is None:
            self.config_dir = self.base_script_path.parent / "config_data"
        else:
            self.config_dir = Path(config_dir).resolve()

        self.cache_base_directory = (
            Path(cache_base_directory).resolve()
            if cache_base_directory
            else self.base_script_path.parent / "tmp" / "crawl4ai_cache"
        )
        self.cache_base_directory.mkdir(parents=True, exist_ok=True)

        self.state_file = (
            Path(state_file).resolve()
            if state_file
            else self.base_script_path.parent / "tmp" / "list_seen_urls.json"
        )
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

        self.browser_config: BrowserConfig | None = None
        self.crawler_config: CrawlerRunConfig | None = None
        self._crawler_instance: AsyncWebCrawler | None = None
        self.logger = self._setup_logger()

    async def __aenter__(self) -> "ListIncrementalCrawler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def start(self) -> None:
        if self._crawler_instance is None:
            self._crawler_instance = AsyncWebCrawler(
                config=self.browser_config,
                base_directory=str(self.cache_base_directory),
            )
            await self._crawler_instance.start()

    async def close(self) -> None:
        if self._crawler_instance:
            await self._crawler_instance.close()
            self._crawler_instance = None
