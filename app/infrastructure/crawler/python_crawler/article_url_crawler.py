import importlib
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling.bfs_strategy import BFSDeepCrawlStrategy

if __package__:
    _crawl4ai_config_utils = importlib.import_module(".crawl4ai_config_utils", __package__)
else:
    _crawl4ai_config_utils = importlib.import_module("crawl4ai_config_utils")

normalize_crawler_overrides = _crawl4ai_config_utils.normalize_crawler_overrides

from .article_crawler_mixins.article_config_mixin import ArticleConfigMixin
from .article_crawler_mixins.article_crawl_mixin import ArticleCrawlMixin
from .article_crawler_mixins.article_format_mixin import ArticleFormatMixin
from .article_crawler_mixins.article_metadata_mixin import ArticleMetadataMixin
from .article_crawler_mixins.article_content_mixin import ArticleContentMixin

class ArticleUrlCrawler(ArticleConfigMixin, ArticleCrawlMixin, ArticleFormatMixin, ArticleMetadataMixin, ArticleContentMixin):

    def __init__(
        self,
        config_dir: str | None = None,
        cache_base_directory: str | None = None,
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

        self.browser_config: BrowserConfig | None = None
        self.crawler_config: CrawlerRunConfig | None = None
        self._crawler_instance: AsyncWebCrawler | None = None

        self.logger = self._setup_logger()

    async def __aenter__(self) -> "ArticleUrlCrawler":
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
            self.logger.info("Crawl4AI article crawler started")

    async def close(self) -> None:
        if self._crawler_instance is not None:
            await self._crawler_instance.close()
            self._crawler_instance = None
            self.logger.info("Crawl4AI article crawler closed")
