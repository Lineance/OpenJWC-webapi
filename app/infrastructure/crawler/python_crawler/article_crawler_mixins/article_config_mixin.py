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

class ArticleConfigMixin:
    """封装 ArticleUrlCrawler 的单一职责方法。"""

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("article_url_crawler")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(fmt)

            console_handler.stream.reconfigure(encoding="utf-8", errors="replace")
            logger.addHandler(console_handler)
        return logger

    def _load_yaml_config(self, filepath: Path) -> dict[str, Any]:
        with open(filepath, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}

    def _create_crawler_config(self, config_data: dict[str, Any]) -> CrawlerRunConfig:
        config_data = normalize_crawler_overrides(dict(config_data), self.logger)

        return CrawlerRunConfig(**config_data)

    def _init_configs(self) -> None:
        browser_config_path = self.config_dir / "browser.yaml"
        crawler_config_path = self.config_dir / "crawler.yaml"

        if browser_config_path.exists():
            browser_data = self._load_yaml_config(browser_config_path)
            self.browser_config = BrowserConfig(**browser_data.get("browser", {}))
        else:
            self.browser_config = BrowserConfig()

        if crawler_config_path.exists():
            crawler_data = self._load_yaml_config(crawler_config_path)
            self.crawler_config = self._create_crawler_config(crawler_data.get("crawler", {}))
        else:
            self.crawler_config = CrawlerRunConfig()

    def load_website_config(self, website_name: str) -> dict[str, Any]:
        config_path = self.config_dir / "websites" / f"{website_name}.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"Website config not found: {config_path}")
        return self._load_yaml_config(config_path)

    def _merge_crawler_configs(
        self,
        base_config: CrawlerRunConfig,
        overrides: dict[str, Any],
    ) -> CrawlerRunConfig:
        merged_config = base_config.clone()
        overrides = normalize_crawler_overrides(dict(overrides), self.logger)
        deep_crawl_config = overrides.get("deep_crawl_strategy")

        for key, value in overrides.items():
            if key == "deep_crawl_strategy":
                continue
            if hasattr(merged_config, key):
                setattr(merged_config, key, value)

        if deep_crawl_config and isinstance(deep_crawl_config, dict):
            if deep_crawl_config.get("enabled", False):
                merged_config.deep_crawl_strategy = BFSDeepCrawlStrategy(
                    max_depth=deep_crawl_config.get("max_depth", 3),
                    max_pages=deep_crawl_config.get("max_pages", 100),
                    include_external=not deep_crawl_config.get("same_domain_only", True),
                )
            else:
                merged_config.deep_crawl_strategy = None

        return merged_config

    def load_config(
        self,
        target: str | list[str],
        is_website_config: bool = False,
        override_config: dict[str, Any] | None = None,
    ) -> tuple[list[str], CrawlerRunConfig, BrowserConfig]:
        self._init_configs()

        urls_to_crawl: list[str]
        run_config = self.crawler_config.clone() if self.crawler_config else CrawlerRunConfig()
        browser_config = self.browser_config.clone() if self.browser_config else BrowserConfig()

        if is_website_config:
            if not isinstance(target, str):
                raise TypeError("target must be website name str when is_website_config=True")
            website_cfg = self.load_website_config(target).get("website", {})
            urls_to_crawl = website_cfg.get("start_urls", [])
            overrides = website_cfg.get("overrides", {})

            article_overrides = overrides.get("article_crawler", overrides.get("crawler", {}))
            run_config = self._merge_crawler_configs(run_config, article_overrides)
            for key, value in overrides.get("browser", {}).items():
                if hasattr(browser_config, key):
                    setattr(browser_config, key, value)
        else:
            urls_to_crawl = [target] if isinstance(target, str) else target
            if override_config:
                run_config = self._merge_crawler_configs(
                    run_config, override_config.get("crawler", {})
                )
                for key, value in override_config.get("browser", {}).items():
                    if hasattr(browser_config, key):
                        setattr(browser_config, key, value)

        return urls_to_crawl, run_config, browser_config
