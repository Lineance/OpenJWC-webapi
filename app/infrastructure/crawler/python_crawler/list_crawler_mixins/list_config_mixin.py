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

class ListConfigMixin:
    """封装 ListIncrementalCrawler 的单一职责方法。"""

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("list_incremental_crawler")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler = logging.StreamHandler()
            handler.setFormatter(fmt)
            logger.addHandler(handler)
        return logger

    def _load_yaml_config(self, filepath: Path) -> dict[str, Any]:
        with open(filepath, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}

    def _repo_root(self) -> Path:
        return self.base_script_path.parents[2]

    def _resolve_path(self, path_value: str) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path.resolve()
        return (self._repo_root() / path).resolve()

    def load_website_config(self, website_name: str) -> dict[str, Any]:
        candidates = [
            self.config_dir / "websites" / f"{website_name}.yaml",
            self._repo_root() / "config" / "websites" / f"{website_name}.yaml",
        ]
        for config_path in candidates:
            if config_path.exists():
                return self._load_yaml_config(config_path)
        raise FileNotFoundError(
            f"Website config not found for {website_name}. searched={candidates}"
        )

    def _merge_crawler_configs(
        self,
        base_config: CrawlerRunConfig,
        overrides: dict[str, Any],
    ) -> CrawlerRunConfig:
        merged = base_config.clone()
        config_data = normalize_crawler_overrides(dict(overrides), self.logger)

        for key, value in config_data.items():
            if hasattr(merged, key):
                setattr(merged, key, value)

        return merged

    def _merge_browser_configs(
        self,
        base_config: BrowserConfig,
        overrides: dict[str, Any],
    ) -> BrowserConfig:
        merged = base_config.clone()
        for key, value in overrides.items():
            if hasattr(merged, key):
                setattr(merged, key, value)
        return merged

    def _create_crawler_config(self, config_data: dict[str, Any]) -> CrawlerRunConfig:
        data = normalize_crawler_overrides(dict(config_data), self.logger)

        return CrawlerRunConfig(**data)

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
