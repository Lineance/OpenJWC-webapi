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


class ListIncrementalCrawler:
    """Discover article links from list pages with incremental state persistence."""

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

    def _load_state(self, state_file_path: Path) -> set[str]:
        if not state_file_path.exists():
            return set()

        try:
            data = json.loads(state_file_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return set(data)
            return set()
        except json.JSONDecodeError:
            self.logger.warning(
                "State file is corrupted, rebuilding from empty: %s", state_file_path
            )
            return set()

    def _save_state(self, urls: set[str], state_file_path: Path) -> None:
        state_file_path.parent.mkdir(parents=True, exist_ok=True)
        state_file_path.write_text(
            json.dumps(sorted(urls), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _state_file_for_list_url(self, base_state_file: Path, list_url: str) -> Path:
        digest = md5(list_url.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]
        return base_state_file.with_name(f"{base_state_file.stem}_{digest}{base_state_file.suffix}")

    def _build_list_page_url(self, seed_url: str, page_num: int) -> str:
        if page_num <= 1:
            return seed_url

        # Typical pattern: list.htm -> list2.htm, list3.htm
        if seed_url.endswith("list.htm"):
            return seed_url.replace("list.htm", f"list{page_num}.htm")

        match = re.search(r"list(\d*)\.htm$", seed_url)
        if match:
            return re.sub(r"list\d*\.htm$", f"list{page_num}.htm", seed_url)

        return seed_url

    def _is_allowed(
        self, url: str, include_patterns: list[str], exclude_patterns: list[str]
    ) -> bool:
        if include_patterns and not any(re.search(pattern, url) for pattern in include_patterns):
            return False
        return not (
            exclude_patterns and any(re.search(pattern, url) for pattern in exclude_patterns)
        )

    def _normalize_link(self, href: str | None, domain: str) -> str | None:
        if not href:
            return None
        if href.startswith("javascript:"):
            return None

        if href.startswith("http://") or href.startswith("https://"):
            return href
        if href.startswith("/"):
            return f"https://{domain}{href}"

        return None

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

            # If no new links found on this page, assume pagination ended.
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
