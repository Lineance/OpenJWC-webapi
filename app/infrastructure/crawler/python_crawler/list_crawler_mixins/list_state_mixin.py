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

class ListStateMixin:
    """封装 ListIncrementalCrawler 的单一职责方法。"""

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
