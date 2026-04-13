from __future__ import annotations

import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast


class _CacheMode:
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    READ_ONLY = "READ_ONLY"
    WRITE_ONLY = "WRITE_ONLY"
    BYPASS = "BYPASS"


@dataclass
class _LLMConfig:
    _data: dict[str, Any] = field(default_factory=dict)

    def __init__(self, **kwargs: Any) -> None:
        object.__setattr__(self, "_data", dict(kwargs))

    def __getattr__(self, item: str) -> Any:
        return self._data.get(item)


class _PruningContentFilter:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


class _BM25ContentFilter:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


class _LLMContentFilter:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


class _DefaultMarkdownGenerator:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


@dataclass
class _BrowserConfig:
    _data: dict[str, Any] = field(default_factory=dict)

    def __init__(self, **kwargs: Any) -> None:
        object.__setattr__(self, "_data", dict(kwargs))

    def __getattr__(self, item: str) -> Any:
        return self._data.get(item)

    def __setattr__(self, key: str, value: Any) -> None:
        if key == "_data":
            object.__setattr__(self, key, value)
            return
        self._data[key] = value

    def clone(self) -> "_BrowserConfig":
        return _BrowserConfig(**self._data)


@dataclass
class _CrawlerRunConfig:
    _data: dict[str, Any] = field(default_factory=dict)

    def __init__(self, **kwargs: Any) -> None:
        object.__setattr__(self, "_data", dict(kwargs))

    def __getattr__(self, item: str) -> Any:
        return self._data.get(item)

    def __setattr__(self, key: str, value: Any) -> None:
        if key == "_data":
            object.__setattr__(self, key, value)
            return
        self._data[key] = value

    def clone(self) -> "_CrawlerRunConfig":
        return _CrawlerRunConfig(**self._data)


class _FakeResult:
    def __init__(self, success: bool, links: dict[str, list[str]] | None = None) -> None:
        self.success = success
        self.links = links or {"internal": []}


class _AsyncWebCrawler:
    results_by_url: dict[str, _FakeResult] = {}

    def __init__(self, config: Any = None, base_directory: Any = None) -> None:
        self.config = config
        self.base_directory = base_directory
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.started = False

    async def arun(self, url: str, config: Any = None) -> _FakeResult:
        return self.results_by_url.get(url, _FakeResult(False))


class _BFSDeepCrawlStrategy:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


def _install_fake_crawl4ai() -> None:
    crawl4ai = types.ModuleType("crawl4ai")
    crawl4ai_any = cast(Any, crawl4ai)
    crawl4ai_any.AsyncWebCrawler = _AsyncWebCrawler
    crawl4ai_any.BrowserConfig = _BrowserConfig
    crawl4ai_any.CacheMode = _CacheMode
    crawl4ai_any.CrawlerRunConfig = _CrawlerRunConfig
    crawl4ai_any.LLMConfig = _LLMConfig

    content_filter_strategy = types.ModuleType("crawl4ai.content_filter_strategy")
    content_filter_strategy_any = cast(Any, content_filter_strategy)
    content_filter_strategy_any.PruningContentFilter = _PruningContentFilter
    content_filter_strategy_any.BM25ContentFilter = _BM25ContentFilter
    content_filter_strategy_any.LLMContentFilter = _LLMContentFilter

    markdown_generation_strategy = types.ModuleType("crawl4ai.markdown_generation_strategy")
    markdown_generation_strategy_any = cast(Any, markdown_generation_strategy)
    markdown_generation_strategy_any.DefaultMarkdownGenerator = _DefaultMarkdownGenerator

    deep_crawling = types.ModuleType("crawl4ai.deep_crawling")
    bfs_strategy = types.ModuleType("crawl4ai.deep_crawling.bfs_strategy")
    bfs_strategy_any = cast(Any, bfs_strategy)
    bfs_strategy_any.BFSDeepCrawlStrategy = _BFSDeepCrawlStrategy

    sys.modules["crawl4ai"] = crawl4ai_any
    sys.modules["crawl4ai.deep_crawling"] = deep_crawling
    sys.modules["crawl4ai.deep_crawling.bfs_strategy"] = bfs_strategy_any
    sys.modules["crawl4ai.content_filter_strategy"] = content_filter_strategy_any
    sys.modules["crawl4ai.markdown_generation_strategy"] = markdown_generation_strategy_any


def pytest_addoption(parser: Any) -> None:
    # Option is now registered in tests/conftest.py to avoid duplication
    pass


def pytest_configure(config: Any) -> None:
    config.addinivalue_line(
        "markers", "real_web: marks tests that require real network and real crawl4ai"
    )

    use_real_crawl4ai = config.getoption("--run-real-web")
    if not use_real_crawl4ai:
        _install_fake_crawl4ai()
    repo_root = Path(__file__).resolve().parents[2]
    crawler_src = repo_root / "app" / "infrastructure" / "crawler" / "python" / "crawler"
    if str(crawler_src) not in sys.path:
        sys.path.insert(0, str(crawler_src))

    # Fix Windows console encoding issue for rich-based output
    import os
    if os.name == "nt":
        # Enable UTF-8 mode for Windows console
        os.environ["PYTHONIOENCODING"] = "utf-8"
        # Try to set the console mode to support UTF-8
        try:
            if sys.stdout is not None:
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            if sys.stderr is not None:
                sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def pytest_collection_modifyitems(config: Any, items: list[Any]) -> None:
    if config.getoption("--run-real-web"):
        return

    selected = []
    deselected = []
    for item in items:
        if item.get_closest_marker("real_web"):
            deselected.append(item)
        else:
            selected.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected


