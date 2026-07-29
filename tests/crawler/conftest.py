from __future__ import annotations

import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast
from tests.crawler.fakes.config import (
    _CacheMode,
    _LLMConfig,
)
from tests.crawler.fakes.filters import (
    _PruningContentFilter,
    _BM25ContentFilter,
)
from tests.crawler.fakes.markdown import (
    _LLMContentFilter,
    _DefaultMarkdownGenerator,
)
from tests.crawler.fakes.crawler_config import (
    _BrowserConfig,
    _CrawlerRunConfig,
)
from tests.crawler.fakes.result import (
    _FakeResult,
)
from tests.crawler.fakes.crawler import (
    _AsyncWebCrawler,
    _BFSDeepCrawlStrategy,
)

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

    import os
    if os.name == "nt":

        os.environ["PYTHONIOENCODING"] = "utf-8"

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
