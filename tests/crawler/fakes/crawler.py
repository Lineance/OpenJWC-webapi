from __future__ import annotations
from typing import Any
from tests.crawler.fakes.result import _FakeResult

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
