from __future__ import annotations
from typing import Any

class _PruningContentFilter:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

class _BM25ContentFilter:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
