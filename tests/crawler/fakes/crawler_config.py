from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

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
