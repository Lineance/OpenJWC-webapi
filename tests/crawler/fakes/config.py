from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

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
