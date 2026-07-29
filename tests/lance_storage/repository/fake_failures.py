from __future__ import annotations
from typing import Any

class _FailingAddTable:
    name = "articles"

    def add(self, _records: list[dict[str, Any]]) -> None:
        raise OSError("disk full")

class _FailingSearchBuilder:
    def where(self, _clause: str) -> "_FailingSearchBuilder":
        return self

    def limit(self, _limit: int) -> "_FailingSearchBuilder":
        return self

    def offset(self, _offset: int) -> "_FailingSearchBuilder":
        return self

    def to_list(self) -> list[dict[str, Any]]:
        raise PermissionError("permission denied")
