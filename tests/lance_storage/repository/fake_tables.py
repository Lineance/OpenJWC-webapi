from __future__ import annotations
from typing import Any
from tests.lance_storage.repository.fake_failures import _FailingSearchBuilder

class _FailingSearchTable:
    name = "articles"

    def search(self, *args: Any, **kwargs: Any) -> _FailingSearchBuilder:
        return _FailingSearchBuilder()

class _FailingTagTable:
    name = "tags"

    def add(self, _records: list[dict[str, Any]]) -> None:
        raise OSError("device unavailable")
