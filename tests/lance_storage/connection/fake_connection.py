from __future__ import annotations
from typing import Any

class _FakeConnForInit:
    def __init__(self) -> None:
        self.create_called = False
        self.index_called = False

    def create_articles_table(self, exist_ok: Any=True) -> Any:
        self.create_called = exist_ok

    def create_indices(self) -> Any:
        self.index_called = True
