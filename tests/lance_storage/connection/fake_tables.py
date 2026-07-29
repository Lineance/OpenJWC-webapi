from __future__ import annotations
from typing import Any

class _TablesWithAttr:
    def __init__(self, names: Any) -> None:
        self.tables = names

class _FakeDB:
    def __init__(self, table_names: Any=None, fail_list: Any=False) -> None:
        self._table_names = table_names or []
        self._fail_list = fail_list
        self.drop_calls = []

    def list_tables(self) -> Any:
        if self._fail_list:
            raise RuntimeError("list failed")
        return self._table_names

    def drop_table(self, name: Any) -> Any:
        self.drop_calls.append(name)
