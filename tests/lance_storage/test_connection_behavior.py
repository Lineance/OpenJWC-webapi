from pathlib import Path

import pytest

from app.infrastructure.storage.lancedb import connection
from app.infrastructure.storage.lancedb.connection import (
    ARTICLES_TABLE_NAME,
    LanceDBConnection,
)


class _TablesWithAttr:
    def __init__(self, names):
        self.tables = names


class _FakeDB:
    def __init__(self, table_names=None, fail_list=False):
        self._table_names = table_names or []
        self._fail_list = fail_list
        self.drop_calls = []

    def list_tables(self):
        if self._fail_list:
            raise RuntimeError("list failed")
        return self._table_names

    def drop_table(self, name):
        self.drop_calls.append(name)


class _FakeConnForInit:
    def __init__(self):
        self.create_called = False
        self.index_called = False

    def create_articles_table(self, exist_ok=True):
        self.create_called = exist_ok

    def create_indices(self):
        self.index_called = True


@pytest.mark.unit
def test_get_connection_is_singleton_for_same_process(temp_db_path: str) -> None:
    LanceDBConnection.reset()
    first = connection.get_connection(temp_db_path)
    second = connection.get_connection(temp_db_path)
    assert first is second


@pytest.mark.unit
def test_table_names_supports_tables_attribute() -> None:
    fake_db = type(
        "DB", (), {"list_tables": lambda self: _TablesWithAttr(["a", "b"])}
    )()
    assert connection._table_names(fake_db) == ["a", "b"]


@pytest.mark.unit
def test_find_project_root_fallback_without_pyproject(tmp_path: Path) -> None:
    nested = tmp_path / "a" / "b" / "c" / "d" / "e" / "f"
    nested.mkdir(parents=True)

    root = connection._find_project_root(nested)

    assert root == nested.parents[4]


@pytest.mark.unit
def test_create_articles_table_raises_when_table_exists(initialized_db) -> None:
    with pytest.raises(ValueError, match="already exists"):
        initialized_db.create_articles_table(exist_ok=False)


@pytest.mark.unit
def test_init_database_creates_articles_table(temp_db_path: str) -> None:
    LanceDBConnection.reset()
    conn = connection.init_database(db_path=temp_db_path, create_indices=False)
    assert conn.table_exists(ARTICLES_TABLE_NAME) is True


@pytest.mark.unit
def test_table_exists_and_drop_table_remove_cache() -> None:
    conn = object.__new__(LanceDBConnection)
    conn._db = _FakeDB(table_names=[ARTICLES_TABLE_NAME])
    conn._tables = {ARTICLES_TABLE_NAME: object()}
    conn._table_lock = connection.threading.Lock()

    assert conn.table_exists(ARTICLES_TABLE_NAME) is True

    conn.drop_table(ARTICLES_TABLE_NAME)

    assert ARTICLES_TABLE_NAME not in conn._tables
    assert conn._db.drop_calls == [ARTICLES_TABLE_NAME]


@pytest.mark.unit
def test_health_check_unhealthy_when_list_tables_fails() -> None:
    conn = object.__new__(LanceDBConnection)
    conn._db = _FakeDB(fail_list=True)
    conn._db_path = "/tmp/fake.lance"

    result = conn.health_check()

    assert result["status"] == "unhealthy"
    assert "list failed" in result["error"]


@pytest.mark.unit
def test_health_check_without_articles_table() -> None:
    conn = object.__new__(LanceDBConnection)
    conn._db = _FakeDB(table_names=["other_table"])
    conn._db_path = "/tmp/fake.lance"

    result = conn.health_check()

    assert result["status"] == "healthy"
    assert result["articles_count"] == 0
    assert result["tables"] == ["other_table"]


@pytest.mark.unit
def test_get_articles_table_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Conn:
        def __init__(self):
            self.calls = []

        def get_table(self, name):
            self.calls.append(name)
            return {"name": name}

    fake_conn = _Conn()
    monkeypatch.setattr(connection, "get_connection", lambda db_path=None: fake_conn)

    table = connection.get_articles_table()

    assert fake_conn.calls == [ARTICLES_TABLE_NAME]
    assert table == {"name": ARTICLES_TABLE_NAME}


@pytest.mark.unit
def test_init_database_calls_create_indices_conditionally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_conn = _FakeConnForInit()
    monkeypatch.setattr(connection, "get_connection", lambda db_path=None: fake_conn)

    result_no_index = connection.init_database(create_indices=False)
    assert result_no_index is fake_conn
    assert fake_conn.create_called is True
    assert fake_conn.index_called is False

    fake_conn.index_called = False
    result_with_index = connection.init_database(create_indices=True)
    assert result_with_index is fake_conn
    assert fake_conn.index_called is True


@pytest.mark.unit
def test_reset_clears_singleton_and_allows_recreate(temp_db_path: str) -> None:
    LanceDBConnection.reset()
    first = connection.get_connection(temp_db_path)
    LanceDBConnection.reset()
    second = connection.get_connection(temp_db_path)
    assert first is not second
