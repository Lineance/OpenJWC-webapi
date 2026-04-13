from pathlib import Path

import pytest

from app.infrastructure.storage.lancedb import connection


@pytest.mark.unit
def test_resolve_db_path_keeps_absolute_path() -> None:
    absolute_path = str(Path("D:/tmp/custom_lancedb").resolve())
    assert connection._resolve_db_path(absolute_path) == absolute_path


@pytest.mark.unit
def test_resolve_db_path_uses_project_root_for_relative_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_root = Path("D:/repo/OpenJWC-webapi")
    monkeypatch.setattr(connection, "_find_project_root", lambda _start: fake_root)

    resolved = connection._resolve_db_path("data/lancedb")

    assert resolved == str((fake_root / "data/lancedb").resolve())
