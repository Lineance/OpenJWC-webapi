"""Repository 异常分层行为测试。"""

from datetime import datetime, timezone
from typing import Any

import pytest
from app.infrastructure.storage.lancedb.exceptions import RepositorySystemError
from app.infrastructure.storage.lancedb.repository import ArticleRepository
from app.infrastructure.storage.lancedb.tag_repository import TagRepository
from app.infrastructure.storage.lancedb.tag_schema import TagRecord
from tests.lance_storage.repository.fake_failures import (
    _FailingAddTable,
    _FailingSearchBuilder,
)
from tests.lance_storage.repository.fake_tables import (
    _FailingSearchTable,
    _FailingTagTable,
)

@pytest.mark.unit
def test_article_add_one_raises_system_error(sample_article_data: dict[str, Any]) -> None:
    repo = ArticleRepository(table=_FailingAddTable())

    sample_article_data["title_embedding"] = [0.1] * 384
    sample_article_data["content_embedding"] = [0.1] * 1024

    with pytest.raises(RepositorySystemError):
        repo.add_one(sample_article_data)

@pytest.mark.unit
def test_article_find_all_raises_system_error() -> None:
    repo = ArticleRepository(table=_FailingSearchTable())

    with pytest.raises(RepositorySystemError):
        repo.find_all(limit=10)

@pytest.mark.unit
def test_tag_add_one_raises_system_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(TagRepository, "_get_or_create_table", lambda self: _FailingTagTable())

    repo = TagRepository(connection=object())
    record = TagRecord(
        tag_id="tag_test_001",
        name="测试标签",
        description="测试描述",
        category="test",
        embedding=[0.1] * 1024,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    with pytest.raises(RepositorySystemError):
        repo.add_one(record)
