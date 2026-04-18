from pathlib import Path

from app.infrastructure.storage.lancedb.schema import ArticleFields
from app.infrastructure.storage.sqlite.notice_repository import NoticeRepository
from app.infrastructure.storage.sqlite.sql_db_service import DBService


def test_notice_repository_upsert_and_list(tmp_path: Path) -> None:
    db_path = tmp_path / "notice_repo.db"
    db_service = DBService(db_path=db_path)
    repo = NoticeRepository(db_service=db_service)

    seeded_article = {
        ArticleFields.NEWS_ID: "notice-sqlite-001",
        ArticleFields.TITLE: "测试公告",
        ArticleFields.PUBLISH_DATE: "2026-04-18T09:00:00Z",
        ArticleFields.URL: "https://example.com/n1",
        ArticleFields.SOURCE_SITE: "教务",
        ArticleFields.TAGS: ["教务"],
        ArticleFields.CONTENT_TEXT: "这是公告正文",
        ArticleFields.METADATA: {
            "label": "教务",
            "detail_url": "https://example.com/n1",
            "is_page": True,
        },
        ArticleFields.ATTACHMENTS: ["https://example.com/a.pdf"],
    }

    assert repo.upsert_from_article(seeded_article) is True

    total, notices = repo.list_for_notices(limit=20, offset=0, label=None)
    assert total == 1
    assert len(notices) == 1
    assert notices[0]["id"] == "notice-sqlite-001"
    assert notices[0]["label"] == "教务"


def test_notice_repository_labels_and_delete(tmp_path: Path) -> None:
    db_path = tmp_path / "notice_repo2.db"
    db_service = DBService(db_path=db_path)
    repo = NoticeRepository(db_service=db_service)

    assert repo.upsert_notice(
        {
            "id": "notice-a",
            "label": "教务",
            "title": "A",
            "date": "2026-04-18",
            "detail_url": "https://example.com/a",
            "is_page": True,
            "content_text": "A content",
            "attachments": [],
        }
    )
    assert repo.upsert_notice(
        {
            "id": "notice-b",
            "label": "考试",
            "title": "B",
            "date": "2026-04-17",
            "detail_url": "https://example.com/b",
            "is_page": True,
            "content_text": "B content",
            "attachments": [],
        }
    )

    labels = repo.get_notice_labels()
    assert labels == ["教务", "考试"]
    assert repo.get_notice_total_labels() == 2
    assert repo.get_notice_info("notice-a") is not None

    assert repo.delete_notice("notice-a") is True
    total, notices = repo.list_for_notices(limit=20, offset=0, label=None)
    assert total == 1
    assert notices[0]["id"] == "notice-b"
