from pathlib import Path

from app.infrastructure.storage.lancedb.schema import ArticleFields
from app.infrastructure.storage.sqlite import notice_repository as notice_repo_module
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


def test_notice_repository_article_projection_behaviors(tmp_path: Path) -> None:
    db_path = tmp_path / "notice_repo_projection.db"
    db_service = DBService(db_path=db_path)
    repo = NoticeRepository(db_service=db_service)

    # Requirement: label/detail_url/is_page/date/attachments should be projected correctly
    # even when source article metadata quality varies.
    assert repo.upsert_from_article(
        {
            ArticleFields.NEWS_ID: "proj-1",
            ArticleFields.TITLE: "标题1",
            ArticleFields.PUBLISH_DATE: "2026-04-18T00:00:00Z",
            ArticleFields.URL: "https://example.com/default",
            ArticleFields.SOURCE_SITE: "教务处",
            ArticleFields.TAGS: [],
            ArticleFields.CONTENT_TEXT: "正文1",
            ArticleFields.METADATA: '{"label":"考试","detail_url":"https://example.com/detail","is_page":false}',
            ArticleFields.ATTACHMENTS: ["https://example.com/a.pdf"],
        }
    )

    assert repo.upsert_from_article(
        {
            ArticleFields.NEWS_ID: "proj-2",
            ArticleFields.TITLE: "标题2",
            ArticleFields.PUBLISH_DATE: None,
            ArticleFields.URL: "https://example.com/fallback",
            ArticleFields.SOURCE_SITE: "学工处",
            ArticleFields.TAGS: [],
            ArticleFields.CONTENT_TEXT: "正文2",
            ArticleFields.METADATA: "{bad-json",
            ArticleFields.ATTACHMENTS: "not-a-list",
        }
    )

    total, notices = repo.list_for_notices(limit=10, offset=0)
    assert total == 2
    by_id = {item["id"]: item for item in notices}

    assert by_id["proj-1"]["label"] == "考试"
    assert by_id["proj-1"]["detail_url"] == "https://example.com/detail"
    assert by_id["proj-1"]["is_page"] is False
    assert by_id["proj-1"]["date"] == "2026-04-18"
    assert by_id["proj-1"]["attachments"] == ["https://example.com/a.pdf"]

    assert by_id["proj-2"]["label"] == "学工处"
    assert by_id["proj-2"]["detail_url"] == "https://example.com/fallback"
    assert by_id["proj-2"]["is_page"] is True
    assert by_id["proj-2"]["date"] == ""
    assert by_id["proj-2"]["attachments"] == []


def test_notice_repository_article_projection_rejects_missing_news_id(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "notice_repo_invalid_projection.db"
    db_service = DBService(db_path=db_path)
    repo = NoticeRepository(db_service=db_service)

    assert (
        repo.upsert_from_article(
            {
                ArticleFields.TITLE: "无 id",
                ArticleFields.URL: "https://example.com/invalid",
                ArticleFields.CONTENT_TEXT: "x",
            }
        )
        is False
    )
    assert repo.list_for_notices(limit=10, offset=0)[0] == 0


def test_notice_repository_article_projection_rejects_invalid_news_id_values(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "notice_repo_invalid_id_values.db"
    db_service = DBService(db_path=db_path)
    repo = NoticeRepository(db_service=db_service)

    invalid_ids = [None, "", "   ", "None"]
    for i, invalid_id in enumerate(invalid_ids, start=1):
        ok = repo.upsert_from_article(
            {
                ArticleFields.NEWS_ID: invalid_id,
                ArticleFields.TITLE: f"无效ID-{i}",
                ArticleFields.URL: "https://example.com/invalid-id",
                ArticleFields.CONTENT_TEXT: "x",
            }
        )
        assert ok is False

    total, items = repo.list_for_notices(limit=20, offset=0)
    assert total == 0
    assert items == []


def test_notice_repository_label_fallback_when_first_tag_is_blank(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "notice_repo_label_fallback.db"
    db_service = DBService(db_path=db_path)
    repo = NoticeRepository(db_service=db_service)

    # Requirement: blank first tag should not suppress fallback to metadata label.
    assert repo.upsert_from_article(
        {
            ArticleFields.NEWS_ID: "label-fallback-1",
            ArticleFields.TITLE: "标签回退测试",
            ArticleFields.PUBLISH_DATE: "2026-04-18",
            ArticleFields.URL: "https://example.com/label",
            ArticleFields.SOURCE_SITE: "教务处",
            ArticleFields.TAGS: [""],
            ArticleFields.CONTENT_TEXT: "正文",
            ArticleFields.METADATA: '{"label":"考试"}',
            ArticleFields.ATTACHMENTS: [],
        }
    )

    info = repo.get_notice_info("label-fallback-1")
    assert info is not None
    assert info["label"] == "考试"


def test_notice_repository_filter_bulk_and_not_found(tmp_path: Path) -> None:
    db_path = tmp_path / "notice_repo3.db"
    db_service = DBService(db_path=db_path)
    repo = NoticeRepository(db_service=db_service)

    assert repo.upsert_notice({"title": "missing-id"}) is False
    assert repo.upsert_many_from_articles([]) == 0

    articles = [
        {
            ArticleFields.NEWS_ID: "bulk-1",
            ArticleFields.TITLE: "A",
            ArticleFields.PUBLISH_DATE: "2026-04-19",
            ArticleFields.URL: "https://example.com/a",
            ArticleFields.TAGS: ["教务"],
            ArticleFields.CONTENT_TEXT: "A",
            ArticleFields.METADATA: "{}",
            ArticleFields.ATTACHMENTS: [],
        },
        {
            ArticleFields.NEWS_ID: "bulk-2",
            ArticleFields.TITLE: "B",
            ArticleFields.PUBLISH_DATE: "2026-04-18",
            ArticleFields.URL: "https://example.com/b",
            ArticleFields.TAGS: ["考试"],
            ArticleFields.CONTENT_TEXT: "B",
            ArticleFields.METADATA: "{}",
            ArticleFields.ATTACHMENTS: [],
        },
    ]
    assert repo.upsert_many_from_articles(articles) == 2

    total_all, _ = repo.list_for_notices(limit=10, offset=0)
    total_label, filtered = repo.list_for_notices(limit=10, offset=0, label="考试")
    assert total_all == 2
    assert total_label == 1
    assert filtered[0]["id"] == "bulk-2"

    assert repo.get_notice_info("not-exist") is None
    assert repo.delete_notice("not-exist") is False


def test_notice_repository_error_paths() -> None:
    class BrokenDBService:
        def get_connection(self):
            raise RuntimeError("db down")

    repo = NoticeRepository(db_service=BrokenDBService())

    assert repo.upsert_notice({"id": "n"}) is False
    assert repo.list_for_notices() == (0, [])
    assert repo.get_notice_labels() == []
    assert repo.get_notice_total_labels() == 0
    assert repo.get_notice_info("n") is None
    assert repo.delete_notice("n") is False


def test_get_notice_repository_singleton() -> None:
    old_repo = notice_repo_module._notice_repository
    notice_repo_module._notice_repository = None
    try:
        first = notice_repo_module.get_notice_repository()
        second = notice_repo_module.get_notice_repository()
        assert first is second
        assert isinstance(first, NoticeRepository)
    finally:
        notice_repo_module._notice_repository = old_repo
