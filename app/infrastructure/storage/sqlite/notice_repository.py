import json
from datetime import datetime
from typing import Any

from app.infrastructure.storage.lancedb.schema import ArticleFields
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.utils.logging_manager import setup_logger

logger = setup_logger("notice_repository_logs")


class NoticeRepository:
    """SQLite-backed notice repository used by notices APIs."""

    def __init__(self, db_service=db):
        self._db = db_service

    @staticmethod
    def _safe_date(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.date().isoformat()
        text = str(value)
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        return text

    @staticmethod
    def _extract_metadata(metadata: Any) -> dict[str, Any]:
        if isinstance(metadata, dict):
            return metadata
        if isinstance(metadata, str) and metadata:
            try:
                loaded = json.loads(metadata)
                if isinstance(loaded, dict):
                    return loaded
            except json.JSONDecodeError:
                return {}
        return {}

    @staticmethod
    def _extract_label(article: dict[str, Any]) -> str | None:
        tags = article.get(ArticleFields.TAGS)
        if isinstance(tags, list) and tags:
            first = tags[0]
            return str(first) if first is not None else None

        metadata = NoticeRepository._extract_metadata(
            article.get(ArticleFields.METADATA)
        )
        label = metadata.get("label")
        if label is not None:
            return str(label)

        source = article.get(ArticleFields.SOURCE_SITE)
        return str(source) if source else None

    @staticmethod
    def _to_notice_payload(article: dict[str, Any]) -> dict[str, Any]:
        metadata = NoticeRepository._extract_metadata(
            article.get(ArticleFields.METADATA)
        )
        attachments = article.get(ArticleFields.ATTACHMENTS)
        if not isinstance(attachments, list):
            attachments = []

        detail_url = metadata.get("detail_url") or article.get(ArticleFields.URL) or ""
        is_page = bool(metadata.get("is_page", True))

        return {
            "id": str(article.get(ArticleFields.NEWS_ID, "")),
            "label": NoticeRepository._extract_label(article),
            "title": str(article.get(ArticleFields.TITLE, "")),
            "date": NoticeRepository._safe_date(
                article.get(ArticleFields.PUBLISH_DATE)
            ),
            "detail_url": str(detail_url),
            "is_page": is_page,
            "content_text": str(article.get(ArticleFields.CONTENT_TEXT, "") or ""),
            "attachments": [str(item) for item in attachments],
        }

    @staticmethod
    def _row_to_notice_item(row: Any) -> dict[str, Any]:
        attachments_text = (
            row["attachments"] if row["attachments"] is not None else "[]"
        )
        try:
            attachments = json.loads(attachments_text)
            if not isinstance(attachments, list):
                attachments = []
        except json.JSONDecodeError:
            attachments = []

        return {
            "id": str(row["id"]),
            "label": row["label"],
            "title": row["title"],
            "date": row["date"] or "",
            "detail_url": row["detail_url"] or "",
            "is_page": bool(row["is_page"]),
            "content_text": row["content_text"] or "",
            "attachments": [str(item) for item in attachments],
        }

    def upsert_notice(self, notice: dict[str, Any]) -> bool:
        if not notice.get("id"):
            return False

        sql = """
        INSERT INTO notices (id, label, title, date, detail_url, is_page, content_text, attachments, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            label=excluded.label,
            title=excluded.title,
            date=excluded.date,
            detail_url=excluded.detail_url,
            is_page=excluded.is_page,
            content_text=excluded.content_text,
            attachments=excluded.attachments,
            updated_at=CURRENT_TIMESTAMP
        """

        try:
            with self._db.get_connection() as conn:
                conn.execute(
                    sql,
                    (
                        notice["id"],
                        notice.get("label"),
                        notice.get("title") or "",
                        notice.get("date") or "",
                        notice.get("detail_url") or "",
                        1 if notice.get("is_page", True) else 0,
                        notice.get("content_text") or "",
                        json.dumps(notice.get("attachments") or [], ensure_ascii=False),
                    ),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.warning(f"Upsert notice failed: {e}")
            return False

    def upsert_from_article(self, article: dict[str, Any]) -> bool:
        payload = self._to_notice_payload(article)
        return self.upsert_notice(payload)

    def upsert_many_from_articles(self, articles: list[dict[str, Any]]) -> int:
        if not articles:
            return 0

        success = 0
        for article in articles:
            if self.upsert_from_article(article):
                success += 1
        return success

    def list_for_notices(
        self,
        limit: int = 20,
        offset: int = 0,
        label: str | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        where_sql = ""
        params: list[Any] = []
        if label:
            where_sql = "WHERE label = ?"
            params.append(label)

        count_sql = f"SELECT COUNT(*) as c FROM notices {where_sql}"
        data_sql = (
            "SELECT id, label, title, date, detail_url, is_page, content_text, attachments "
            f"FROM notices {where_sql} "
            "ORDER BY date DESC, updated_at DESC, id DESC LIMIT ? OFFSET ?"
        )

        try:
            with self._db.get_connection() as conn:
                total = int(conn.execute(count_sql, tuple(params)).fetchone()["c"])
                rows = conn.execute(
                    data_sql, tuple(params + [limit, offset])
                ).fetchall()
            return total, [self._row_to_notice_item(row) for row in rows]
        except Exception as e:
            logger.error(f"List notices failed: {e}")
            return 0, []

    def get_notice_labels(self) -> list[str]:
        try:
            with self._db.get_connection() as conn:
                rows = conn.execute(
                    "SELECT DISTINCT label FROM notices WHERE label IS NOT NULL AND label != '' ORDER BY label"
                ).fetchall()
            return [str(row["label"]) for row in rows if row["label"]]
        except Exception as e:
            logger.error(f"Get notice labels failed: {e}")
            return []

    def get_notice_total_labels(self) -> int:
        return len(self.get_notice_labels())

    def get_notice_info(self, notice_id: str) -> dict[str, Any] | None:
        try:
            with self._db.get_connection() as conn:
                row = conn.execute(
                    "SELECT id, label, title, date, detail_url, is_page FROM notices WHERE id = ?",
                    (notice_id,),
                ).fetchone()
            if row is None:
                return None
            return {
                "id": str(row["id"]),
                "label": row["label"],
                "title": row["title"],
                "date": row["date"] or "",
                "detail_url": row["detail_url"] or "",
                "is_page": bool(row["is_page"]),
            }
        except Exception as e:
            logger.error(f"Get notice info failed: {e}")
            return None

    def delete_notice(self, notice_id: str) -> bool:
        try:
            with self._db.get_connection() as conn:
                cursor = conn.execute("DELETE FROM notices WHERE id = ?", (notice_id,))
                conn.commit()
            return int(cursor.rowcount or 0) > 0
        except Exception as e:
            logger.error(f"Delete notice failed: {e}")
            return False


_notice_repository: NoticeRepository | None = None


def get_notice_repository() -> NoticeRepository:
    global _notice_repository
    if _notice_repository is None:
        _notice_repository = NoticeRepository()
    return _notice_repository
