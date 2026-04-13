import json
import sqlite3
from typing import Any

from app.domain.submission.models import SubmissionRecord, SubmissionStatus
from app.utils.logging_manager import setup_logger

logger = setup_logger("submission_repository_logs")


class SubmissionRepository:
    def __init__(self, db_service: Any):
        self._db = db_service

    def create(self, record: SubmissionRecord) -> bool:
        sql = """
            INSERT INTO submissions (id, label, title, date, detail_url, is_page, content_text, attachments, submitter_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self._db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        record.submission_id,
                        record.label,
                        record.title,
                        record.date,
                        record.detail_url,
                        record.is_page,
                        record.content_text,
                        json.dumps(record.attachment_urls, ensure_ascii=False),
                        record.submitter_id,
                    ),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                logger.warning(f"submission already exists: {record.submission_id}")
                return False

    def get_by_id(self, submission_id: str) -> SubmissionRecord | None:
        sql = "SELECT * FROM submissions WHERE id = ?"
        with self._db.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, (submission_id,))
            row = cursor.fetchone()
            if not row:
                return None
            row_dict = dict(row)
            attachments = row_dict.get("attachments") or "[]"
            if isinstance(attachments, str):
                attachments = json.loads(attachments)
            return SubmissionRecord(
                submission_id=str(row_dict["id"]),
                submitter_id=str(row_dict.get("submitter_id") or ""),
                label=str(row_dict.get("label") or ""),
                title=str(row_dict.get("title") or ""),
                date=str(row_dict.get("date") or ""),
                detail_url=row_dict.get("detail_url"),
                is_page=bool(row_dict.get("is_page")),
                content_text=str(row_dict.get("content_text") or ""),
                attachment_urls=list(attachments),
                status=SubmissionStatus(str(row_dict.get("status") or "pending")),
                review=str(row_dict.get("review") or ""),
            )

    def list_for_admin(
        self,
        limit: int = 20,
        offset: int = 0,
        status: str | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        with self._db.get_connection() as conn:
            count_query = "SELECT COUNT(*) FROM submissions"
            count_params: list[str] = []
            if status is not None:
                count_query += " WHERE status = ?"
                count_params.append(status)
            cursor = conn.cursor()
            cursor.execute(count_query, tuple(count_params))
            total_count = cursor.fetchone()[0]

            query = """
                SELECT id, label, title, date, detail_url, is_page, status
                FROM submissions
            """
            params: list[Any] = []
            if status is not None:
                query += " WHERE status = ?"
                params.append(status)
            query += " ORDER BY date DESC, id DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            results: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["is_page"] = bool(item.get("is_page"))
                item["id"] = str(item.get("id"))
                results.append(item)
            return int(total_count), results

    def list_by_submitter(self, submitter_id: str) -> list[dict[str, Any]]:
        sql = "SELECT * FROM submissions WHERE submitter_id = ?"
        with self._db.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, (submitter_id,))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                result.append(
                    {
                        "id": str(row["id"]),
                        "label": row["label"],
                        "title": row["title"],
                        "date": row["date"],
                        "detail_url": row["detail_url"],
                        "is_page": bool(row["is_page"]),
                        "status": row["status"],
                        "review": row["review"],
                    }
                )
            return result

    def update_status(self, submission_id: str, status: str, review: str = "") -> bool:
        sql = """
            UPDATE submissions
            SET status = ?, review = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """
        with self._db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (status, review, submission_id))
            conn.commit()
            return cursor.rowcount > 0
