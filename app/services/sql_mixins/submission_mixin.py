import json
import sqlite3
from app.services.db_interface import DBInterface
from typing import List, Dict, Any, Tuple, Optional
from app.utils.logging_manager import setup_logger
from app.models.schemas import SubmissionRequest

logger = setup_logger("submission_db_logs")


class SubmissionMixin:
    def create_submission(
        self: DBInterface, submission: SubmissionRequest, submitter_id: str
    ) -> int | None:
        """
        客户端：提交一条新资讯
        submission为客户端传来的请求体
        submitter_id为客户端的apikey
        """
        sql = """
            INSERT INTO submissions (label, title, date, detail_url, is_page,  content_text, attachments, submitter_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                sql,
                (
                    submission.label,
                    submission.title,
                    submission.date,
                    submission.detail_url,
                    submission.is_page,
                    submission.content.text,
                    json.dumps(submission.content.attachment_urls, ensure_ascii=False),
                    submitter_id,
                ),
            )
            conn.commit()
            logger.info(f"收到新资讯提交, ID: {cursor.lastrowid}")
            return cursor.lastrowid

    def get_submissions_by_status(
        self: DBInterface, status: str = "pending"
    ) -> List[Dict[str, Any]]:
        """WebUI管理端：获取某状态的提交列表"""
        sql = "SELECT * FROM submissions WHERE status = ? ORDER BY created_at DESC"
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, (status,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_submissions_for_admin(
        self: DBInterface,
        limit: int = 20,
        offset: int = 0,
        status: Optional[str] = None,
    ) -> Tuple[int, List[dict]]:
        """按页数获取提交信息列表，供控制面板处理待审核提交。"""
        with self.get_connection() as conn:
            count_query = "SELECT COUNT(*) FROM submissions"
            count_params = []
            if status is not None:
                count_query += " WHERE status = ? "
                count_params.append(status)
            cursor = conn.cursor()
            cursor.execute(count_query, tuple(count_params))
            total_count = cursor.fetchone()[0]
            query = """
                SELECT id, label, title, date, detail_url, is_page, status 
                FROM submissions 
            """
            params = []
            if status is not None:
                query += " WHERE status = ? "
                params.append(status)
            query += " ORDER BY date DESC, id DESC LIMIT ? OFFSET ? "
            params.extend([limit, offset])

            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            results = []
            for row in rows:
                item = dict(row)
                item["is_page"] = bool(item["is_page"])
                results.append(item)
            logger.info(
                f"提交查询成功 (status: {status}, total: {total_count}, count: {len(results)})"
            )
            return total_count, results

    def update_submission_status(
        self: DBInterface, sub_id: int, status: str, review: str = ""
    ) -> bool:
        """WebUI管理端：更新审核状态"""
        sql = """
            UPDATE submissions 
            SET status = ?, review = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (status, review, sub_id))
            conn.commit()
            return cursor.rowcount > 0

    def get_submission_by_id(self: DBInterface, sub_id: str) -> Dict[str, Any] | None:
        """获取单个提交的详细信息"""
        sql = "SELECT * FROM submissions WHERE id = ?"
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, (sub_id,))
            row = cursor.fetchone()
            if row:
                item = dict(row)
                item["attachments"] = json.loads(item["attachments"])
                return item
            else:
                return None

    def get_submission_by_apikey(
        self: DBInterface, apikey: str
    ) -> Dict[str, Any] | None:
        """获取来自某个用户的提交"""
        sql = "SELECT * FROM submissions WHERE submitter_id = ?"
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, (apikey,))
            row = cursor.fetchone()
            return dict(row) if row else None
