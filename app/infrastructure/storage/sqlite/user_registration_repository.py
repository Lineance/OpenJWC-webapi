import sqlite3
from typing import Any

from app.domain.user_registration.models import UserRegistrationRecord, UserRegistrationStatus
from app.utils.logging_manager import setup_logger

logger = setup_logger("user_registration_repository_logs")


class UserRegistrationRepository:
    def __init__(self, db_service: Any):
        self._db = db_service

    def list_for_admin(
        self,
        limit: int = 20,
        offset: int = 0,
        status: str | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        with self._db.get_connection() as conn:
            count_query = "SELECT COUNT(*) FROM user_registrations"
            count_params: list[str] = []
            if status is not None:
                count_query += " WHERE status = ?"
                count_params.append(status)
            cursor = conn.cursor()
            cursor.execute(count_query, tuple(count_params))
            total_count = cursor.fetchone()[0]

            query = """
                SELECT id, username, email, status, created_at
                FROM user_registrations
            """
            params: list[Any] = []
            if status is not None:
                query += " WHERE status = ?"
                params.append(status)
            query += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            results: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["id"] = str(item.get("id"))
                results.append(item)
            return int(total_count), results

    def get_by_id(self, user_id: str) -> UserRegistrationRecord | None:
        sql = "SELECT id, username, email, status, created_at FROM user_registrations WHERE id = ?"
        with self._db.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            row = cursor.fetchone()
            if not row:
                return None
            row_dict = dict(row)
            return UserRegistrationRecord(
                id=int(row_dict["id"]),
                username=str(row_dict.get("username") or ""),
                email=str(row_dict.get("email") or ""),
                status=UserRegistrationStatus(str(row_dict.get("status") or "pending")),
                created_at=str(row_dict.get("created_at") or ""),
            )

    def create(self, username: str, email: str, password_hash: str) -> int:
        sql = """
            INSERT INTO user_registrations (username, email, password_hash)
            VALUES (?, ?, ?)
        """
        with self._db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (username, email, password_hash))
            conn.commit()
            return cursor.lastrowid

    def get_by_username(self, username: str) -> UserRegistrationRecord | None:
        sql = "SELECT id, username, email, status, created_at FROM user_registrations WHERE username = ?"
        with self._db.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, (username,))
            row = cursor.fetchone()
            if not row:
                return None
            row_dict = dict(row)
            return UserRegistrationRecord(
                id=int(row_dict["id"]),
                username=str(row_dict.get("username") or ""),
                email=str(row_dict.get("email") or ""),
                status=UserRegistrationStatus(str(row_dict.get("status") or "pending")),
                created_at=str(row_dict.get("created_at") or ""),
            )

    def get_by_email(self, email: str) -> UserRegistrationRecord | None:
        sql = "SELECT id, username, email, status, created_at FROM user_registrations WHERE email = ?"
        with self._db.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql, (email,))
            row = cursor.fetchone()
            if not row:
                return None
            row_dict = dict(row)
            return UserRegistrationRecord(
                id=int(row_dict["id"]),
                username=str(row_dict.get("username") or ""),
                email=str(row_dict.get("email") or ""),
                status=UserRegistrationStatus(str(row_dict.get("status") or "pending")),
                created_at=str(row_dict.get("created_at") or ""),
            )

    def get_password_hash(self, registration_id: str) -> str | None:
        sql = "SELECT password_hash FROM user_registrations WHERE id = ?"
        with self._db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (registration_id,))
            row = cursor.fetchone()
            return row[0] if row else None

    def delete(self, registration_id: str) -> bool:
        sql = "DELETE FROM user_registrations WHERE id = ?"
        with self._db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (registration_id,))
            conn.commit()
            return cursor.rowcount > 0

    def update_status(self, user_id: str, status: str, review: str = "") -> bool:
        sql = """
            UPDATE user_registrations
            SET status = ?, review = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """
        with self._db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (status, review, user_id))
            conn.commit()
            return cursor.rowcount > 0
