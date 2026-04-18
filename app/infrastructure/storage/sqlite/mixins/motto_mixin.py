from typing import Tuple
from app.infrastructure.storage.sqlite.db_interface import DBInterface, logger
from app.application.motto.motto_service import get_daily_quote


class MottoMixin:
    def insert_motto_from_hitokoto(self: DBInterface, date_str: str) -> bool:
        quote = get_daily_quote(category="a")
        if not quote["success"]:
            logger.warning("Hitokoto接口调用失败")
            return False
        with self.get_connection() as conn:
            cursor = conn.cursor()
            insert_sql = """
                INSERT INTO mottos (date_str ,motto_content, motto_author)
                VALUES (?, ?, ?)
            """
            cursor.execute(
                insert_sql,
                (
                    date_str,
                    quote["text"],
                    quote["author"],
                ),
            )
            conn.commit()
            return True

    def replace_motto_from_hitokoto(self: DBInterface, date_str: str) -> bool:
        quote = get_daily_quote(category="a")
        if not quote["success"]:
            logger.warning("Hitokoto接口调用失败")
            return False
        with self.get_connection() as conn:
            cursor = conn.cursor()
            insert_sql = """
                REPLACE INTO mottos (date_str ,motto_content, motto_author)
                VALUES (?, ?, ?)
            """
            cursor.execute(
                insert_sql,
                (
                    date_str,
                    quote["text"],
                    quote["author"],
                ),
            )
            conn.commit()
            return True

    def get_today_motto(self: DBInterface, date_str: str) -> Tuple[bool, dict]:
        """按照日期获取motto"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                (
                    """
                    SELECT motto_content, motto_author 
                    FROM mottos
                    WHERE date_str = ?
                    """
                ),
                (date_str,),
            )
        row = cursor.fetchone()
        if row:
            return True, {"motto_content": row[0], "motto_author": row[1]}
        else:
            return False, {}

