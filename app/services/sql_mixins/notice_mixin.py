from typing import List, Dict, Optional, Tuple
from app.services.db_interface import DBInterface, logger
import json
import os


class NoticeMixin:
    def get_all_notices(self: DBInterface) -> list[dict]:
        """返回格式: [{'id': '...', 'title': '...', 'content_text': '...', 'date': '...'}]"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, title, content_text, date
                FROM notices
                ORDER BY date DESC, id DESC
                """
            )

            rows = cursor.fetchall()

            results = []
            for row in rows:
                results.append(
                    {
                        "id": row["id"],
                        "title": row["title"],
                        "content_text": row["content_text"],
                        "date": row["date"],
                    }
                )

            return results

    def get_total_notices(self: DBInterface) -> int:
        """获取当前数据库中所有资讯的总数"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM notices")
            result = cursor.fetchone()
            return result[0] if result else 0

    def drop_table(self: DBInterface, table: str):
        """直接删除表结构并重新创建。"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            logger.info(f"{table}表结构已删除。")
            self.init_db()  # 重新创建干净的表

    def sync_from_json(self: DBInterface, json_file_path: str) -> Dict[str, int]:
        """
        核心功能：从爬虫生成的 JSON 文件读取数据并同步到数据库中。
        """
        logger.warning("正在尝试从JSON文件同步数据库")
        if not os.path.exists(json_file_path):
            logger.error("JSON文件不存在")
            return {"error": "JSON 文件不存在"}

        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        new_notices_count = 0
        updated_notices_count = 0

        with self.get_connection() as conn:
            cursor = conn.cursor()

            for item in data:
                # 检查这个 ID 是否已经在数据库里了
                cursor.execute(
                    "SELECT id, content_text FROM notices WHERE id = ?", (item["id"],)
                )
                existing_record = cursor.fetchone()
                # 在循环内部
                content_data = item.get("content") or {}  # 如果是 null 则给空字典
                text = content_data.get("text")
                # 附件列表转为 JSON 字符串存储
                attachments = json.dumps(
                    content_data.get("attachment_urls", []), ensure_ascii=False
                )

                if not existing_record:
                    # 这是一条全新的通知
                    cursor.execute(
                        """
                        INSERT INTO notices (id, label, title, date, detail_url, is_page, content_text, attachments, is_pushed)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            item["id"],
                            item["label"],
                            item["title"],
                            item["date"],
                            item["detail_url"],
                            item["is_page"],
                            text,
                            attachments,
                            0,  # 新通知默认为未推送
                        ),
                    )
                    new_notices_count += 1
                    logger.info(f"sql数据库注册新通知：{item['id']}")
                else:
                    # 记录存在，但这可能是因为爬虫一开始抓不到正文(content为null)，
                    # 后来重新抓取时才拿到了正文，所以我们要支持"更新 content"
                    if text and not existing_record["content_text"]:
                        cursor.execute(
                            "UPDATE notices SET content_text = ?, attachments = ? WHERE id = ?",
                            (text, attachments, item["id"]),
                        )
                        updated_notices_count += 1
                        logger.info(f"sql数据库更新旧通知：{item['id']}")

            conn.commit()

        return {"new_added": new_notices_count, "updated": updated_notices_count}

    def get_notices_for_app(
        self: DBInterface, limit: int = 20, offset: int = 0, label: Optional[str] = None
    ) -> Tuple[int, List[dict]]:
        """供 FastAPI 路由调用的查询接口（给移动端的列表页面）"""
        with self.get_connection() as conn:
            count_query = "SELECT COUNT(*) FROM notices"
            count_params = []
            if label is not None:
                count_query += " WHERE label = ? "
                count_params.append(label)
            cursor = conn.cursor()
            cursor.execute(count_query, tuple(count_params))
            total_count = cursor.fetchone()[0]
            query = """
                SELECT id, label, title, date, detail_url, is_page, content_text, attachments 
                FROM notices 
            """
            params = []
            if label is not None:
                query += " WHERE label = ? "
                params.append(label)
            query += " ORDER BY date DESC, id DESC LIMIT ? OFFSET ? "
            params.extend([limit, offset])

            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            results = []
            for row in rows:
                item = dict(row)
                item["is_page"] = bool(item["is_page"])
                item["attachments"] = json.loads(item["attachments"])
                results.append(item)
            logger.info(
                f"资讯查询成功 (label: {label}, total: {total_count}, count: {len(results)})"
            )
            return total_count, results

    def get_notice_content(self: DBInterface, notice_id: str) -> Optional[dict]:
        """供 LLM (大语言模型) 提取正文时使用"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT title, content_text, date FROM notices WHERE id = ?",
                (notice_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_total_labels(self: DBInterface) -> int:
        """获取标签总数"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(DISTINCT label) FROM notices")
            result = cursor.fetchone()
            return result[0] if result else 0

    def get_labels(self: DBInterface) -> List[str]:
        """获取所有标签"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT label FROM notices")
            results = cursor.fetchall()
            return [row[0] for row in results] if results else []

    def delete_notice_by_id(self: DBInterface, notice_id: str) -> bool:
        """按ID删除通知"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # 先检查通知是否存在
            cursor.execute("SELECT id FROM notices WHERE id = ?", (notice_id,))
            if not cursor.fetchone():
                logger.warning(f"通知 [ID: {notice_id}] 不存在")
                return False

            cursor.execute("DELETE FROM notices WHERE id = ?", (notice_id,))
            conn.commit()
            logger.info(f"通知 [ID: {notice_id}] 已被删除")
            return True

    def insert_notice_from_dict(self: DBInterface, notice_data: dict) -> bool:
        """纯写入：将字典数据插入 notices 表"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # 检查是否已存在
            cursor.execute("SELECT id FROM notices WHERE id = ?", (notice_data["id"],))
            if cursor.fetchone():
                return False

            insert_sql = """
                INSERT INTO notices (id, label, title, date, detail_url, is_page, content_text, attachments, is_pushed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            attachments = json.dumps(
                notice_data.get("attachment_urls", []), ensure_ascii=False
            )
            cursor.execute(
                insert_sql,
                (
                    notice_data["id"],
                    notice_data.get("label"),
                    notice_data["title"],
                    notice_data["date"],
                    notice_data.get("detail_url"),
                    notice_data.get("is_page", 0),
                    notice_data["content_text"],
                    attachments,
                    0,
                ),
            )
            conn.commit()
            return True
