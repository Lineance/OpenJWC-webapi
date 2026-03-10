import sqlite3
import json
import os
from pathlib import Path
from typing import List, Dict, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT_DIR / "data"
NOTICE_DB = DATA_DIR / "jwc_notices.db"
NOTICE_JSON = DATA_DIR / "output.json"


class DBService:
    def __init__(self, db_path=NOTICE_DB):
        self.db_path = db_path
        self.init_db()

    def get_connection(self):
        """获取数据库连接（并且让返回的查询结果表现得像字典，非常方便）"""
        conn = sqlite3.connect(self.db_path)
        # 将行数据转化为字典，而不是粗糙的元组
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        """初始化数据库表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS notices (
            id TEXT PRIMARY KEY,
            label TEXT,
            title TEXT,
            date TEXT,
            detail_url TEXT,
            is_page BOOLEAN,
            content TEXT,
            is_pushed BOOLEAN DEFAULT 0  -- 0代表未推送，1代表已推送
        )
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()

    def sync_from_json(self, json_file_path: str) -> Dict[str, int]:
        """
        核心功能：从爬虫生成的 JSON 文件读取数据并同步到数据库中。
        """
        if not os.path.exists(json_file_path):
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
                    "SELECT id, content FROM notices WHERE id = ?", (item["id"],)
                )
                existing_record = cursor.fetchone()

                if not existing_record:
                    # 这是一条全新的通知
                    cursor.execute(
                        """
                        INSERT INTO notices (id, label, title, date, detail_url, is_page, content, is_pushed)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            item["id"],
                            item["label"],
                            item["title"],
                            item["date"],
                            item["detail_url"],
                            item["is_page"],
                            item.get("content"),
                            0,  # 新通知默认为未推送
                        ),
                    )
                    new_notices_count += 1
                else:
                    # 记录存在，但这可能是因为爬虫一开始抓不到正文(content为null)，
                    # 后来重新抓取时才拿到了正文，所以我们要支持"更新 content"
                    if item.get("content") and not existing_record["content"]:
                        cursor.execute(
                            "UPDATE notices SET content = ? WHERE id = ?",
                            (item["content"], item["id"]),
                        )
                        updated_notices_count += 1

            conn.commit()

        return {"new_added": new_notices_count, "updated": updated_notices_count}

    def get_notices_for_app(self, limit: int = 20, offset: int = 0) -> List[dict]:
        """供 FastAPI 路由调用的查询接口（给移动端的列表页面）"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # 按照日期倒序排列，拿最新的
            cursor.execute(
                """
                SELECT id, label, title, date, detail_url, is_page 
                FROM notices 
                ORDER BY date DESC, id DESC
                LIMIT ? OFFSET ?
            """,
                (limit, offset),
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_notice_content(self, notice_id: str) -> Optional[dict]:
        """供 LLM (大语言模型) 提取正文时使用"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT title, content, date FROM notices WHERE id = ?", (notice_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None


# 单例模式导出，方便全局其他地方使用同一个实例
db = DBService()
