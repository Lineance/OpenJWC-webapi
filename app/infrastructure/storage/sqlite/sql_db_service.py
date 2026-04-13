import sqlite3
from app.core.config import NOTICE_DB, NOTICE_JSON
from app.infrastructure.storage.sqlite.db_interface import logger
from app.domain.sql_mixins.notice_mixin import NoticeMixin
from app.domain.sql_mixins.validation_mixin import ValidationMixin
from app.domain.sql_mixins.admin_mixin import AdminMixin
from app.domain.sql_mixins.device_mixin import DeviceMixin
from app.domain.sql_mixins.submission_mixin import SubmissionMixin
from app.domain.sql_mixins.motto_mixin import MottoMixin


class DBService(
    NoticeMixin, ValidationMixin, AdminMixin, DeviceMixin, SubmissionMixin, MottoMixin
):
    def __init__(self, db_path=NOTICE_DB):
        self.db_path = db_path
        self.init_db()

    def get_connection(self):
        """获取数据库连接（并且让返回的查询结果表现得像字典，非常方便）"""
        conn = sqlite3.connect(self.db_path)
        # 将行数据转化为字典，而不是粗糙的元组
        conn.row_factory = sqlite3.Row
        logger.debug("sql数据库连接成功")
        return conn

    def init_db(self):
        """初始化数据库表"""
        logger.info("正在尝试初始化sql数据库")
        create_notices_sql = """
        CREATE TABLE IF NOT EXISTS notices (
            id TEXT PRIMARY KEY,
            label TEXT,
            title TEXT,
            date TEXT,
            detail_url TEXT,
            is_page BOOLEAN,
            content_text TEXT,
            attachments TEXT,
            is_pushed BOOLEAN DEFAULT 0  -- 0代表未推送，1代表已推送
        )
        """
        create_keys_sql = """
        CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_string TEXT UNIQUE NOT NULL,
            owner_name TEXT,
            is_active BOOLEAN DEFAULT 1,
            max_devices INTEGER DEFAULT 3,
            bound_devices TEXT DEFAULT '[]',
            total_requests INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        create_admin_sql = """
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name TEXT UNIQUE NOT NULL,
            hashed_password TEXT NOT NULL
        )
        """
        create_system_sql = """
        CREATE TABLE IF NOT EXISTS system_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value TEXT
        )
        """
        create_motto_sql = """
        CREATE TABLE IF NOT EXISTS mottos (
            date_str TEXT PRIMARY KEY,
            motto_content TEXT,
            motto_author TEXT
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        create_index_sql = (
            "CREATE INDEX IF NOT EXISTS idx_key_string ON api_keys(key_string);"
        )
        create_submissions_sql = """
        CREATE TABLE IF NOT EXISTS submissions (
            id TEXT PRIMARY KEY,
            label TEXT,
            title TEXT NOT NULL,
            date TEXT,
            detail_url TEXT,
            is_page BOOLEAN,
            content_text TEXT NOT NULL,
            attachments TEXT,
            submitter_id TEXT,
            status TEXT DEFAULT 'pending',   -- 状态: pending, approved, rejected
            review TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(create_notices_sql)
            cursor.execute(create_keys_sql)
            cursor.execute(create_admin_sql)
            cursor.execute(create_index_sql)
            cursor.execute(create_system_sql)
            cursor.execute(create_motto_sql)
            cursor.execute(create_submissions_sql)
            conn.commit()
            logger.info("sql数据库初始化完成")


# 单例模式导出，方便全局其他地方使用同一个实例
db = DBService()


if __name__ == "__main__":
    db.init_db()

    db._sync_settings()
    result = db.sync_from_json(NOTICE_JSON)
    logger.info(f"同步完成: {result}")

