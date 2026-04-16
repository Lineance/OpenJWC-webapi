import sqlite3

from app.core.config import SQLITE_DB_PATH
from app.infrastructure.storage.sqlite.db_interface import logger
from app.infrastructure.storage.sqlite.mixins.admin_mixin import AdminMixin
from app.infrastructure.storage.sqlite.mixins.device_mixin import DeviceMixin
from app.infrastructure.storage.sqlite.mixins.motto_mixin import MottoMixin
from app.infrastructure.storage.sqlite.mixins.validation_mixin import ValidationMixin
from app.infrastructure.storage.sqlite.mixins.user_mixin import UserMixin

SQLITE_USER_STATE_TABLES = {
    "api_keys",
    "admin_users",
    "system_settings",
    "mottos",
    "submissions",
    "users",
    "user_devices",
    "user_registrations",
}


class DBService(ValidationMixin, AdminMixin, DeviceMixin, MottoMixin, UserMixin):
    def __init__(self, db_path=SQLITE_DB_PATH):
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
        """初始化 SQLite 用户态/系统态表。"""
        logger.info("正在初始化 SQLite 用户态数据库")
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
            motto_author TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        create_index_sql = (
            "CREATE INDEX IF NOT EXISTS idx_key_string ON api_keys(key_string);"
        )
        create_users_sql = """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        create_user_devices_sql = """
        CREATE TABLE IF NOT EXISTS user_devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            device_uuid TEXT NOT NULL,
            device_name TEXT NOT NULL,
            token_hash TEXT,
            last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            UNIQUE(user_id, device_uuid)
        )
        """
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
        create_user_registrations_sql = """
        CREATE TABLE IF NOT EXISTS user_registrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            status TEXT DEFAULT 'pending',   -- 状态: pending, approved, rejected
            review TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(create_keys_sql)
            cursor.execute(create_admin_sql)
            cursor.execute(create_index_sql)
            cursor.execute(create_system_sql)
            cursor.execute(create_motto_sql)
            cursor.execute(create_submissions_sql)
            cursor.execute(create_users_sql)
            cursor.execute(create_user_devices_sql)
            cursor.execute(create_user_registrations_sql)
            # user_devices 索引
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_devices_token_hash ON user_devices(token_hash);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_devices_user_device ON user_devices(user_id, device_uuid);"
            )
            conn.commit()
            logger.info("sql数据库初始化完成")

    def drop_table(self, table: str):
        """CLI兼容：删除允许的 SQLite 用户态表并重建结构。"""
        if table not in SQLITE_USER_STATE_TABLES:
            allowed = ", ".join(sorted(SQLITE_USER_STATE_TABLES))
            raise ValueError(f"不允许删除表: {table}. 允许的表: {allowed}")

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            logger.info(f"{table}表结构已删除。")
        self.init_db()


# 单例模式导出，方便全局其他地方使用同一个实例
db = DBService()


if __name__ == "__main__":
    db.init_db()
    db._sync_settings()
    logger.info("SQLite 用户态与系统设置初始化完成")
