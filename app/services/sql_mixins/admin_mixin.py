from typing import Dict, Optional
from app.services.db_interface import DBInterface, logger
from app.core.security import get_password_hash


class AdminMixin:
    # 管理员鉴权
    def get_admin_user(self: DBInterface, username: str) -> Optional[dict]:
        """
        供登录接口验证账号密码使用
        接受一个用户名，在sql数据库的admin_users表中查询该用户是否存在
        :param username: 用户名
        :return: 如果存在，返回一个字典，包含user_name和hashed_password；如果不存在，返回None
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT user_name, hashed_password FROM admin_users WHERE user_name = ?",
                (username,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def create_admin(self: DBInterface, username: str, password: str) -> str:
        """超管方法：创建管理员账号"""
        hashed_password = get_password_hash(password=password)
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO admin_users (user_name, hashed_password) 
                VALUES (?, ?)
                """,
                (username, hashed_password),
            )
            conn.commit()
            logger.info(f"创建了新的管理员账号：{username}")
            return "管理员账号创建成功"

    def delete_admin(self: DBInterface, admin_name: str) -> bool:
        """超管方法：删除一个管理员账号"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # 先检查存不存在
            cursor.execute(
                "SELECT user_name FROM admin_users WHERE user_name = ?", (admin_name,)
            )
            if not cursor.fetchone():
                return False

            cursor.execute("DELETE FROM admin_users WHERE user_name = ?", (admin_name,))
            conn.commit()
            logger.info(f"管理员账号 [ID: {admin_name}] 已被永久删除")
            return True

    # 系统设置
    def get_system_setting(
        self: DBInterface, setting_key: str, default_value: str = None
    ) -> Optional[str]:
        """获取系统配置（如大模型Key、系统提示词、爬虫周期）"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT setting_value FROM system_settings WHERE setting_key = ?",
                (setting_key,),
            )
            row = cursor.fetchone()
            return row["setting_value"] if row else default_value

    def get_all_settings(self: DBInterface) -> Dict[str, str]:
        """获取所有配置，供后台面板一次性展示"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT setting_key, setting_value FROM system_settings")
            rows = cursor.fetchall()
            return {row["setting_key"]: row["setting_value"] for row in rows}

    def update_system_setting(self: DBInterface, setting_key: str, setting_value: str):
        """
        更新或插入系统配置。
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO system_settings (setting_key, setting_value)
                VALUES (?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET setting_value = excluded.setting_value
                """,
                (setting_key, setting_value),
            )
            conn.commit()
            logger.info(f"系统配置已更新: {setting_key}")

    # dashboard
    def get_dashboard_stats(self: DBInterface) -> dict:
        """获取首页概览数据"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM notices")
            total_notices = cursor.fetchone()["count"]
            cursor.execute("SELECT COUNT(*) as count FROM api_keys WHERE is_active = 1")
            active_keys = cursor.fetchone()["count"]

            return {
                "total_notices": total_notices,
                "active_api_keys": active_keys,
            }
