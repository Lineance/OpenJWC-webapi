from typing import Dict, Optional
from app.services.db_interface import DBInterface, logger
from app.core.security import get_password_hash
from app.core.config import ALLOWED_SETTINGS, ADMIN_CONFIG_PATH
import os
import json


class AdminMixin:
    # 管理员鉴权
    def get_all_admins(self: DBInterface) -> list[dict]:
        """
        获取数据库中所有的管理员账号
        :return: 包含所有管理员信息的字典列表
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_name, hashed_password FROM admin_users")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def sync_admins_from_config(self: DBInterface) -> bool:
        """
        从配置文件单向同步管理员名单
        只保留配置中出现的管理员，配置中未出现的管理员账号会被删除
        :return: 同步成功返回True，配置文件不存在返回False
        """
        if not os.path.exists(ADMIN_CONFIG_PATH):
            return False
        logger.info("管理员初始化配置成功读取")
        with open(ADMIN_CONFIG_PATH, "r") as f:
            admin_lists = json.load(f)
        if len(admin_lists) == 0:
            logger.warning("未读取到任何有效管理员账号。")
            return False
        admin_usernames = []
        for admin_info in admin_lists:
            username = admin_info["username"]
            admin_usernames.append(username)
            password = admin_info["password"]
            if not self.get_admin_user(username):
                self.create_admin(username, password)
                logger.info(f"管理员{username}账号初始化成功，密码为{password}")

        existing_admins = self.get_all_admins()
        for admin in existing_admins:
            if admin["user_name"] not in admin_usernames:
                self.delete_admin(admin["user_name"])
                logger.info(f"管理员{username}账号删除成功")
            else:
                logger.info(f"管理员{username}账号已存在")

        return True

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
                logger.warning(f"管理员账号 [ID: {admin_name}] 不存在")
                return False

            cursor.execute("DELETE FROM admin_users WHERE user_name = ?", (admin_name,))
            conn.commit()
            logger.info(f"管理员账号 [ID: {admin_name}] 已被永久删除")
            return True

    def modify_password(self: DBInterface, admin_name: str, new_password: str) -> bool:
        """修改一个管理员的登录密码"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT user_name FROM admin_users WHERE user_name = ?", (admin_name,)
            )
            if not cursor.fetchone():
                logger.warning(f"管理员账号 [ID: {admin_name}] 不存在")
                return False

            hashed_password = get_password_hash(password=new_password)
            cursor.execute(
                "UPDATE admin_users SET hashed_password = ? WHERE user_name = ?",
                (hashed_password, admin_name),
            )
            conn.commit()
            logger.info(f"管理员账号 [ID: {admin_name}] 的密码已更新")
            return True

    # 系统设置
    def get_system_setting(self: DBInterface, setting_key: str) -> Optional[str]:
        """获取系统配置（如大模型Key、系统提示词、爬虫周期）"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT setting_value FROM system_settings WHERE setting_key = ?",
                (setting_key,),
            )
            row = cursor.fetchone()
            return row["setting_value"] if row else ALLOWED_SETTINGS[setting_key]

    def _sync_settings(
        self: DBInterface,
    ):
        """从ALLOWED_SETTINGS中同步所有允许的配置项到数据库"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT setting_key FROM system_settings")
            existing_keys = {row[0] for row in cursor.fetchall()}
            allowed_keys = set(ALLOWED_SETTINGS.keys())
            keys_to_delete = existing_keys - allowed_keys
            if keys_to_delete:
                placeholders = ",".join("?" for _ in keys_to_delete)
                cursor.execute(
                    f"DELETE FROM system_settings WHERE setting_key IN ({placeholders})",
                    tuple(keys_to_delete),
                )
                logger.info(f"已删除无效的系统配置: {keys_to_delete}")
            keys_to_insert = allowed_keys - existing_keys
            if keys_to_insert:
                insert_data = [(key, ALLOWED_SETTINGS[key]) for key in keys_to_insert]
                cursor.executemany(
                    "INSERT INTO system_settings (setting_key, setting_value) VALUES (?, ?)",
                    insert_data,
                )
                logger.info(
                    f"已注册并初始化新的系统配置: {[k for k in keys_to_insert]}"
                )

            conn.commit()
            logger.info("系统配置同步完成。")

    def get_all_settings(self: DBInterface) -> dict:
        """获取所有配置，供后台面板一次性展示"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT setting_key, setting_value FROM system_settings")
            rows = cursor.fetchall()
            return {
                "settings": [
                    {"key": row["setting_key"], "value": row["setting_value"]}
                    for row in rows
                ]
            }

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

    def reset_system_setting(self: DBInterface, setting_key: str) -> bool:
        """
        将指定设置重置为 ALLOWED_SETTINGS 中的默认值
        """
        if setting_key not in ALLOWED_SETTINGS:
            logger.error(f"尝试重置不存在的配置项: {setting_key}")
            return False

        default_value = ALLOWED_SETTINGS[setting_key]
        self.update_system_setting(setting_key, default_value)
        logger.info(f"系统配置已重置为默认值: {setting_key}")
        return True

    def reset_all_settings(self: DBInterface):
        """
        将所有设置重置为 ALLOWED_SETTINGS 中的默认值
        """
        self._sync_settings()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # 使用 executemany 进行批量更新，效率更高
            update_data = [(value, key) for key, value in ALLOWED_SETTINGS.items()]
            cursor.executemany(
                "UPDATE system_settings SET setting_value = ? WHERE setting_key = ?",
                update_data,
            )
            conn.commit()
            logger.info("所有系统配置已重置为默认值")
