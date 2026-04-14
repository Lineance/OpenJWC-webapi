import json
from datetime import datetime, timezone
from typing import Optional, List

from app.core.security import get_password_hash, verify_password
from app.infrastructure.storage.sqlite.db_interface import DBInterface, logger


class UserMixin:
    """v2 账密鉴权相关的数据库操作"""

    # ==================== 用户账号 ====================

    def create_user(self: DBInterface, username: str, email: str, password_hash: str) -> bool:
        """
        注册新用户。
        password_hash 是客户端传来的 SHA256，服务端再做一次 bcrypt 后存储。
        :return: True 表示成功
        :raises ValueError: 用户名或邮箱已存在
        """
        hashed = get_password_hash(password_hash)
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # 检查用户名是否已存在
            cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
            if cursor.fetchone():
                raise ValueError("用户名已存在")
            # 检查邮箱是否已存在
            cursor.execute("SELECT id FROM users WHERE email = ?", (email,))
            if cursor.fetchone():
                raise ValueError("邮箱已被注册")
            cursor.execute(
                "INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)",
                (username, email, hashed),
            )
            conn.commit()
            logger.info(f"新用户注册成功: {username}")
            return True

    def authenticate_user(
        self: DBInterface, account: str, password_hash: str
    ) -> Optional[dict]:
        """
        验证用户登录。account 可以是用户名或邮箱。
        password_hash 是客户端传来的 SHA256。
        :return: 匹配的用户字典 {id, username, email}，验证失败返回 None
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, username, email, password_hash FROM users WHERE username = ? OR email = ?",
                (account, account),
            )
            row = cursor.fetchone()
            if not row:
                return None
            if not verify_password(password_hash, row["password_hash"]):
                return None
            return {"id": row["id"], "username": row["username"], "email": row["email"]}

    def get_user_by_username(self: DBInterface, username: str) -> Optional[dict]:
        """根据用户名查询用户"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, username, email FROM users WHERE username = ?",
                (username,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    # ==================== 用户设备 ====================

    def upsert_user_device(
        self: DBInterface, user_id: int, device_uuid: str, device_name: str
    ) -> None:
        """
        登录时记录/更新设备。
        如果设备已存在则更新 last_login 和 device_name，否则插入新记录。
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO user_devices (user_id, device_uuid, device_name, last_login)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, device_uuid) DO UPDATE SET
                    device_name = excluded.device_name,
                    last_login = excluded.last_login
                """,
                (user_id, device_uuid, device_name, now),
            )
            conn.commit()
            logger.info(f"用户[{user_id}]设备[{device_uuid[:8]}...]登录记录已更新")

    def get_user_devices(self: DBInterface, user_id: int) -> List[dict]:
        """获取用户的所有已登录设备"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT device_uuid, device_name, last_login FROM user_devices WHERE user_id = ? ORDER BY last_login DESC",
                (user_id,),
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def unbind_user_device(self: DBInterface, user_id: int, device_uuid: str) -> bool:
        """
        解绑用户的指定设备。
        :return: True 表示成功解绑，False 表示设备不存在
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM user_devices WHERE user_id = ? AND device_uuid = ?",
                (user_id, device_uuid),
            )
            if not cursor.fetchone():
                logger.warning(f"用户[{user_id}]设备[{device_uuid[:8]}...]解绑失败：设备不存在")
                return False
            cursor.execute(
                "DELETE FROM user_devices WHERE user_id = ? AND device_uuid = ?",
                (user_id, device_uuid),
            )
            conn.commit()
            logger.info(f"用户[{user_id}]设备[{device_uuid[:8]}...]解绑成功")
            return True
