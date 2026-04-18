import hashlib
from datetime import datetime, timezone
from typing import Optional, List

from app.core.security import get_password_hash, verify_password
from app.infrastructure.storage.sqlite.db_interface import DBInterface, logger


class UserMixin:
    """v2 账密鉴权相关的数据库操作"""

    # ==================== 用户账号 ====================

    def create_user_from_registration(
        self: DBInterface, username: str, email: str, password_hash: str
    ) -> int:
        """
        从注册审核通过创建正式用户。
        :return: 新用户的ID
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)",
                (username, email, password_hash),
            )
            conn.commit()
            logger.info(f"用户从注册审核通过创建成功: {username}")
            return cursor.lastrowid

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
                "SELECT id, username, email, password_hash, is_active FROM users WHERE username = ? OR email = ?",
                (account, account),
            )
            row = cursor.fetchone()
            if not row:
                return None
            if not row["is_active"]:
                return None
            if not verify_password(password_hash, row["password_hash"]):
                return None
            return {"id": row["id"], "username": row["username"], "email": row["email"]}

    def get_user_by_id(self: DBInterface, user_id: int) -> Optional[dict]:
        """根据用户ID查询用户，用于Token鉴权时验证用户是否真实存在"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, username, email, is_active FROM users WHERE id = ?",
                (user_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_user_by_username(self: DBInterface, username: str) -> Optional[dict]:
        """根据用户名查询用户"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, username, email, is_active FROM users WHERE username = ?",
                (username,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def set_user_active_status(self: DBInterface, user_id: int, is_active: bool) -> bool:
        """设置用户账号是否可用"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET is_active = ? WHERE id = ?",
                (1 if is_active else 0, user_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    def list_users_for_admin(
        self: DBInterface, offset: int = 0, limit: int = 20, is_active: bool | None = None
    ) -> tuple[int, list[dict]]:
        """获取用户列表（管理员视角）"""
        with self.get_connection() as conn:
            count_query = "SELECT COUNT(*) FROM users"
            count_params: list[Any] = []
            if is_active is not None:
                count_query += " WHERE is_active = ?"
                count_params.append(1 if is_active else 0)
            cursor = conn.cursor()
            cursor.execute(count_query, tuple(count_params))
            total_count = cursor.fetchone()[0]

            query = """
                SELECT id, username, email, is_active, created_at
                FROM users
            """
            params: list[Any] = []
            if is_active is not None:
                query += " WHERE is_active = ?"
                params.append(1 if is_active else 0)
            query += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            results: list[dict] = []
            for row in rows:
                item = dict(row)
                item["id"] = str(item.get("id"))
                item["is_active"] = bool(item.get("is_active"))
                results.append(item)
            return int(total_count), results

    def delete_user(self: DBInterface, user_id: int) -> bool:
        """删除用户账号（级联删除关联设备记录）"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM users WHERE id = ?", (user_id,))
            if not cursor.fetchone():
                logger.warning(f"删除用户失败：用户ID {user_id} 不存在")
                return False
            cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
            conn.commit()
            logger.info(f"用户ID {user_id} 删除成功")
            return True

    # ==================== 用户设备 ====================

    def upsert_user_device(
        self: DBInterface, user_id: int, device_uuid: str, device_name: str,
        token: str = None,
    ) -> None:
        """
        登录时记录/更新设备。
        如果设备已存在则更新 last_login、device_name 和 token_hash，否则插入新记录。
        token_hash 存储 Token 的 SHA256 哈希，用于鉴权时校验绑定关系。
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        t_hash = hashlib.sha256(token.encode()).hexdigest() if token else None
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO user_devices (user_id, device_uuid, device_name, token_hash, last_login)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id, device_uuid) DO UPDATE SET
                    device_name = excluded.device_name,
                    token_hash = excluded.token_hash,
                    last_login = excluded.last_login
                """,
                (user_id, device_uuid, device_name, t_hash, now),
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

    def check_device_token_binding(
        self: DBInterface, user_id: int, device_uuid: str, token: str
    ) -> bool:
        """
        校验设备-Token绑定关系。
        计算Token的SHA256哈希，与数据库中存储的token_hash比对。
        :return: True 表示绑定关系有效
        """
        t_hash = hashlib.sha256(token.encode()).hexdigest()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM user_devices WHERE user_id = ? AND device_uuid = ? AND token_hash = ?",
                (user_id, device_uuid, t_hash),
            )
            return cursor.fetchone() is not None

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
