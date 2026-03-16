import json
import secrets
from app.services.db_interface import DBInterface
from typing import Dict, List, Optional
from app.utils.logging_manager import setup_logger

logger = setup_logger("validation_logs")


class ValidationMixin:
    def validate_and_use_key(
        self: DBInterface, key_string: str, device_id: str
    ) -> tuple[bool, str]:
        """
        核心鉴权逻辑：检查 Key 的有效性，校验设备指纹，并增加请求计数。
        返回值: (是否通过鉴权, 提示信息)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, is_active, max_devices, bound_devices FROM api_keys WHERE key_string = ?",
                (key_string,),
            )
            row = cursor.fetchone()
            if not row:
                return False, "该API Key不存在"

            if not row["is_active"]:
                return False, "该API Key已停用"

            bound_devices = json.loads(row["bound_devices"])
            if device_id not in bound_devices:
                if len(bound_devices) >= row["max_devices"]:
                    return (
                        False,
                        f"绑定设备达到上限：{row['max_devices']}",
                    )

                # 未超限，绑定新设备
                bound_devices.append(device_id)
                new_devices_json = json.dumps(bound_devices)
                cursor.execute(
                    "UPDATE api_keys SET bound_devices = ? WHERE id = ?",
                    (new_devices_json, row["id"]),
                )
                logger.info(f"API Key [{row['id']}] 绑定了新设备: {device_id}")

            # 鉴权通过，增加请求计数
            cursor.execute(
                "UPDATE api_keys SET total_requests = total_requests + 1 WHERE id = ?",
                (row["id"],),
            )
            conn.commit()

            return True, "Success"

    def validate_key_and_device(
        self: DBInterface, key_string: str, device_id: str
    ) -> tuple[bool, str]:
        """
        核心鉴权逻辑：检查 Key 的有效性，校验设备指纹，并增加请求计数。
        返回值: (是否通过鉴权, 提示信息)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, is_active, max_devices, bound_devices FROM api_keys WHERE key_string = ?",
                (key_string,),
            )
            row = cursor.fetchone()
            if not row:
                return False, "该API Key不存在"

            if not row["is_active"]:
                return False, "该API Key已停用"

            bound_devices = json.loads(row["bound_devices"])
            if device_id not in bound_devices:
                return False, "设备不存在"

            return True, "API Key有效且设备存在"

    def create_api_key(self: DBInterface, owner_name: str, max_devices: int = 3) -> str:
        """管理员接口：生成一个新的 API Key"""
        # 生成类似 sk-xxxxxx 的随机字符串
        new_key = f"sk-{secrets.token_hex(16)}"

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO api_keys (key_string, owner_name, max_devices) 
                VALUES (?, ?, ?)
                """,
                (new_key, owner_name, max_devices),
            )
            conn.commit()
            logger.info(f"生成了新的 API Key: {owner_name}")
            return new_key

    def get_all_api_keys(self: DBInterface) -> List[dict]:
        """管理员接口：获取所有用户状态供前端面板展示"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM api_keys ORDER BY created_at DESC")
            rows = cursor.fetchall()

            results = []
            for row in rows:
                r_dict = dict(row)
                r_dict["bound_devices"] = json.loads(
                    r_dict["bound_devices"]
                )  # 把 JSON 字符串转回列表给前端
                results.append(r_dict)
            return results

    def get_target_api_keys(
        self: DBInterface, page: int = 1, size: int = 20, keyword: Optional[str] = None
    ) -> Dict:
        """
        管理员接口：获取用户状态供前端面板展示。
        支持分页和按用户名 (owner_name) 搜索。

        :param page: 页码，从 1 开始
        :param size: 每页数量
        :param keyword: 用户名精确匹配关键字
        """
        page = max(1, page)
        offset = (page - 1) * size

        with self.get_connection() as conn:
            cursor = conn.cursor()

            if keyword:
                sql = """
                    SELECT * FROM api_keys 
                    WHERE owner_name = ? 
                    ORDER BY created_at DESC 
                    LIMIT ? OFFSET ?
                """
                params = (keyword, size, offset)
            else:
                sql = """
                    SELECT * FROM api_keys 
                    ORDER BY created_at DESC 
                    LIMIT ? OFFSET ?
                """
                params = (size, offset)

            cursor.execute(sql, params)
            rows = cursor.fetchall()

            items = []
            for row in rows:
                r_dict = dict(row)

                try:
                    bound_devices_str = r_dict.get("bound_devices") or "[]"
                    r_dict["bound_devices"] = json.loads(bound_devices_str)
                except (json.JSONDecodeError, TypeError):
                    r_dict["bound_devices"] = []

                items.append(r_dict)
            total = self.get_keys_counts()

            return {"total": total, "items": items}

    def get_total_api_calls(self: DBInterface) -> int:
        """获取所有API调用次数"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(total_requests) FROM api_keys")
            result = cursor.fetchone()
            return result[0] if result else 0

    def get_active_keys_counts(self: DBInterface) -> int:
        """获取当前活跃的API密钥数量"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM api_keys WHERE is_active = TRUE")
            result = cursor.fetchone()
            return result[0] if result else 0

    def get_keys_counts(self: DBInterface) -> int:
        """获取当前所有API密钥数量"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM api_keys")
            result = cursor.fetchone()
            return result[0] if result else 0

    def toggle_key_status(self: DBInterface, key_id: int, is_active: bool):
        """管理员接口：拉黑/解封某个用户"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE api_keys SET is_active = ? WHERE id = ?",
                (1 if is_active else 0, key_id),
            )
            conn.commit()

    def delete_api_key(self: DBInterface, key_id: int) -> bool:
        """管理员接口：永久删除某个 API Key"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # 先检查存不存在
            cursor.execute("SELECT id FROM api_keys WHERE id = ?", (key_id,))
            if not cursor.fetchone():
                return False

            cursor.execute("DELETE FROM api_keys WHERE id = ?", (key_id,))
            conn.commit()
            logger.info(f"API Key [ID: {key_id}] 已被永久删除")
            return True
