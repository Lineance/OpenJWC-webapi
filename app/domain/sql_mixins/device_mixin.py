from app.services.db_interface import DBInterface, logger
from app.models.schemas import ResponseModel
import json


class DeviceMixin:
    def unbind_device(self: DBInterface, api_key: str, device_id: str) -> bool:
        """解绑特定设备"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT bound_devices FROM api_keys WHERE key_string = ?", (api_key,)
            )
            row = cursor.fetchone()
            if not row:
                logger.warning("未找到匹配的apikey，解绑请求失败")
                return False
            devices = json.loads(row[0])
            if device_id in devices:
                devices.remove(device_id)
            else:
                logger.warning("未找到匹配的device，解绑请求失败")
                return False
            new_devices_json = json.dumps(devices)
            cursor.execute(
                "UPDATE api_keys SET bound_devices = ? WHERE key_string = ?",
                (new_devices_json, api_key),
            )
            conn.commit()
            logger.info(f"设备'{device_id[:8]}...'与apikey'{api_key[:8]}...'解绑成功")
            return True

    def get_device_info(
        self: DBInterface, key_string: str, device_id: str
    ) -> ResponseModel:
        """
        检查 Key 的有效性，校验设备指纹，并增加请求计数。
        获取该apikey能绑定的最多设备数以及目前已经绑定的设备。
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, is_active, max_devices, bound_devices FROM api_keys WHERE key_string = ?",
                (key_string,),
            )
            row = cursor.fetchone()
            if not row:
                return ResponseModel(msg="该API Key不存在", data={})

            if not row["is_active"]:
                return ResponseModel(msg="该API Key已停用", data={})

            bound_devices = json.loads(row["bound_devices"])
            if device_id not in bound_devices:
                return ResponseModel(msg="设备不存在或与apikey之间无绑定关系", data={})

            return ResponseModel(
                msg="请求成功",
                data={
                    "total": row["max_devices"],
                    "bound_devices": json.loads(row["bound_devices"]),
                },
            )
