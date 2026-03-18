import asyncio
import sqlite3
from app.core.config import NOTICE_DB, NOTICE_JSON
from app.services.db_interface import logger
from app.services.sql_mixins.notice_mixin import NoticeMixin
from app.services.sql_mixins.validation_mixin import ValidationMixin
from app.services.sql_mixins.admin_mixin import AdminMixin
from app.services.sql_mixins.device_mixin import DeviceMixin
from app.utils.ping_check import diagnose_network_environment
from rich import print

import cmd


class DBService(NoticeMixin, ValidationMixin, AdminMixin, DeviceMixin):
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
        create_index_sql = (
            "CREATE INDEX IF NOT EXISTS idx_key_string ON api_keys(key_string);"
        )
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(create_notices_sql)
            cursor.execute(create_keys_sql)
            cursor.execute(create_admin_sql)
            cursor.execute(create_index_sql)
            cursor.execute(create_system_sql)
            conn.commit()
            logger.info("sql数据库初始化完成")


# 单例模式导出，方便全局其他地方使用同一个实例
db = DBService()


class SQLCLI(cmd.Cmd):
    prompt = "sqlcli>>> "

    def do_q(self, arg):
        """
        退出cli工具。
        """
        return True

    def do_valid(self, arg: str):
        """
        模拟登录验证。
        参数分别为apikey和device id。
        """
        args = arg.split()
        isvalid, info = db.validate_and_use_key(key_string=args[0], device_id=args[1])
        logger.info("API VALIDATION: " + str(isvalid))
        logger.info("info: " + info)

    def do_create(self, arg: str):
        """
        创建user或admin。
        user: 参数分别为用户名和最大设备数。
        admin: 参数分别为用户名和密码。
        """
        args = arg.split()
        if len(args) == 3:
            if args[0] == "user":
                new_key = db.create_api_key(
                    owner_name=args[1], max_devices=int(args[2])
                )
                logger.info("NEW KEY: " + new_key)
            elif args[0] == "admin":
                logger.info(db.create_admin(username=args[1], password=args[2]))

    def do_show(self, arg: str):
        """
        显示所有apikey的状态
        """
        keys = db.get_all_api_keys()
        for key in keys:
            print(key)

    def do_admin(self, arg: str):
        """
        check: 查看一个admin账号的状态。
        delete: 删除一个admin账号。
        """
        args = arg.split()
        if args[0] == "check":
            logger.info(db.get_admin_user(args[1]))
        elif args[0] == "delete":
            logger.info(db.delete_admin(args[1]))

    def do_toggle(self, arg: str):
        """
        启停一个apikey。
        """
        args = arg.split()
        db.toggle_key_status(key_id=int(args[0]), is_active=(args[1] != "0"))

    def do_unbind(self, arg: str):
        """
        解绑设备。
        参数分别为apikey和device。
        """
        args = arg.split()
        db.unbind_device(api_key=args[0], device_id=args[1])

    def do_delete(self, arg: str):
        """
        删除apikey。
        """
        ids = arg.split()
        for id in ids:
            deleted = db.delete_api_key(key_id=int(id))
            logger.info(f"移除{id}: {deleted}")

    def do_check(self, arg: str):
        """
        查看一个apikey的设备绑定情况。
        """
        args = arg.split()
        info = db.get_device_info(key_string=args[0], device_id=args[1])
        print(info)

    def do_set(self, arg: str):
        """
        调整系统设置。
        """
        args = arg.split()
        db.update_system_setting(setting_key=args[0], setting_value=args[1])
        logger.info(f"设置{args[0]}为{args[1]}")

    def do_echo(self, arg: str):
        """
        获取系统设置。
        """
        print(db.get_all_settings())

    def do_sync(self, arg: str):
        """
        同步系统设置。
        """
        db._sync_settings()

    def do_diagnose(self, arg: str):
        asyncio.run(diagnose_network_environment())


if __name__ == "__main__":
    import sys

    if "--reset" in sys.argv:
        logger.info("正在进行重置同步模式...")
        db.drop_table()
    else:
        db.init_db()

    result = db.sync_from_json(NOTICE_JSON)
    logger.info(f"同步完成: {result}")
