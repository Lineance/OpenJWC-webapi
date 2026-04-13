import asyncio
from datetime import date

import cmd2
from rich import print

from app.application.submission.submission_service import audit_and_import_submission
from app.infrastructure.crawler.rust_crawler_wrapper import run_crawler_job
from app.infrastructure.storage.lancedb.repository import get_article_repository
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.infrastructure.storage.sqlite.submission_repository import SubmissionRepository
from app.utils.logging_manager import setup_logger
from app.utils.ping_check import diagnose_network_environment

logger = setup_logger("cli_logs")
article_repo = get_article_repository()
submission_repo = SubmissionRepository(db)


class SQLCLI(cmd2.Cmd):
    def __init__(self):
        super().__init__()
        self.prompt = "admin>>> "
        self.debug = True

    def preloop(self) -> None:
        print("[bold green]OpenJWC Admin CLI[/bold green]")

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
        args = arg.split()
        if args[0] == "apikeys":
            keys = db.get_all_api_keys()
            for key in keys:
                print(key)
        elif args[0] == "notices":
            print(article_repo.get_latest(limit=int(args[1])))
        elif args[0] == "submissions":
            status = args[1]
            _, items = submission_repo.list_for_admin(
                limit=1000, offset=0, status=status
            )
            print(items)
        elif args[0] == "admins":
            print(db.get_all_admins())

    def do_audit(self, arg: str):
        args = arg.split()
        audit_and_import_submission(
            submission_id=args[0], status=args[1], review=args[2]
        )

    def do_admin(self, arg: str):
        """
        check: 查看一个admin账号的状态。
        delete: 删除一个admin账号。
        mdpw: 修改admin账号的密码。
        sync: 按照配置文件单向同步管理员账号。
        """
        args = arg.split()
        if args[0] == "check":
            logger.info(db.get_admin_user(args[1]))
        elif args[0] == "delete":
            logger.info(db.delete_admin(args[1]))
        elif args[0] == "mdpw":
            logger.info(db.modify_password(args[1], args[2]))
        elif args[0] == "sync":
            logger.info(db.sync_admins_from_config())

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
        args = arg.split()
        if args[0] == "apikeys":
            ids = args[1:]
            for id in ids:
                deleted = db.delete_api_key(key_id=int(id))
                logger.info(f"移除{id}: {deleted}")
        elif args[0] == "notices":
            ids = args[1:]
            for id in ids:
                deleted = article_repo.delete(id)
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

    def do_reset(self, arg: str):
        """
        重置系统设置为默认值。
        """
        args = arg.split()
        if args:
            for setting in args:
                db.reset_system_setting(setting)
        else:
            db.reset_all_settings()

    def do_diagnose(self, arg: str):
        """
        诊断网络环境。
        """
        asyncio.run(diagnose_network_environment())

    def do_drop(self, arg: str):
        """
        删除指定 SQLite 用户态表（受白名单限制）。
        """
        args = arg.split()
        for table in args:
            try:
                db.drop_table(table)
            except ValueError as exc:
                logger.warning(str(exc))

    def do_crawl(self, arg: str):
        run_crawler_job()

    def do_newmotto(self, arg: str):
        if db.replace_motto_from_hitokoto(date.today().strftime("%Y-%m-%d")):
            logger.info("success.")
        else:
            logger.warning("fail.")
