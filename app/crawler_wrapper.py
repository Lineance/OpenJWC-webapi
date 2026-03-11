# app/worker.py
import time
import subprocess
import logging

# 引入我们之前写好的配置和数据库服务
from app.core.config import BIN_DIR, DATA_DIR
from app.services.sql_db_service import db

# 配置简单的日志，这样你在服务器上用 docker logs 就能看到爬虫运行情况
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# 定义路径
CRAWLER_BIN = BIN_DIR / "jwc-crawler"
OUTPUT_JSON = DATA_DIR / "output.json"


def run_crawler_job():
    """执行完整的：爬取 -> 存 JSON -> 同步数据库 流程"""
    logging.info("开始执行定时爬虫任务")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        # 1. 组装命令: ./jwc-crawler -o /绝对路径/data/output.json
        cmd = [str(CRAWLER_BIN), "-o", str(OUTPUT_JSON)]
        logging.info(f"正在执行命令: {' '.join(cmd)}")

        # subprocess.run 会阻塞 Python 脚本，直到 Rust 爬虫运行结束
        # check=True 表示如果 Rust 爬虫报错崩溃，Python 也会捕捉到错误
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logging.info("Rust 爬虫运行结束。")

        # 2. 将爬取到的 JSON 同步进 SQLite 数据库
        if OUTPUT_JSON.exists():
            sync_result = db.sync_from_json(str(OUTPUT_JSON))
            logging.info(
                f"数据库同步完成: 新增 {sync_result['new_added']} 条，更新 {sync_result['updated']} 条。"
            )
        else:
            logging.error("未找到 output.json，爬虫可能未成功输出文件。")

    except subprocess.CalledProcessError as e:
        logging.error(f"爬虫执行失败! 错误信息: {e.stderr}")
    except Exception as e:
        logging.error(f"发生未知错误: {e}")

    logging.info("爬虫任务环节结束\n")


if __name__ == "__main__":
    INTERVAL_SECONDS = 8 * 60 * 60

    logging.info("后台爬虫 Worker 服务已启动...")

    run_crawler_job()

    while True:
        logging.info(f"等待 {INTERVAL_SECONDS / 3600} 小时后进行下一次爬取...")
        time.sleep(INTERVAL_SECONDS)
        run_crawler_job()
