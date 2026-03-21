# app/worker.py
import time
import subprocess
from datetime import date, timedelta

from app.core.config import DATA_DIR, CRAWLER_BIN, NOTICE_JSON
from app.services.sql_db_service import db
from app.services.vector_db_service import vector_db
from app.utils.logging_manager import setup_logger

logger = setup_logger("crawler_logs")


def sync_vector_db():
    """将 SQL 数据库中的数据单向同步到 VectorDB"""
    logger.info("开始进行 向量数据库 (VectorDB) 同步...")

    try:
        all_notices = db.get_all_notices()
        total_count = len(all_notices)
        logger.info(f"从 SQL 数据库读取到 {total_count} 条记录，准备核对向量库。")
        new_embedded_count = 0
        for notice in all_notices:
            is_new = vector_db.process_and_index_notice(
                {
                    "id": notice["id"],
                    "title": notice["title"],
                    "content_text": notice["content_text"],
                    "date": notice["date"],
                }
            )
            if is_new:
                new_embedded_count += 1

        logger.info(
            f"向量库同步完成！跳过了 {total_count - new_embedded_count} 条，实际新增向量化 {new_embedded_count} 条。"
        )

    except Exception as e:
        logger.exception(f"向量数据库同步失败: {e}")


def execute_crawling_task():
    """执行一次爬取任务，返回爬虫命令的执行结果"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(CRAWLER_BIN),
        "-o",
        str(NOTICE_JSON),
        "-d",
        str(
            (
                date.today()
                - timedelta(days=int(db.get_system_setting("crawler_days_gap") or "0"))
            ).strftime("%Y-%m-%d"),
        ),
    ]
    logger.info(f"正在执行命令: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    logger.info("Rust 爬虫运行结束。")
    return result


def process_crawling_result():
    """处理爬虫结果，将数据同步到数据库和向量数据库"""
    if NOTICE_JSON.exists():
        sync_result = db.sync_from_json(str(NOTICE_JSON))
        logger.info(
            f"数据库同步完成: 新增 {sync_result['new_added']} 条，更新 {sync_result['updated']} 条。"
        )
        if sync_result["new_added"] > 0 or sync_result["updated"] > 0:
            sync_vector_db()
        else:
            logger.info("SQL 数据无变化，跳过向量数据库同步。")
    else:
        logger.error("未找到 output.json，爬虫可能未成功输出文件。")


def run_crawler_job():
    """执行完整的：爬取 -> 存 JSON -> 同步数据库 流程"""
    logger.info("开始执行定时爬虫任务")
    try:
        execute_crawling_task()
        process_crawling_result()
    except subprocess.CalledProcessError:
        logger.exception("爬虫执行失败!")
    except Exception:
        logger.exception("发生未知错误!")
    logger.info("爬虫任务环节结束\n")


if __name__ == "__main__":
    logger.info("后台爬虫服务已启动...")
    run_crawler_job()
    while True:
        logger.info(
            f"等待 {int(db.get_system_setting('crawler_interval_minutes')) / 60} 小时后进行下一次爬取..."
        )
        time.sleep(int(db.get_system_setting("crawler_interval_minutes")) * 60)
        run_crawler_job()
