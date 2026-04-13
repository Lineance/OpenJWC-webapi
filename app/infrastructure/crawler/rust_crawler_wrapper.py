# app/worker.py
import subprocess
import time
from datetime import date, timedelta

from app.core.config import CRAWLER_BIN, DATA_DIR, NOTICE_JSON
from app.infrastructure.ingestion.adapters import CrawlerAdapter
from app.infrastructure.ingestion.pipeline import IngestionPipeline
from app.infrastructure.storage.lancedb.connection import get_connection
from app.infrastructure.storage.sqlite.sql_db_service import db
from app.utils.logging_manager import setup_logger

logger = setup_logger("crawler_logs")
adapter = CrawlerAdapter()
pipeline = IngestionPipeline()


def _get_crawler_interval_minutes() -> int:
    setting_value = db.get_system_setting("crawler_interval_minutes")
    return int(setting_value or "480")


def sync_search_index():
    """将爬虫结果通过 ingestion pipeline 同步到 LanceDB 检索索引"""
    logger.info("开始进行 LanceDB 检索索引同步...")

    try:
        docs = adapter.load_from_file(str(NOTICE_JSON))
        result = pipeline.process_batch(docs)
        get_connection().rebuild_article_order()
        logger.info(
            f"LanceDB 检索索引同步完成！total={result.total}, success={result.success}, duplicate={result.duplicate}, invalid={result.invalid}, error={result.error}"
        )

    except Exception as e:
        logger.exception(f"LanceDB 检索索引同步失败: {e}")


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
    """处理爬虫结果，将数据同步到 LanceDB 检索索引"""
    if NOTICE_JSON.exists():
        sync_search_index()
    else:
        logger.error("未找到 output.json，爬虫可能未成功输出文件。")


def run_crawler_job():
    """执行完整的：爬取 -> 存 JSON -> 同步 LanceDB 流程"""
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
        interval_minutes = _get_crawler_interval_minutes()
        logger.info(f"等待 {interval_minutes / 60} 小时后进行下一次爬取...")
        time.sleep(interval_minutes * 60)
        run_crawler_job()
