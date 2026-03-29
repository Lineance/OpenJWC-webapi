import logging
import chromadb
from app.services.sql_db_service import db
from datetime import date
from zhipuai import ZhipuAI
from zhipuai.core._errors import (
    APIConnectionError,
    APITimeoutError,
    APIInternalError,
    APIReachLimitError,
)
from datetime import datetime, timedelta
from app.utils.logging_manager import setup_logger
from app.core.config import MAX_DAY_DIFF

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# 初始化智谱客户端和向量数据库
client = ZhipuAI(api_key=db.get_system_setting("zhipu_api_key"), timeout=60)
chroma_client = chromadb.PersistentClient(path="./data/chroma_db")
collection = chroma_client.get_or_create_collection(name="notices")

logger = setup_logger("vector_db_logs")


class VectorDBService:
    def __init__(self):
        self.client = ZhipuAI(
            api_key=db.get_system_setting("zhipu_api_key"), timeout=60
        )

    def reinitialize_client(self):
        """重新实例化client"""
        logger.info("正在重新初始化智谱AI客户端")
        self.client = ZhipuAI(
            api_key=db.get_system_setting("zhipu_api_key"), timeout=60
        )
        logger.info("智谱AI客户端重新初始化完成")

    @retry(
        # 针对智谱 SDK 抛出的网络、超时、限流、500错误进行重试
        retry=retry_if_exception_type(
            (
                APIConnectionError,
                APITimeoutError,
                APIInternalError,
                APIReachLimitError,
                Exception,
            )
        ),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(4),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _call_zhipu_embedding_with_retry(self, text: str):
        return self.client.embeddings.create(
            model="embedding-3",
            input=text,
        )

    def get_embedding(self, text: str):
        """调用智谱 embedding-3 接口"""
        preview_text = text[:20].replace("\n", "") + "..." if len(text) > 20 else text
        logger.info(f"调用智谱 embedding-3 接口，输入文本: {preview_text}")
        try:
            response = self._call_zhipu_embedding_with_retry(text)
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"调用智谱 embedding-3 接口失败: {e}")
            raise Exception("调用智谱 embedding-3 接口失败")

    def check_notice_exists(self, source_notice_id: str) -> bool:
        """
        核心拦截器：检查这篇资讯是否已经处理过。
        """
        results = collection.get(
            where={"source_id": source_notice_id}, limit=1, include=["metadatas"]
        )
        if len(results["ids"]) > 0:
            logger.info(f"资讯{source_notice_id}已经处理")
            return True
        else:
            logger.info(f"资讯未处理{source_notice_id}，将进行向量化处理")
            return False

    def add_chunk(
        self, chunk_id: str, child_content: str, parent_content: str, metadata: dict
    ):
        logger.info(f"正在为 Chunk {chunk_id} 生成 Embedding (消耗 Token)...")
        vector = self.get_embedding(child_content)

        collection.upsert(
            ids=[chunk_id],
            embeddings=[vector],
            documents=[parent_content],
            metadatas=[metadata],
        )

    def sync_vector_db_metadata(self):
        """
        同步notices表中的数据到向量数据库的元信息中，
        确保search只能搜索到notices表中存在的资讯
        """
        logger.info("开始同步向量数据库元信息...")
        notices = db.get_all_notices()
        notice_ids_in_db = {notice["id"] for notice in notices}
        all_vector_ids = set()
        try:
            offset = 0
            batch_size = 1000
            while True:
                results = collection.get(
                    include=["metadatas"], offset=offset, limit=batch_size
                )
                if not results.get("ids") or len(results["ids"]) == 0:
                    break
                for metadata in results.get("metadatas", []):
                    if metadata and "source_id" in metadata:
                        all_vector_ids.add(metadata["source_id"])
                offset += batch_size
                if len(results["ids"]) < batch_size:
                    break
        except Exception as e:
            logger.error(f"获取向量数据库元信息失败: {e}")
            return False
        logger.info(f"notices表中有 {len(notice_ids_in_db)} 条资讯")
        logger.info(f"向量数据库中有 {len(all_vector_ids)} 条资讯")
        updated_count = 0
        for vector_id in all_vector_ids:
            is_in_notices = vector_id in notice_ids_in_db
            try:
                results = collection.get(
                    where={"source_id": vector_id}, include=["metadatas"]
                )
                if results.get("ids"):
                    for i, metadata in enumerate(results.get("metadatas", [])):
                        if metadata:
                            updated_metadata = metadata.copy()
                            updated_metadata["in_notices_table"] = is_in_notices

                            chunk_id = results["ids"][i]
                            collection.upsert(
                                ids=[chunk_id], metadatas=[updated_metadata]
                            )
                            updated_count += 1

            except Exception as e:
                logger.error(f"更新资讯 {vector_id} 元信息失败: {e}")
                continue

        logger.info(f"元信息同步完成，共更新 {updated_count} 个chunks")
        return True

    def search(self, query: str, n_results: int = 10) -> str:
        """语义搜索"""
        query_vector = self.get_embedding(f"{query}, 今日日期{date.today()}")
        cutoff_date = date.today() - timedelta(
            days=int(db.get_system_setting("search_max_day_diff"))
        )
        cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")
        cutoff_date_int = int(cutoff_date.strftime("%Y%m%d"))
        where_clause = {
            "date_int": {"$gte": cutoff_date_int},
            "in_notices_table": {"$eq": True},
        }
        results = collection.query(
            query_embeddings=[query_vector], n_results=n_results, where=where_clause
        )
        if not results.get("documents") or not results["documents"][0]:
            logger.warning("知识库中暂无相关资讯")
            return "知识库中暂无相关资讯"
        logger.info(f"搜索到 {len(results['documents'][0])} 条相关资讯")
        return "\n".join(results["documents"][0])

    def process_and_index_notice(self, notice: dict):
        """
        用来向量化并入库整条信息。
        此处接受的notice结构应为{"id":..., "title":..., "content_text":..., "date":...}
        原因是出于向量数据库、传统数据库以及json文件之间解耦的考量，
        此处写入向量数据库的数据是从传统数据库中读出的。
        """
        notice_id = notice["id"]  # 这里的 id 是 Rust 传来的链接哈希
        if self.check_notice_exists(notice_id):
            logger.info(f"资讯 [{notice['title']}] ({notice_id}) 内容未变，跳过。")
            return False
        # 此处按理因为id不是全文哈希，我们其实看不出内容有没有变，但是先这样了。
        date_str = notice["date"]
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        today = datetime.now()
        diff = today - date_obj
        if diff.days > MAX_DAY_DIFF:
            logger.info(f"资讯发布日期距今已有{MAX_DAY_DIFF}天，认作旧闻而跳过。")
            return False
        content = notice["content_text"]
        if content is None:
            logger.info(f"资讯[{notice['title']}]无正文信息，跳过")
            return False
        chunk_size = 500
        chunks = [
            f"资讯标题：{notice['title']};资讯日期：{notice['date']};资讯正文：{content[max(i - 50, 0) : min(i + chunk_size + 50, len(content))]}"
            for i in range(0, len(content), chunk_size)
        ]
        logger.info(f"开始处理并向量化新资讯 [{notice['title']}]...")
        for i, chunk in enumerate(chunks):
            chunk_id = f"{notice_id}_chunk_{i}"
            # 存入向量库
            self.add_chunk(
                chunk_id=chunk_id,
                child_content=chunk,
                parent_content=chunk,
                metadata={
                    "source_id": notice_id,
                    "title": notice["title"],
                    "date": notice["date"],
                    "date_int": int(notice["date"].replace("-", "")),
                    "in_notices_table": True,
                },
            )
        logger.info(f"资讯 [{notice['title']}] 入库完成。")
        return True


vector_db = VectorDBService()

if __name__ == "__main__":
    from app.crawler_wrapper import sync_vector_db

    sync_vector_db()
    vector_db.sync_vector_db_metadata()
