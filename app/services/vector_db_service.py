import os
import chromadb
from zhipuai import ZhipuAI

# 初始化智谱客户端和向量数据库
client = ZhipuAI(api_key=os.environ.get("ZHIPUAI_API_KEY"))
chroma_client = chromadb.PersistentClient(path="./data/chroma_db")
collection = chroma_client.get_or_create_collection(name="notices")


class VectorDBService:
    @staticmethod
    def get_embedding(text: str):
        """调用智谱 embedding-3 接口"""
        response = client.embeddings.create(
            model="embedding-3",
            input=text,
        )
        return response.data[0].embedding

    def check_notice_exists(self, source_notice_id: str) -> bool:
        """
        核心拦截器：检查这篇资讯是否已经处理过。
        """
        results = collection.get(
            where={"source_id": source_notice_id}, limit=1, include=["metadatas"]
        )
        return len(results["ids"]) > 0

    def add_chunk(self, chunk_id: str, content: str, metadata: dict):
        print(f"正在为 Chunk {chunk_id} 生成 Embedding (消耗 Token)...")
        vector = self.get_embedding(content)

        collection.upsert(
            ids=[chunk_id],
            embeddings=[vector],
            documents=[content],
            metadatas=[metadata],
        )

    def search(self, query: str, n_results: int = 3):
        """语义搜索"""
        query_vector = self.get_embedding(query)
        results = collection.query(query_embeddings=[query_vector], n_results=n_results)

        # 安全隐患修复：如果数据库为空，results["documents"][0] 会报错 IndexError
        if not results.get("documents") or not results["documents"][0]:
            return "知识库中暂无相关资讯。"

        return "\n".join(results["documents"][0])

    def process_and_index_notice(self, notice: dict):
        """入口函数：在这里做拦截"""
        notice_id = notice["id"]  # 这里的 id 是 Rust 传来的全文哈希
        if self.check_notice_exists(notice_id):
            print(f"资讯 [{notice['title']}] ({notice_id}) 内容未变，跳过。")
            return

        print(f"开始处理并向量化新资讯 [{notice['title']}]...")
        content = notice["text"]
        chunk_size = 500
        chunks = [
            content[i : i + chunk_size] for i in range(0, len(content), chunk_size)
        ]

        for i, chunk in enumerate(chunks):
            chunk_id = f"{notice_id}_chunk_{i}"
            # 存入向量库
            self.add_chunk(
                chunk_id=chunk_id,
                content=chunk,
                metadata={"source_id": notice_id, "title": notice["title"]},
            )
        print(f"资讯 [{notice['title']}] 入库完成。")


vector_db = VectorDBService()

if __name__ == "__main__":
    pass
