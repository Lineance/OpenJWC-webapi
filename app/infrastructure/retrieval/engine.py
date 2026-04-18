"""
Retrieval Engine - 混合检索引擎

提供高级的混合检索功能，整合向量搜索和全文搜索。

Responsibilities:
    - 混合检索 (向量 + 关键词)
    - 查询预处理和向量化
    - 结果融合和排序
    - 过滤和分页
"""

import logging
from typing import Any, Literal

from .schema.article import ArticleQuery
from .store import LanceStore, create_store
from .utils.embedding import RetrievalEmbedder, get_retrieval_embedder

logger = logging.getLogger(__name__)


# =============================================================================
# 检索引擎
# =============================================================================


class RetrievalEngine:
    """
    混合检索引擎

    整合向量搜索和全文搜索，提供统一的检索接口。

    Features:
        - 混合检索 (向量相似度 + 关键词匹配)
        - 多字段搜索 (标题、正文、来源等)
        - 高级过滤 (时间范围、来源、作者等)
        - 结果融合和重排序
    """

    def __init__(
        self,
        store: LanceStore | None = None,
        embedder: RetrievalEmbedder | None = None,
        db_path: str = "../data/lancedb",
        table_name: str = "articles",
    ) -> None:
        """
        初始化检索引擎

        Args:
            store: LanceStore 实例
            embedder: 检索向量化器
            db_path: 数据库路径（相对于 backend/ 目录）
            table_name: 表名
        """
        if store is None:
            # 索引由写入链路确保；检索初始化仅负责拿到可用存储实例。
            store = create_store(db_path, table_name, create_indices=False)

        # 检查 store 类型
        if isinstance(store, str):
            logger.error(f"create_store returned a string: {store}")
            raise TypeError(f"Expected LanceStore but got string: {store}")

        if not hasattr(store, "hybrid_search"):
            logger.error("store object missing hybrid_search method")
            raise TypeError("store object is not a LanceStore instance")

        self._store = store
        self._embedder = embedder or get_retrieval_embedder()

        logger.info(
            f"RetrievalEngine initialized with store type: {type(store).__name__}"
        )

    # =========================================================================
    # 核心检索方法
    # =========================================================================

    def search(
        self,
        query: str,
        search_type: str = "hybrid",
        limit: int = 10,
        offset: int = 0,
        **filters: Any,
    ) -> dict[str, Any]:
        """
        通用搜索接口

        Args:
            query: 查询文本
            search_type: 搜索类型 (hybrid, vector, fulltext)
            limit: 返回数量
            offset: 偏移量
            **filters: 过滤条件

        Returns:
            搜索结果
        """
        # 构建查询对象
        query_obj = ArticleQuery(
            keyword=query,
            limit=limit,
            offset=offset,
            **filters,
        )

        # 验证查询
        is_valid, errors = query_obj.validate_data()
        if not is_valid:
            raise ValueError(f"Invalid query: {errors}")

        # 执行搜索
        if search_type == "vector":
            results = self._vector_search(query_obj)
        elif search_type == "fulltext":
            results = self._fulltext_search(query_obj)
        else:  # hybrid
            results = self._hybrid_search(query_obj)

        # 应用分页
        paginated_results = results[offset : offset + limit]

        return {
            "query": query,
            "search_type": search_type,
            "total": len(results),
            "limit": limit,
            "offset": offset,
            "results": paginated_results,
            "filters": filters,
        }

    def _vector_search(self, query_obj: ArticleQuery) -> list[dict[str, Any]]:
        """向量搜索"""

        # 生成查询向量
        vector: list[float] | tuple[list[float], list[float]]
        if query_obj.vector_query:
            vector = query_obj.vector_query
        else:
            # 确保 keyword 不是 None
            keyword = query_obj.keyword or ""
            # 确保 field 参数类型正确
            field_str = query_obj.vector_field.replace("_embedding", "")
            # 将 field_str 转换为 Literal['title', 'content', 'both'] 类型
            field: Literal["title", "content", "both"]
            if field_str == "title":
                field = "title"
            elif field_str == "content":
                field = "content"
            else:
                # 默认使用 content
                field = "content"

            vector = self._embedder.embed_query(
                keyword,
                field=field,
            )

        # 确保 vector 是列表类型
        if isinstance(vector, tuple):
            vector = vector[0]  # 取标题向量

        results = self._store.vector_search(
            query_vector=vector,
            vector_field=query_obj.vector_field,
            limit=query_obj.limit * 3,  # 获取更多结果用于后续处理
            where=query_obj.build_where_clause(),
        )
        return results

    def _fulltext_search(self, query_obj: ArticleQuery) -> list[dict[str, Any]]:
        """全文搜索"""
        if not query_obj.keyword:
            return []

        return self._store.fulltext_search(
            query=query_obj.keyword,
            fields=query_obj.search_fields,
            limit=query_obj.limit * 3,
            where=query_obj.build_where_clause(),
        )

    def _hybrid_search(self, query_obj: ArticleQuery) -> list[dict[str, Any]]:
        """混合搜索"""
        return self._store.hybrid_search(
            query=query_obj.keyword or "",
            query_obj=query_obj,
        )

    # =========================================================================
    # 高级检索方法
    # =========================================================================

    def semantic_search(
        self,
        query: str,
        field: Literal["title", "content", "both"] = "content",
        similarity_threshold: float = 0.7,
        limit: int = 10,
        **filters: Any,
    ) -> dict[str, Any]:
        """
        语义搜索 (纯向量)

        Args:
            query: 查询文本
            field: 搜索字段 (title/content)
            similarity_threshold: 相似度阈值
            limit: 返回数量
            **filters: 过滤条件

        Returns:
            搜索结果
        """
        query_obj = ArticleQuery(
            keyword=query,
            vector_field=f"{field}_embedding",
            similarity_threshold=similarity_threshold,
            limit=limit,
            **filters,
        )

        results = self._vector_search(query_obj)

        # 按相似度排序
        results.sort(key=lambda x: x.get("_score", 0), reverse=True)

        return {
            "query": query,
            "search_type": "semantic",
            "field": field,
            "similarity_threshold": similarity_threshold,
            "total": len(results),
            "results": results[:limit],
        }

    def keyword_search(
        self,
        query: str,
        fields: list[str] | None = None,
        match_type: str = "any",  # any, all, phrase
        limit: int = 10,
        **filters: Any,
    ) -> dict[str, Any]:
        """
        关键词搜索 (纯全文)

        Args:
            query: 查询文本
            fields: 搜索字段
            match_type: 匹配类型
            limit: 返回数量
            **filters: 过滤条件

        Returns:
            搜索结果
        """
        if fields is None:
            fields = ["title", "content_text"]

        query_obj = ArticleQuery(
            keyword=query,
            search_fields=fields,
            limit=limit,
            **filters,
        )

        results = self._fulltext_search(query_obj)

        # 根据匹配类型过滤结果
        if match_type == "all":
            keywords = query.lower().split()
            filtered = []
            for result in results:
                text = " ".join(str(result.get(field, "")) for field in fields).lower()
                if all(keyword in text for keyword in keywords):
                    filtered.append(result)
            results = filtered
        elif match_type == "phrase":
            filtered = []
            for result in results:
                text = " ".join(str(result.get(field, "")) for field in fields).lower()
                if query.lower() in text:
                    filtered.append(result)
            results = filtered

        return {
            "query": query,
            "search_type": "keyword",
            "fields": fields,
            "match_type": match_type,
            "total": len(results),
            "results": results[:limit],
        }

    def advanced_search(
        self,
        query: str,
        vector_weight: float = 0.6,
        keyword_weight: float = 0.4,
        title_weight: float = 0.3,
        content_weight: float = 0.7,
        limit: int = 10,
        **filters: Any,
    ) -> dict[str, Any]:
        """
        高级混合搜索

        Args:
            query: 查询文本
            vector_weight: 向量搜索权重
            keyword_weight: 关键词搜索权重
            title_weight: 标题向量权重
            content_weight: 正文向量权重
            limit: 返回数量
            **filters: 过滤条件

        Returns:
            搜索结果
        """
        # 构建查询对象
        query_obj = ArticleQuery(
            keyword=query,
            keyword_weight=keyword_weight,
            vector_weight=vector_weight,
            limit=limit,
            **filters,
        )

        # 执行混合搜索
        results = self._hybrid_search(query_obj)

        # 计算综合分数
        for result in results:
            # 向量相似度
            title_sim = 0.0
            content_sim = 0.0

            if "title_embedding" in result:
                title_vec = self._embedder.embed_query(query, field="title")
                if isinstance(title_vec, tuple):
                    title_vec = title_vec[0]
                title_sim = self._embedder.cosine_similarity(
                    title_vec, result["title_embedding"]
                )

            if "content_embedding" in result:
                content_vec = self._embedder.embed_query(query, field="content")
                if isinstance(content_vec, tuple):
                    content_vec = (
                        content_vec[1] if len(content_vec) > 1 else content_vec[0]
                    )
                content_sim = self._embedder.cosine_similarity(
                    content_vec, result["content_embedding"]
                )

            # 综合分数
            vector_score = title_sim * title_weight + content_sim * content_weight
            keyword_score = result.get("_score", 0.5)  # 从混合搜索获取

            result["_vector_score"] = vector_score
            result["_keyword_score"] = keyword_score
            result["_final_score"] = (
                vector_score * vector_weight + keyword_score * keyword_weight
            )

        # 按最终分数排序
        results.sort(key=lambda x: x.get("_final_score", 0), reverse=True)

        return {
            "query": query,
            "search_type": "advanced",
            "weights": {
                "vector": vector_weight,
                "keyword": keyword_weight,
                "title": title_weight,
                "content": content_weight,
            },
            "total": len(results),
            "results": results[:limit],
        }

    # =========================================================================
    # 辅助方法
    # =========================================================================

    def get_document(self, news_id: str) -> dict[str, Any] | None:
        """
        获取单个文档

        Args:
            news_id: 新闻ID

        Returns:
            文档数据
        """
        try:
            results = (
                self._store.table.search()
                .where(f"news_id = '{news_id}'")
                .limit(1)
                .to_list()
            )
            return results[0] if results else None
        except Exception as e:
            logger.error(f"Failed to get document {news_id}: {e}")
            return None

    def get_similar_documents(
        self,
        news_id: str,
        field: str = "content",
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """
        获取相似文档

        Args:
            news_id: 参考文档ID
            field: 相似字段 (title/content)
            limit: 返回数量

        Returns:
            相似文档列表
        """
        # 获取参考文档
        doc = self.get_document(news_id)
        if not doc:
            return []

        # 获取文档向量
        vector_field = f"{field}_embedding"
        if vector_field not in doc:
            return []

        # 向量搜索
        return self._store.vector_search(
            query_vector=doc[vector_field],
            vector_field=vector_field,
            limit=limit + 1,  # 包含自己
            where=f"news_id != '{news_id}'",  # 排除自己
        )[:limit]

    def get_statistics(self) -> dict[str, Any]:
        """
        获取统计信息

        Returns:
            统计信息
        """
        try:
            count = self._store.count()
            info = self._store.info()

            # 获取来源分布
            sources: dict[str, int] = {}
            try:
                results = self._store.table.search().select(["source_site"]).to_list()
                for doc in results:
                    source = doc.get("source_site", "未知")
                    sources[source] = sources.get(source, 0) + 1
            except Exception as e:
                print(f"RetrievalEngine Exception:{e}")
                pass

            # 获取时间范围
            time_range = {}
            try:
                results = self._store.table.search().select(["publish_date"]).to_list()
                dates = [
                    doc["publish_date"]
                    for doc in results
                    if doc.get("publish_date") is not None
                ]
                if dates:
                    time_range["min"] = min(dates)
                    time_range["max"] = max(dates)
            except Exception as e:
                print(f"RetrievalEngine Exception:{e}")
                pass

            return {
                "total_documents": count,
                "table_info": info,
                "source_distribution": sources,
                "time_range": time_range,
            }
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}


# =============================================================================
# 便捷函数
# =============================================================================


def create_engine(
    db_path: str = "../data/lancedb",
    table_name: str = "articles",
) -> RetrievalEngine:
    """
    创建检索引擎

    Args:
        db_path: 数据库路径
        table_name: 表名

    Returns:
        RetrievalEngine 实例
    """
    return RetrievalEngine(db_path=db_path, table_name=table_name)


def get_engine() -> RetrievalEngine:
    """
    获取检索引擎单例

    Returns:
        RetrievalEngine 实例
    """
    return create_engine()
