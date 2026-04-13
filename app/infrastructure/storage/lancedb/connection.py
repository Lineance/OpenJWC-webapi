"""
Database Connection - LanceDB 连接池管理

单例模式管理 LanceDB 连接，提供线程安全的访问接口。

Responsibilities:
    - 单例模式管理数据库连接
    - 线程安全的表访问
    - 表初始化和索引创建
    - 连接健康检查
"""

import logging
import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

import lancedb

from .schema import ARTICLES_TABLE_NAME, ARTICLE_ORDER_TABLE_NAME, ArticleFields, IndexConfig, get_article_schema, get_article_order_schema

if TYPE_CHECKING:
    from lancedb.db import DBConnection
    from lancedb.table import Table

logger = logging.getLogger(__name__)

# =============================================================================
# 默认配置
# =============================================================================

# 默认数据库路径 (相对于项目根目录)
DEFAULT_DB_PATH = "data/lancedb"


# =============================================================================
# LanceDB 连接管理器
# =============================================================================


class LanceDBConnection:
    """
    LanceDB 连接池管理器 (单例模式)

    Features:
        - 线程安全的单例实例
        - 自动创建数据库目录
        - 表初始化和索引管理
        - 连接健康检查

    Usage:
        >>> conn = LanceDBConnection()
        >>> table = conn.get_table("articles")
        >>> # 或使用全局函数
        >>> table = get_articles_table()
    """

    _instance: "LanceDBConnection | None" = None
    _lock = threading.Lock()
    _initialized: bool
    _db_path: str
    _db: "DBConnection"

    def __new__(cls, db_path: str | None = None) -> "LanceDBConnection":
        """
        创建或获取单例实例

        Args:
            db_path: 数据库路径，仅在首次创建时生效

        Returns:
            LanceDBConnection 单例实例
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance

    def __init__(self, db_path: str | None = None) -> None:
        """
        初始化连接

        Args:
            db_path: 数据库路径，默认为 ./data/campus.lance
        """
        if getattr(self, "_initialized", False):
            return

        resolved_db_path = db_path or os.getenv("LANCE_DB_PATH") or DEFAULT_DB_PATH
        # 确保路径是绝对的
        if not os.path.isabs(resolved_db_path):
            # 基于项目根目录解析相对路径
            project_root = Path(__file__).resolve().parents[2]  # connection.py -> database -> backend -> 项目根
            resolved_db_path = str(project_root / resolved_db_path)
        self._db_path = resolved_db_path
        self._ensure_db_directory()

        logger.info(f"Connecting to LanceDB at: {self._db_path}")
        self._db: DBConnection = lancedb.connect(self._db_path)
        self._tables: dict[str, Table] = {}
        self._table_lock = threading.Lock()
        self._initialized = True

        logger.info("LanceDB connection established successfully")

    def _ensure_db_directory(self) -> None:
        """确保数据库目录存在"""
        db_dir = Path(self._db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

    @property
    def db(self) -> "DBConnection":
        """获取原始数据库连接"""
        return self._db

    @property
    def db_path(self) -> str:
        """获取数据库路径"""
        return self._db_path

    def get_table(self, name: str = ARTICLES_TABLE_NAME) -> "Table":
        """
        获取表对象 (线程安全)

        Args:
            name: 表名，默认为 articles

        Returns:
            LanceDB Table 对象

        Raises:
            ValueError: 表不存在
        """
        if name not in self._tables:
            with self._table_lock:
                if name not in self._tables:
                    try:
                        self._tables[name] = self._db.open_table(name)
                        logger.debug(f"Opened existing table: {name}")
                    except Exception as e:
                        raise ValueError(f"Table '{name}' does not exist: {e}") from e
        return self._tables[name]

    def create_articles_table(self, exist_ok: bool = True) -> "Table":
        """
        创建 articles 表并初始化索引

        Args:
            exist_ok: 如果表已存在，是否返回现有表

        Returns:
            创建或获取的表对象
        """
        table_names = self._db.table_names()

        if ARTICLES_TABLE_NAME in table_names:
            if exist_ok:
                logger.info(f"Table '{ARTICLES_TABLE_NAME}' already exists, returning existing")
                return self.get_table(ARTICLES_TABLE_NAME)
            raise ValueError(f"Table '{ARTICLES_TABLE_NAME}' already exists")

        # 创建空表
        logger.info(f"Creating table: {ARTICLES_TABLE_NAME}")
        schema = get_article_schema()
        table = self._db.create_table(ARTICLES_TABLE_NAME, schema=schema)

        # 缓存表引用
        with self._table_lock:
            self._tables[ARTICLES_TABLE_NAME] = table

        logger.info(f"Table '{ARTICLES_TABLE_NAME}' created successfully")
        return table

    def create_article_order_table(self, exist_ok: bool = True) -> "Table":
        """
        创建 article_order 表用于维护按时间排序的文章 ID 列表

        Args:
            exist_ok: 如果表已存在，是否返回现有表

        Returns:
            创建或获取的表对象
        """
        table_names = self._db.table_names()

        if ARTICLE_ORDER_TABLE_NAME in table_names:
            if exist_ok:
                logger.info(f"Table '{ARTICLE_ORDER_TABLE_NAME}' already exists")
                return self.get_table(ARTICLE_ORDER_TABLE_NAME)
            raise ValueError(f"Table '{ARTICLE_ORDER_TABLE_NAME}' already exists")

        logger.info(f"Creating table: {ARTICLE_ORDER_TABLE_NAME}")
        schema = get_article_order_schema()
        table = self._db.create_table(ARTICLE_ORDER_TABLE_NAME, schema=schema)

        with self._table_lock:
            self._tables[ARTICLE_ORDER_TABLE_NAME] = table

        logger.info(f"Table '{ARTICLE_ORDER_TABLE_NAME}' created successfully")
        return table

    def get_ordered_news_ids(self, offset: int, limit: int, category: str | None = None) -> tuple[list[str], int]:
        """
        从 article_order 表获取按时间排序的新闻 ID 列表

        注意: LanceDB 0.30.x 不支持 order_by，数据在插入时已按 ordinal 排序存储，
        因此可以直接使用 limit/offset 获取分页数据。

        Args:
            offset: 偏移量
            limit: 返回数量
            category: 分类筛选，None 表示全局排序

        Returns:
            (news_ids, total_count) 元组
        """
        order_table = self.create_article_order_table(exist_ok=True)

        # 如果 order 表为空，自动重建
        if order_table.count_rows() == 0:
            logger.info("Order table is empty, rebuilding...")
            self.rebuild_article_order()

        # 全局排序：数据在插入时已按 ordinal 排序，直接使用 limit/offset
        if not category:
            total = order_table.count_rows()
            # 数据已按 ordinal 排序存储，直接 limit/offset 即可
            results = order_table.search().limit(limit).offset(offset).to_list()
        else:
            # 分类筛选：需要过滤 + 排序（因为分类内序号 ordinal_by_category 也是插入时排序的）
            # 由于 order_by 不可用，只能在 Python 中排序
            all_results = order_table.search().where(f"category = '{category}'").to_list()
            total = len(all_results)
            # 按 ordinal_by_category 排序（数据已按此顺序插入）
            all_results.sort(key=lambda r: r.get("ordinal_by_category", 0))
            results = all_results[offset:offset + limit]

        news_ids = [r.get("news_id") for r in results if r.get("news_id")]
        return news_ids, total

    def rebuild_article_order(self) -> int:
        """
        重建 article_order 表，根据 publish_date 降序排序

        同时计算全局 ordinal 和按分类的 ordinal_by_category

        Returns:
            排序后的记录数
        """
        articles_table = self.get_table(ARTICLES_TABLE_NAME)
        order_table = self.create_article_order_table(exist_ok=True)

        # 获取所有文章 - 只选择需要的列
        try:
            all_articles = articles_table.search().select(["news_id", "publish_date", "source_site"]).to_list()
        except Exception as e:
            logger.warning(f"search with select failed: {e}, falling back to pandas")
            all_articles = articles_table.to_pandas()[["news_id", "publish_date", "source_site"]].to_dict("records")

        # 按 publish_date 降序排序
        def sort_key(item):
            pd = item.get("publish_date")
            if pd is None:
                return "1970-01-01"
            if hasattr(pd, "isoformat"):
                return pd.isoformat()
            return str(pd)

        all_articles.sort(key=sort_key, reverse=True)

        # 构建排序后的列表，同时计算分类内序号
        order_data = []
        category_counters: dict[str, int] = {}

        for i, article in enumerate(all_articles, start=1):
            category = article.get("source_site") or ""

            # 分类内序号
            if category not in category_counters:
                category_counters[category] = 0
            category_counters[category] += 1

            order_data.append({
                "ordinal": i,
                "ordinal_by_category": category_counters[category],
                "news_id": article.get("news_id", ""),
                "publish_date": article.get("publish_date"),
                "category": category,
            })

        # 清空并重新写入
        try:
            self._db.drop_table(ARTICLE_ORDER_TABLE_NAME)
            with self._table_lock:
                self._tables.pop(ARTICLE_ORDER_TABLE_NAME, None)
            order_table = self._db.create_table(ARTICLE_ORDER_TABLE_NAME, schema=get_article_order_schema())
            with self._table_lock:
                self._tables[ARTICLE_ORDER_TABLE_NAME] = order_table
        except Exception as e:
            logger.warning(f"Failed to recreate order table: {e}")

        if order_data:
            order_table.add(order_data)

        logger.info(f"Rebuilt article_order with {len(order_data)} articles")
        return len(order_data)

    def create_indices(self, table_name: str = ARTICLES_TABLE_NAME) -> None:
        """
        为表创建索引 (向量索引 + 全文索引)

        规则：
        1. 向量索引：只有在数据行数 >= 256 时才创建
        2. 全文索引：为每个字段单独创建

        注意: 索引创建需要表中有数据才能生效

        Args:
            table_name: 表名
        """
        table = self.get_table(table_name)

        # 检查表是否有数据
        row_count = table.count_rows()
        if row_count == 0:
            logger.warning(f"Table '{table_name}' is empty, skipping index creation")
            return

        logger.info(f"Creating indices for table '{table_name}' ({row_count} rows)")

        # 1. 创建正文向量索引 (用于语义搜索) - 仅在行数 >= 256 时创建
        if row_count >= 256:
            try:
                table.create_index(
                    metric="cosine",
                    vector_column_name=ArticleFields.CONTENT_EMBEDDING,
                    index_type=IndexConfig.VECTOR_INDEX_TYPE,
                    num_partitions=min(IndexConfig.IVF_PARTITIONS, row_count),
                    num_sub_vectors=IndexConfig.PQ_SUBQUANTIZERS,
                    replace=True,
                )
                logger.info(
                    f"Vector index created on '{ArticleFields.CONTENT_EMBEDDING}' (row_count={row_count} >= 256)"
                )
            except Exception as e:
                logger.warning(f"Failed to create vector index: {e}")
        else:
            logger.info(f"Skipping vector index creation: row_count={row_count} < 256")

        # 2. 创建全文索引 (Tantivy) - 为每个字段单独创建
        try:
            for field in IndexConfig.FTS_FIELDS:
                try:
                    table.create_fts_index(
                        field,
                        use_tantivy=IndexConfig.FTS_USE_TANTIVY,
                        replace=True,
                    )
                    logger.info(f"FTS index created on '{field}'")
                except Exception as e:
                    logger.warning(f"Failed to create FTS index on field '{field}': {e}")
                    # 继续为其他字段创建索引
        except Exception as e:
            logger.warning(f"Failed to create FTS indices: {e}")

    def table_exists(self, name: str = ARTICLES_TABLE_NAME) -> bool:
        """检查表是否存在"""
        return name in self._db.table_names()

    def drop_table(self, name: str) -> None:
        """
        删除表

        Args:
            name: 表名

        Warning:
            此操作不可恢复
        """
        logger.warning(f"Dropping table: {name}")
        self._db.drop_table(name)
        with self._table_lock:
            self._tables.pop(name, None)

    def health_check(self) -> dict[str, Any]:
        """
        执行健康检查

        Returns:
            包含健康状态的字典
        """
        try:
            tables = self._db.table_names()
            articles_count = 0

            if ARTICLES_TABLE_NAME in tables:
                articles_count = self.get_table(ARTICLES_TABLE_NAME).count_rows()

            return {
                "status": "healthy",
                "db_path": self._db_path,
                "tables": tables,
                "articles_count": articles_count,
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
            }

    @classmethod
    def reset(cls) -> None:
        """
        重置单例实例 (仅用于测试)

        Warning:
            此方法仅应在测试中使用
        """
        with cls._lock:
            if cls._instance is not None:
                cls._instance._tables.clear()
                cls._instance = None
                logger.warning("LanceDB connection reset")


# =============================================================================
# 便捷函数
# =============================================================================


def get_connection(db_path: str | None = None) -> LanceDBConnection:
    """
    获取 LanceDB 连接实例

    Args:
        db_path: 数据库路径 (仅首次调用时生效)

    Returns:
        LanceDBConnection 单例实例
    """
    return LanceDBConnection(db_path)


def get_articles_table() -> "Table":
    """
    获取 articles 表

    Returns:
        articles 表对象

    Raises:
        ValueError: 表不存在时抛出
    """
    return get_connection().get_table(ARTICLES_TABLE_NAME)


def init_database(db_path: str | None = None, create_indices: bool = False) -> LanceDBConnection:
    """
    初始化数据库 (创建表和索引)

    Args:
        db_path: 数据库路径
        create_indices: 是否创建索引

    Returns:
        LanceDBConnection 实例
    """
    conn = get_connection(db_path)
    conn.create_articles_table(exist_ok=True)

    if create_indices:
        conn.create_indices()

    return conn
