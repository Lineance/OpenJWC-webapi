"""Database Connection - LanceDB 连接池管理"""

import logging
import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

import lancedb

from .schema import (
    ARTICLES_TABLE_NAME,
    ArticleFields,
    IndexConfig,
    get_article_schema,
)

if TYPE_CHECKING:
    from lancedb.db import DBConnection
    from lancedb.table import Table

logger = logging.getLogger(__name__)

def _table_names(db: "DBConnection") -> list[str]:
    tables_obj = db.list_tables()
    names = getattr(tables_obj, "tables", tables_obj)
    return list(names)

DEFAULT_DB_PATH = "data/lancedb"

def _find_project_root(start: Path) -> Path:
    """从当前文件向上查找项目根目录（以 pyproject.toml 为锚点）。"""
    current = start.resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate

    return current.parents[4] if len(current.parents) > 4 else current.parent

def _resolve_db_path(db_path: str | None) -> str:
    """解析 LanceDB 路径，保证相对路径统一锚定到项目根目录。"""
    raw_path = db_path or os.getenv("LANCE_DB_PATH") or DEFAULT_DB_PATH
    expanded_path = os.path.expanduser(raw_path)
    if os.path.isabs(expanded_path):
        return expanded_path

    project_root = _find_project_root(Path(__file__).resolve().parent)
    return str((project_root / expanded_path).resolve())

from .connection_mixins.connection_index_mixin import ConnectionIndexMixin
from .connection_mixins.connection_management_mixin import ConnectionManagementMixin

class LanceDBConnection(ConnectionIndexMixin, ConnectionManagementMixin):
    """LanceDB 连接池管理器 (单例模式)"""

    _instance: "LanceDBConnection | None" = None
    _lock = threading.Lock()
    _initialized: bool
    _db_path: str
    _db: "DBConnection"

    def __new__(cls, db_path: str | None = None) -> "LanceDBConnection":
        """创建或获取单例实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance

    def __init__(self, db_path: str | None = None) -> None:
        """初始化连接"""
        if getattr(self, "_initialized", False):
            return

        self._db_path = _resolve_db_path(db_path)
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
        """获取表对象 (线程安全)"""
        if name not in self._tables:
            with self._table_lock:
                if name not in self._tables:
                    try:
                        self._tables[name] = self._db.open_table(name)
                        logger.debug(f"Opened existing table: {name}")
                    except Exception as e:
                        raise ValueError(f"Table '{name}' does not exist: {e}") from e
        return self._tables[name]

def get_connection(db_path: str | None = None) -> LanceDBConnection:
    """获取 LanceDB 连接实例"""
    return LanceDBConnection(db_path)

def get_articles_table() -> "Table":
    """获取 articles 表"""
    return get_connection().get_table(ARTICLES_TABLE_NAME)

def init_database(
    db_path: str | None = None, create_indices: bool = False
) -> LanceDBConnection:
    """初始化数据库 (创建表和索引)"""
    conn = get_connection(db_path)
    conn.create_articles_table(exist_ok=True)

    if create_indices:
        conn.create_indices()

    return conn
