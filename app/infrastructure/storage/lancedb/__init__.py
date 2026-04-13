"""
Data Layer - 数据层

提供 LanceDB 数据库的纯 I/O 操作，不包含业务逻辑。

Responsibilities:
    - LanceDB 连接管理
    - 数据表 CRUD 操作
    - Schema 定义和验证
    - SQL 安全防护

主要组件:
    - Connection: 数据库连接管理
    - Repository: 数据仓库操作
    - Schema: 数据结构定义
    - Guard: SQL 安全验证
"""

from .connection import (
    LanceDBConnection,
    get_articles_table,
    get_connection,
    init_database,
)
from .guard import SQLGuard, sanitize, validate_sql
from .repository import (
    ArticleRepository,
    create_article_repository,
    get_article_repository,
)
from .schema import ArticleFields, ArticleRecord, get_article_schema

__all__ = [
    # Connection
    "LanceDBConnection",
    "get_connection",
    "get_articles_table",
    "init_database",
    # Repository
    "ArticleRepository",
    "get_article_repository",
    "create_article_repository",
    # Schema
    "ArticleFields",
    "ArticleRecord",
    "get_article_schema",
    # Guard
    "SQLGuard",
    "validate_sql",
    "sanitize",
]
