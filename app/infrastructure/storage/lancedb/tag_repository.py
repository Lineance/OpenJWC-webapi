"""Tag Repository - 标签数据 CRUD 操作"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import lancedb

from .connection import LanceDBConnection, get_connection
from .exceptions import RepositorySystemError
from .tag_schema import (
    TAG_EMBEDDING_DIM,
    TagFields,
    TagIndexConfig,
    TagRecord,
    get_tag_schema,
)

logger = logging.getLogger(__name__)

TAGS_TABLE_NAME = "tags"

from .tag_repository_mixins.tag_write_mixin import TagWriteMixin
from .tag_repository_mixins.tag_find_mixin import TagFindMixin
from .tag_repository_mixins.tag_index_mixin import TagIndexMixin
from .tag_repository_mixins.tag_stats_mixin import TagStatsMixin

class TagRepository(TagWriteMixin, TagFindMixin, TagIndexMixin, TagStatsMixin):
    """Tag 数据仓库"""

    def __init__(self, connection: LanceDBConnection | None = None) -> None:
        """初始化仓库"""
        self._conn = connection or get_connection()
        self._table = self._get_or_create_table()
        logger.info(f"TagRepository initialized for table: {self._table.name}")

    def _get_or_create_table(self) -> lancedb.table.Table:
        """获取或创建 tags 表"""
        tables_obj = self._conn.db.list_tables()
        table_names = getattr(tables_obj, "tables", tables_obj)

        if TAGS_TABLE_NAME in table_names:
            logger.info(f"Opening existing table: {TAGS_TABLE_NAME}")
            return self._conn.get_table(TAGS_TABLE_NAME)

        logger.info(f"Creating new table: {TAGS_TABLE_NAME}")
        schema = get_tag_schema()
        table = self._conn.db.create_table(TAGS_TABLE_NAME, schema=schema)
        logger.info(f"Table '{TAGS_TABLE_NAME}' created successfully")
        return table

    @property
    def table(self) -> lancedb.table.Table:
        """获取底层表对象"""
        return self._table

    @property
    def schema(self) -> Any:
        """获取表结构"""
        return self._table.schema

def get_tag_repository(connection: LanceDBConnection | None = None) -> TagRepository:
    """获取 TagRepository 实例"""
    return TagRepository(connection=connection)

def create_tag_repository() -> TagRepository:
    """创建 TagRepository 实例"""
    return get_tag_repository()
