"""Tag Schema - 标签数据结构定义"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

logger = logging.getLogger(__name__)

TAG_EMBEDDING_DIM = 1024

class TagFields:
    """Tag 表字段名常量"""

    TAG_ID = "tag_id"
    NAME = "name"
    DESCRIPTION = "description"
    CATEGORY = "category"
    EMBEDDING = "embedding"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"

def get_tag_schema() -> pa.Schema:
    """获取 Tag 表的 PyArrow Schema"""
    return pa.schema(
        [

            pa.field(TagFields.TAG_ID, pa.string(), nullable=False),
            pa.field(TagFields.NAME, pa.string(), nullable=False),
            pa.field(TagFields.DESCRIPTION, pa.string(), nullable=False),
            pa.field(TagFields.CATEGORY, pa.string(), nullable=True),

            pa.field(
                TagFields.EMBEDDING,
                pa.list_(pa.float32(), TAG_EMBEDDING_DIM),
                nullable=False,
            ),

            pa.field(
                TagFields.CREATED_AT,
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
            pa.field(
                TagFields.UPDATED_AT,
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
        ]
    )

from app.infrastructure.storage.lancedb.tag_parts.tag_record import (
    TagRecord,
)

class TagIndexConfig:
    """Tag 表索引配置"""

    VECTOR_INDEX_TYPE = "IVF_PQ"

    IVF_PARTITIONS = 128

    PQ_SUBQUANTIZERS = 64

    VECTOR_INDEX_FIELDS = [TagFields.EMBEDDING]

    FTS_FIELDS = [TagFields.NAME, TagFields.DESCRIPTION]

def validate_tag_embedding(embedding: list[float]) -> bool:
    """验证向量是否符合要求"""
    if not embedding:
        return False
    if len(embedding) != TAG_EMBEDDING_DIM:
        logger.warning(
            f"Tag embedding dimension mismatch: "
            f"expected {TAG_EMBEDDING_DIM}, got {len(embedding)}"
        )
        return False
    return True

def normalize_tag_name(name: str) -> str:
    """标准化标签名称"""
    return name.strip()

def normalize_tag_description(description: str) -> str:
    """标准化标签描述"""
    return description.strip()

from app.infrastructure.storage.lancedb.tag_parts.tag_categories import (
    TagCategories,
)
