"""
Tag Schema - 标签数据结构定义

定义标签的数据结构，用于存储预定义标签及其向量表示。

Responsibilities:
    - Tag 数据模型定义
    - PyArrow Schema 生成
    - Tag 记录的序列化/反序列化
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

logger = logging.getLogger(__name__)

# =============================================================================
# 向量维度常量 (与正文向量保持一致)
# =============================================================================

# 使用 BAAI/bge-large-zh 模型 (1024 维)
TAG_EMBEDDING_DIM = 1024

# =============================================================================
# 字段名常量
# =============================================================================


class TagFields:
    """Tag 表字段名常量"""

    TAG_ID = "tag_id"
    NAME = "name"
    DESCRIPTION = "description"
    CATEGORY = "category"
    EMBEDDING = "embedding"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


# =============================================================================
# PyArrow Schema 定义
# =============================================================================


def get_tag_schema() -> pa.Schema:
    """
    获取 Tag 表的 PyArrow Schema

    字段说明:
    - tag_id: 主键，唯一标识符
    - name: 标签名称 (如 "学术讲座")
    - description: 详细描述 (用于向量匹配)
    - category: 分类 (如 "event", "career", "admin")
    - embedding: 描述文本的向量表示 (1024 维)
    - created_at: 创建时间
    - updated_at: 更新时间

    Returns:
        pa.Schema: PyArrow Schema 对象
    """
    return pa.schema(
        [
            # 主键和基本信息
            pa.field(TagFields.TAG_ID, pa.string(), nullable=False),
            pa.field(TagFields.NAME, pa.string(), nullable=False),
            pa.field(TagFields.DESCRIPTION, pa.string(), nullable=False),
            pa.field(TagFields.CATEGORY, pa.string(), nullable=True),
            # 向量字段
            pa.field(
                TagFields.EMBEDDING,
                pa.list_(pa.float32(), TAG_EMBEDDING_DIM),
                nullable=False,
            ),
            # 时间戳字段
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


# =============================================================================
# 数据模型类
# =============================================================================


@dataclass
class TagRecord:
    """
    Tag 记录的数据类

    Attributes:
        tag_id: 标签唯一标识符
        name: 标签名称
        description: 详细描述
        category: 分类
        embedding: 向量表示
        created_at: 创建时间
        updated_at: 更新时间
    """

    tag_id: str
    name: str
    description: str
    embedding: list[float]
    created_at: datetime
    updated_at: datetime
    category: str | None = None

    @classmethod
    def create_new(
        cls,
        name: str,
        description: str,
        embedding: list[float],
        category: str | None = None,
    ) -> "TagRecord":
        """
        创建新的 TagRecord

        Args:
            name: 标签名称
            description: 详细描述
            embedding: 向量表示
            category: 分类

        Returns:
            TagRecord 实例
        """
        now = datetime.now()
        tag_id = f"tag_{uuid.uuid4().hex[:8]}"

        return cls(
            tag_id=tag_id,
            name=name,
            description=description,
            embedding=embedding,
            category=category,
            created_at=now,
            updated_at=now,
        )

    def to_dict(self) -> dict[str, Any]:
        """
        转换为字典格式，用于写入 LanceDB

        Returns:
            字典数据
        """
        return {
            TagFields.TAG_ID: self.tag_id,
            TagFields.NAME: self.name,
            TagFields.DESCRIPTION: self.description,
            TagFields.CATEGORY: self.category,
            TagFields.EMBEDDING: self.embedding,
            TagFields.CREATED_AT: self.created_at,
            TagFields.UPDATED_AT: self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TagRecord":
        """
        从字典创建 TagRecord 实例

        Args:
            data: 字典数据

        Returns:
            TagRecord 实例
        """
        return cls(
            tag_id=data[TagFields.TAG_ID],
            name=data[TagFields.NAME],
            description=data[TagFields.DESCRIPTION],
            category=data.get(TagFields.CATEGORY),
            embedding=data[TagFields.EMBEDDING],
            created_at=data[TagFields.CREATED_AT],
            updated_at=data[TagFields.UPDATED_AT],
        )

    def update_embedding(self, new_embedding: list[float]) -> None:
        """
        更新向量表示

        Args:
            new_embedding: 新的向量
        """
        self.embedding = new_embedding
        self.updated_at = datetime.now()

    def update_info(
        self,
        name: str | None = None,
        description: str | None = None,
        category: str | None = None,
    ) -> None:
        """
        更新标签信息

        Args:
            name: 新名称
            description: 新描述
            category: 新分类
        """
        if name is not None:
            self.name = name
        if description is not None:
            self.description = description
        if category is not None:
            self.category = category
        self.updated_at = datetime.now()


# =============================================================================
# 索引配置
# =============================================================================


class TagIndexConfig:
    """Tag 表索引配置"""

    # 向量索引类型
    VECTOR_INDEX_TYPE = "IVF_PQ"

    # IVF 分区数
    IVF_PARTITIONS = 128

    # PQ 子量化器数量
    PQ_SUBQUANTIZERS = 64

    # 向量索引字段
    VECTOR_INDEX_FIELDS = [TagFields.EMBEDDING]

    # 全文索引字段
    FTS_FIELDS = [TagFields.NAME, TagFields.DESCRIPTION]


# =============================================================================
# 便捷函数
# =============================================================================


def validate_tag_embedding(embedding: list[float]) -> bool:
    """
    验证向量是否符合要求

    Args:
        embedding: 向量列表

    Returns:
        是否有效
    """
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
    """
    标准化标签名称

    Args:
        name: 原始名称

    Returns:
        标准化后的名称
    """
    return name.strip()


def normalize_tag_description(description: str) -> str:
    """
    标准化标签描述

    Args:
        description: 原始描述

    Returns:
        标准化后的描述
    """
    return description.strip()


# =============================================================================
# 预定义标签类别
# =============================================================================


class TagCategories:
    """预定义标签类别常量"""

    EVENT = "event"  # 活动类 (讲座、会议等)
    CAREER = "career"  # 职业类 (招聘、实习等)
    ADMIN = "admin"  # 行政类 (通知、公告等)
    ACADEMIC = "academic"  # 学术类 (科研、论文等)
    CAMPUS = "campus"  # 校园生活类
    OTHER = "other"  # 其他

    @classmethod
    def get_all_categories(cls) -> list[str]:
        """获取所有类别"""
        return [
            cls.EVENT,
            cls.CAREER,
            cls.ADMIN,
            cls.ACADEMIC,
            cls.CAMPUS,
            cls.OTHER,
        ]
