from __future__ import annotations

from app.infrastructure.storage.lancedb.tag_schema import (
    Any,
    TAG_EMBEDDING_DIM,
    TagFields,
    dataclass,
    datetime,
    get_tag_schema,
    logger,
    logging,
    pa,
    uuid,
)

@dataclass
class TagRecord:
    """Tag 记录的数据类"""

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
        """创建新的 TagRecord"""
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
        """转换为字典格式，用于写入 LanceDB"""
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
        """从字典创建 TagRecord 实例"""
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
        """更新向量表示"""
        self.embedding = new_embedding
        self.updated_at = datetime.now()

    def update_info(
        self,
        name: str | None = None,
        description: str | None = None,
        category: str | None = None,
    ) -> None:
        """更新标签信息"""
        if name is not None:
            self.name = name
        if description is not None:
            self.description = description
        if category is not None:
            self.category = category
        self.updated_at = datetime.now()
