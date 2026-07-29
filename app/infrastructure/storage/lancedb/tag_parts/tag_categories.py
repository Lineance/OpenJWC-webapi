from __future__ import annotations

from app.infrastructure.storage.lancedb.tag_schema import (
    Any,
    TAG_EMBEDDING_DIM,
    TagFields,
    TagIndexConfig,
    TagRecord,
    dataclass,
    datetime,
    get_tag_schema,
    logger,
    logging,
    normalize_tag_description,
    normalize_tag_name,
    pa,
    uuid,
    validate_tag_embedding,
)

class TagCategories:
    """预定义标签类别常量"""

    EVENT = "event"
    CAREER = "career"
    ADMIN = "admin"
    ACADEMIC = "academic"
    CAMPUS = "campus"
    OTHER = "other"

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
