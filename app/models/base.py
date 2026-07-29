from typing import Any, Self

from pydantic import BaseModel


class SemanticModel(BaseModel):
    """为外部数据模型提供语义明确的创建入口。"""

    @classmethod
    def from_payload(cls, **data: Any) -> Self:
        return cls.model_validate(data)
