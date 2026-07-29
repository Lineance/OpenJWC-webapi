from __future__ import annotations

from app.infrastructure.storage.lancedb.exceptions import (
    DatabaseError,
    RepositoryNotFoundError,
    RepositorySystemError,
)

class ValidationError(DatabaseError):
    """数据验证错误"""

    pass
