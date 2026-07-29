from __future__ import annotations

from app.infrastructure.storage.lancedb.exceptions import (
    DatabaseError,
)

class RepositorySystemError(DatabaseError):
    """仓库系统错误 - 基础设施故障（如磁盘满、权限问题）"""

    pass

class RepositoryNotFoundError(DatabaseError):
    """仓库记录未找到"""

    pass
