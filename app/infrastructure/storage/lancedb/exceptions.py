"""
Database Exceptions

数据库层异常定义。
"""


class DatabaseError(Exception):
    """数据库操作基础异常"""

    pass


class RepositorySystemError(DatabaseError):
    """仓库系统错误 - 基础设施故障（如磁盘满、权限问题）"""

    pass


class RepositoryNotFoundError(DatabaseError):
    """仓库记录未找到"""

    pass


class ValidationError(DatabaseError):
    """数据验证错误"""

    pass
