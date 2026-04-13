"""
SQL Guard - SQL 安全验证模块

防止 SQL 注入攻击，验证用户输入的安全性。

Responsibilities:
    - SQL 注入检测
    - 参数化查询转换
    - 危险模式识别
    - 白名单字段验证
"""

import logging
import re
from typing import Any

from .schema import ArticleFields

logger = logging.getLogger(__name__)


# =============================================================================
# 安全配置
# =============================================================================

# 允许查询的字段白名单
ALLOWED_FIELDS = frozenset(
    [
        ArticleFields.NEWS_ID,
        ArticleFields.TITLE,
        ArticleFields.PUBLISH_DATE,
        ArticleFields.URL,
        ArticleFields.SOURCE_SITE,
        ArticleFields.AUTHOR,
        ArticleFields.TAGS,
        ArticleFields.CONTENT_TEXT,
        ArticleFields.CRAWL_VERSION,
        ArticleFields.LAST_UPDATED,
    ]
)

# 危险的 SQL 关键字模式
DANGEROUS_PATTERNS = [
    r";\s*(?:DROP|DELETE|TRUNCATE|ALTER|CREATE|INSERT|UPDATE|COPY|EXECUTE)",
    r"/\*.*\*/",  # 块注释
    r"UNION\s+(?:ALL\s+)?SELECT",
    r"INTO\s+(?:OUTFILE|DUMPFILE)",
    r"LOAD_FILE",
    r"SLEEP\s*\(",
    r"BENCHMARK\s*\(",
    r"\bCOPY\b",  # COPY 语句关键字
    r"\bEXECUTE\b",  # EXECUTE 语句关键字
    r"0x[0-9a-fA-F]+",  # 十六进制值
]

# 编译正则表达式
DANGEROUS_REGEX = re.compile("|".join(DANGEROUS_PATTERNS), re.IGNORECASE)


# =============================================================================
# SQL Guard 类
# =============================================================================


class SQLGuard:
    """
    SQL 安全验证器

    提供 SQL 语句和参数的安全检查，防止注入攻击。

    Usage:
        >>> guard = SQLGuard()
        >>> guard.validate_where("source_site = '教务处'")  # True
        >>> guard.validate_where("1=1; DROP TABLE --")      # False
        >>> guard.sanitize_string("O'Reilly")              # "O''Reilly"
    """

    def __init__(self, allowed_fields: frozenset[str] | None = None):
        """
        初始化验证器

        Args:
            allowed_fields: 允许查询的字段白名单
        """
        self._allowed_fields = allowed_fields or ALLOWED_FIELDS

    def validate_where(self, where_clause: str) -> bool:
        """
        验证 WHERE 子句的安全性

        Args:
            where_clause: SQL WHERE 子句

        Returns:
            是否安全

        Raises:
            ValueError: 检测到危险模式时抛出
        """
        if not where_clause:
            return True

        # 移除字符串字面量，避免字符串内容被误判为注入
        # 将 'xxx' 替换为占位符，再检查危险模式
        stripped_clause = self._strip_string_literals(where_clause)

        # 检查危险模式（排除字符串内的内容）
        if DANGEROUS_REGEX.search(stripped_clause):
            logger.warning(f"Dangerous SQL pattern detected: {where_clause}")
            raise ValueError("SQL injection pattern detected")

        # 检查多语句（只有在字符串外有分号才报错）
        if ";" in stripped_clause:
            logger.warning(f"Multiple statements detected: {where_clause}")
            raise ValueError("Multiple SQL statements not allowed")

        return True

    @staticmethod
    def _strip_string_literals(clause: str) -> str:
        """
        移除 SQL 字符串字面量，用占位符替换

        这样可以避免字符串内容被误判为 SQL 注入

        Args:
            clause: SQL 子句

        Returns:
            移除字符串字面量后的子句
        """
        # 匹配单引号字符串，包括转义的单引号
        return re.sub(r"'(?:[^']|'')*'", "'__STR__'", clause)

    def validate_field(self, field_name: str) -> bool:
        """
        验证字段名是否在白名单中

        Args:
            field_name: 字段名

        Returns:
            是否有效
        """
        return field_name in self._allowed_fields

    def validate_fields(self, field_names: list[str]) -> bool:
        """
        验证多个字段名

        Args:
            field_names: 字段名列表

        Returns:
            是否全部有效

        Raises:
            ValueError: 发现非法字段时抛出
        """
        invalid = [f for f in field_names if f not in self._allowed_fields]
        if invalid:
            raise ValueError(f"Invalid fields: {invalid}")
        return True

    @staticmethod
    def sanitize_string(value: str) -> str:
        """
        清理字符串值，防止 SQL 注入

        Args:
            value: 原始字符串

        Returns:
            清理后的字符串
        """
        if not isinstance(value, str):
            return value
        # 转义单引号
        return value.replace("'", "''")

    @staticmethod
    def sanitize_identifier(identifier: str) -> str:
        """
        清理标识符 (表名、字段名)

        只允许字母、数字、下划线

        Args:
            identifier: 标识符

        Returns:
            清理后的标识符

        Raises:
            ValueError: 标识符包含非法字符
        """
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier):
            raise ValueError(f"Invalid identifier: {identifier}")
        return identifier

    def build_safe_where(
        self,
        conditions: dict[str, Any],
        operator: str = "AND",
    ) -> str:
        """
        构建安全的 WHERE 子句

        Args:
            conditions: 条件字典 {字段名: 值}
            operator: 条件连接符 (AND/OR)

        Returns:
            安全的 WHERE 子句

        Raises:
            ValueError: 字段不在白名单中
        """
        if not conditions:
            return ""

        clauses = []
        for field, value in conditions.items():
            # 验证字段名
            if not self.validate_field(field):
                raise ValueError(f"Field '{field}' not in whitelist")

            # 构建条件
            if value is None:
                clauses.append(f"{field} IS NULL")
            elif isinstance(value, str):
                safe_value = self.sanitize_string(value)
                clauses.append(f"{field} = '{safe_value}'")
            elif isinstance(value, bool):
                clauses.append(f"{field} = {str(value).lower()}")
            elif isinstance(value, (int, float)):
                clauses.append(f"{field} = {value}")
            elif isinstance(value, list):
                # IN 查询
                if all(isinstance(v, str) for v in value):
                    safe_values = [f"'{self.sanitize_string(v)}'" for v in value]
                else:
                    safe_values = [str(v) for v in value]
                clauses.append(f"{field} IN ({', '.join(safe_values)})")
            else:
                raise ValueError(f"Unsupported value type for field '{field}'")

        return f" {operator} ".join(clauses)

    def build_safe_like(self, field: str, pattern: str) -> str:
        """
        构建安全的 LIKE 子句

        Args:
            field: 字段名
            pattern: 搜索模式

        Returns:
            安全的 LIKE 子句
        """
        if not self.validate_field(field):
            raise ValueError(f"Field '{field}' not in whitelist")

        # 转义特殊字符
        safe_pattern = (
            self.sanitize_string(pattern)
            .replace("%", r"\%")
            .replace("_", r"\_")
        )
        return f"{field} LIKE '%{safe_pattern}%'"


# =============================================================================
# 便捷函数
# =============================================================================


def validate_sql(where_clause: str) -> bool:
    """
    快速验证 SQL WHERE 子句

    Args:
        where_clause: WHERE 子句

    Returns:
        是否安全
    """
    guard = SQLGuard()
    return guard.validate_where(where_clause)


def sanitize(value: str) -> str:
    """
    快速清理字符串

    Args:
        value: 原始字符串

    Returns:
        清理后的字符串
    """
    return SQLGuard.sanitize_string(value)


def build_where(conditions: dict[str, Any]) -> str:
    """
    快速构建安全的 WHERE 子句

    Args:
        conditions: 条件字典

    Returns:
        WHERE 子句
    """
    guard = SQLGuard()
    return guard.build_safe_where(conditions)
