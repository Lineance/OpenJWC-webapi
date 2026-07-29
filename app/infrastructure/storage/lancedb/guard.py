"""SQL Guard - SQL 安全验证模块"""

import logging
import re
from typing import Any

from .schema import ArticleFields

logger = logging.getLogger(__name__)

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

DANGEROUS_PATTERNS = [
    r";\s*(?:DROP|DELETE|TRUNCATE|ALTER|CREATE|INSERT|UPDATE|COPY|EXECUTE)",
    r"/\*.*\*/",
    r"UNION\s+(?:ALL\s+)?SELECT",
    r"INTO\s+(?:OUTFILE|DUMPFILE)",
    r"LOAD_FILE",
    r"SLEEP\s*\(",
    r"BENCHMARK\s*\(",
    r"\bCOPY\b",
    r"\bEXECUTE\b",
    r"0x[0-9a-fA-F]+",
]

DANGEROUS_REGEX = re.compile("|".join(DANGEROUS_PATTERNS), re.IGNORECASE)

class SQLGuard:
    """SQL 安全验证器"""

    def __init__(self, allowed_fields: frozenset[str] | None = None) -> None:
        """初始化验证器"""
        self._allowed_fields = allowed_fields or ALLOWED_FIELDS

    def validate_where(self, where_clause: str) -> bool:
        """验证 WHERE 子句的安全性"""
        if not where_clause:
            return True

        stripped_clause = self._strip_string_literals(where_clause)

        if DANGEROUS_REGEX.search(stripped_clause):
            logger.warning(f"Dangerous SQL pattern detected: {where_clause}")
            raise ValueError("SQL injection pattern detected")

        if ";" in stripped_clause:
            logger.warning(f"Multiple statements detected: {where_clause}")
            raise ValueError("Multiple SQL statements not allowed")

        return True

    @staticmethod
    def _strip_string_literals(clause: str) -> str:
        """移除 SQL 字符串字面量，用占位符替换"""

        return re.sub(r"'(?:[^']|'')*'", "'__STR__'", clause)

    def validate_field(self, field_name: str) -> bool:
        """验证字段名是否在白名单中"""
        return field_name in self._allowed_fields

    def validate_fields(self, field_names: list[str]) -> bool:
        """验证多个字段名"""
        invalid = [f for f in field_names if f not in self._allowed_fields]
        if invalid:
            raise ValueError(f"Invalid fields: {invalid}")
        return True

    @staticmethod
    def sanitize_string(value: str) -> str:
        """清理字符串值，防止 SQL 注入"""
        if not isinstance(value, str):
            return value

        return value.replace("'", "''")

    @staticmethod
    def sanitize_identifier(identifier: str) -> str:
        """清理标识符 (表名、字段名)"""
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier):
            raise ValueError(f"Invalid identifier: {identifier}")
        return identifier

    def build_safe_where(
        self,
        conditions: dict[str, Any],
        operator: str = "AND",
    ) -> str:
        """构建安全的 WHERE 子句"""
        if not conditions:
            return ""

        clauses = []
        for field, value in conditions.items():

            if not self.validate_field(field):
                raise ValueError(f"Field '{field}' not in whitelist")

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

                if all(isinstance(v, str) for v in value):
                    safe_values = [f"'{self.sanitize_string(v)}'" for v in value]
                else:
                    safe_values = [str(v) for v in value]
                clauses.append(f"{field} IN ({', '.join(safe_values)})")
            else:
                raise ValueError(f"Unsupported value type for field '{field}'")

        return f" {operator} ".join(clauses)

    def build_safe_like(self, field: str, pattern: str) -> str:
        """构建安全的 LIKE 子句"""
        if not self.validate_field(field):
            raise ValueError(f"Field '{field}' not in whitelist")

        safe_pattern = (
            self.sanitize_string(pattern)
            .replace("%", r"\%")
            .replace("_", r"\_")
        )
        return f"{field} LIKE '%{safe_pattern}%'"

def validate_sql(where_clause: str) -> bool:
    """快速验证 SQL WHERE 子句"""
    guard = SQLGuard()
    return guard.validate_where(where_clause)

def sanitize(value: str) -> str:
    """快速清理字符串"""
    return SQLGuard.sanitize_string(value)

def build_where(conditions: dict[str, Any]) -> str:
    """快速构建安全的 WHERE 子句"""
    guard = SQLGuard()
    return guard.build_safe_where(conditions)
