from __future__ import annotations

from app.infrastructure.ingestion.validators import (
    ALLOWED_DOMAINS,
    ALLOWED_SCHEMES,
    Any,
    MAX_CONTENT_LENGTH,
    MAX_TITLE_LENGTH,
    MIN_CONTENT_LENGTH,
    MIN_TITLE_LENGTH,
    REQUIRED_FIELDS,
    dataclass,
    field,
    logger,
    logging,
    re,
    urlparse,
)

@dataclass
class ValidationResult:
    """验证结果"""

    is_valid: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add_error(self, message: str) -> None:
        """添加错误"""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str) -> None:
        """添加警告"""
        self.warnings.append(message)

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """合并另一个验证结果"""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.is_valid = self.is_valid and other.is_valid
        return self
