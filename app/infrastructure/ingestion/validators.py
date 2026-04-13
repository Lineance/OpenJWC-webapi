"""
Data Validators - 数据验证模块

提供 URL 和内容的验证功能。

Responsibilities:
    - URL 格式验证
    - 内容长度检查
    - 必填字段验证
    - 编码检测 (UTF-8)
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


# =============================================================================
# 验证配置
# =============================================================================

# URL 验证配置
ALLOWED_SCHEMES = frozenset(["http", "https"])
ALLOWED_DOMAINS = frozenset(["seu.edu.cn"])  # 允许的域名后缀

# 内容长度配置
MIN_CONTENT_LENGTH = 10  # 最小内容长度
MAX_CONTENT_LENGTH = 1_000_000  # 最大内容长度 (1MB)
MIN_TITLE_LENGTH = 1  # 最小标题长度
MAX_TITLE_LENGTH = 500  # 最大标题长度

# 必填字段
REQUIRED_FIELDS = ["news_id", "title", "url"]


# =============================================================================
# 验证结果
# =============================================================================


@dataclass
class ValidationResult:
    """
    验证结果

    Attributes:
        is_valid: 是否通过验证
        errors: 错误信息列表
        warnings: 警告信息列表
    """

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


# =============================================================================
# URL 验证器
# =============================================================================


class URLValidator:
    """
    URL 格式验证器

    验证规则:
    - 必须是有效的 URL 格式
    - 协议必须是 http 或 https
    - 可选: 域名白名单验证
    """

    def __init__(
        self,
        allowed_schemes: frozenset[str] = ALLOWED_SCHEMES,
        allowed_domains: frozenset[str] | None = None,
        require_domain_whitelist: bool = False,
    ):
        """
        初始化 URL 验证器

        Args:
            allowed_schemes: 允许的协议
            allowed_domains: 允许的域名后缀
            require_domain_whitelist: 是否强制域名白名单
        """
        self._allowed_schemes = allowed_schemes
        self._allowed_domains = allowed_domains or ALLOWED_DOMAINS
        self._require_domain_whitelist = require_domain_whitelist

    def validate(self, url: str) -> ValidationResult:
        """
        验证 URL

        Args:
            url: 待验证的 URL

        Returns:
            验证结果
        """
        result = ValidationResult()

        if not url:
            result.add_error("URL is empty")
            return result

        if not isinstance(url, str):
            result.add_error(f"URL must be string, got {type(url).__name__}")
            return result

        # 解析 URL
        try:
            parsed = urlparse(url)
        except Exception as e:
            result.add_error(f"Invalid URL format: {e}")
            return result

        # 验证协议
        if parsed.scheme not in self._allowed_schemes:
            result.add_error(
                f"Invalid scheme '{parsed.scheme}', allowed: {', '.join(self._allowed_schemes)}"
            )

        # 验证域名
        if not parsed.netloc:
            result.add_error("URL must have a domain")
        elif self._require_domain_whitelist:
            domain = parsed.netloc.lower()
            if not any(domain.endswith(d) for d in self._allowed_domains):
                result.add_error(
                    f"Domain '{domain}' not in whitelist: {', '.join(self._allowed_domains)}"
                )

        return result


# =============================================================================
# 内容验证器
# =============================================================================


class ContentValidator:
    """
    内容验证器

    验证规则:
    - 内容不能为空
    - 内容长度在允许范围内
    - 可选: UTF-8 编码检测
    """

    def __init__(
        self,
        min_length: int = MIN_CONTENT_LENGTH,
        max_length: int = MAX_CONTENT_LENGTH,
        check_encoding: bool = True,
    ):
        """
        初始化内容验证器

        Args:
            min_length: 最小长度
            max_length: 最大长度
            check_encoding: 是否检查编码
        """
        self._min_length = min_length
        self._max_length = max_length
        self._check_encoding = check_encoding

    def validate(self, content: str) -> ValidationResult:
        """
        验证内容

        Args:
            content: 待验证的内容

        Returns:
            验证结果
        """
        result = ValidationResult()

        if not content:
            result.add_error("Content is empty")
            return result

        if not isinstance(content, str):
            result.add_error(f"Content must be string, got {type(content).__name__}")
            return result

        # 检查长度
        content_length = len(content)
        if content_length < self._min_length:
            result.add_error(f"Content too short: {content_length} < {self._min_length}")
        elif content_length > self._max_length:
            result.add_error(f"Content too long: {content_length} > {self._max_length}")

        # 检查编码
        if self._check_encoding:
            try:
                content.encode("utf-8")
            except UnicodeEncodeError as e:
                result.add_error(f"Invalid UTF-8 encoding: {e}")

        # 检查是否只有空白
        if content.strip() == "":
            result.add_error("Content contains only whitespace")

        return result


# =============================================================================
# 文档验证器
# =============================================================================


class DocumentValidator:
    """
    文档综合验证器

    验证整个文档的所有字段
    """

    def __init__(
        self,
        required_fields: list[str] | None = None,
        url_validator: URLValidator | None = None,
        content_validator: ContentValidator | None = None,
    ):
        """
        初始化文档验证器

        Args:
            required_fields: 必填字段列表
            url_validator: URL 验证器
            content_validator: 内容验证器
        """
        self._required_fields = required_fields or REQUIRED_FIELDS
        self._url_validator = url_validator or URLValidator()
        self._content_validator = content_validator or ContentValidator()
        self._title_validator = ContentValidator(
            min_length=MIN_TITLE_LENGTH,
            max_length=MAX_TITLE_LENGTH,
        )

    def validate(self, document: dict[str, Any]) -> ValidationResult:
        """
        验证文档

        Args:
            document: 待验证的文档

        Returns:
            验证结果
        """
        result = ValidationResult()

        if not document:
            result.add_error("Document is empty")
            return result

        if not isinstance(document, dict):
            result.add_error(f"Document must be dict, got {type(document).__name__}")
            return result

        # 检查必填字段
        for _field in self._required_fields:
            if _field not in document or document[_field] is None:
                result.add_error(f"Missing required field: {_field}")
            elif isinstance(document[_field], str) and not document[_field].strip():
                result.add_error(f"Required field '{_field}' is empty")

        # 验证 URL
        if "url" in document and document["url"]:
            url_result = self._url_validator.validate(document["url"])
            result.merge(url_result)

        # 验证标题
        if "title" in document and document["title"]:
            title_result = self._title_validator.validate(document["title"])
            for error in title_result.errors:
                result.add_error(f"Title: {error}")

        # 验证内容
        if "content_text" in document and document["content_text"]:
            content_result = self._content_validator.validate(document["content_text"])
            for error in content_result.errors:
                result.add_error(f"Content: {error}")

        # 验证 news_id 格式
        if "news_id" in document and document["news_id"]:
            news_id = document["news_id"]
            if not re.match(r"^[a-zA-Z0-9_\-]+$", news_id):
                result.add_warning(f"news_id contains special characters: {news_id}")

        return result


# =============================================================================
# 便捷函数
# =============================================================================


def validate_url(url: str) -> bool:
    """
    快速验证 URL

    Args:
        url: URL 字符串

    Returns:
        是否有效
    """
    validator = URLValidator()
    return validator.validate(url).is_valid


def validate_content(content: str) -> bool:
    """
    快速验证内容

    Args:
        content: 内容字符串

    Returns:
        是否有效
    """
    validator = ContentValidator()
    return validator.validate(content).is_valid


def validate_document(document: dict[str, Any]) -> ValidationResult:
    """
    验证文档

    Args:
        document: 文档字典

    Returns:
        验证结果
    """
    validator = DocumentValidator()
    return validator.validate(document)


def is_valid_document(document: dict[str, Any]) -> bool:
    """
    快速检查文档是否有效

    Args:
        document: 文档字典

    Returns:
        是否有效
    """
    return validate_document(document).is_valid
