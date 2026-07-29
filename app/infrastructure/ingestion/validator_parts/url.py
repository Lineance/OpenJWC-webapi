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
    ValidationResult,
    dataclass,
    field,
    logger,
    logging,
    re,
    urlparse,
)

class URLValidator:
    """URL 格式验证器"""

    def __init__(
        self,
        allowed_schemes: frozenset[str] = ALLOWED_SCHEMES,
        allowed_domains: frozenset[str] | None = None,
        require_domain_whitelist: bool = False,
    ) -> None:
        """初始化 URL 验证器"""
        self._allowed_schemes = allowed_schemes
        self._allowed_domains = allowed_domains or ALLOWED_DOMAINS
        self._require_domain_whitelist = require_domain_whitelist

    def validate(self, url: str) -> ValidationResult:
        """验证 URL"""
        result = ValidationResult()

        if not url:
            result.add_error("URL is empty")
            return result

        if not isinstance(url, str):
            result.add_error(f"URL must be string, got {type(url).__name__}")
            return result

        try:
            parsed = urlparse(url)
        except Exception as e:
            result.add_error(f"Invalid URL format: {e}")
            return result

        if parsed.scheme not in self._allowed_schemes:
            result.add_error(
                f"Invalid scheme '{parsed.scheme}', allowed: {', '.join(self._allowed_schemes)}"
            )

        if not parsed.netloc:
            result.add_error("URL must have a domain")
        elif self._require_domain_whitelist:
            domain = parsed.netloc.lower()
            if not any(domain.endswith(d) for d in self._allowed_domains):
                result.add_error(
                    f"Domain '{domain}' not in whitelist: {', '.join(self._allowed_domains)}"
                )

        return result
