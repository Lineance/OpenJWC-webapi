"""URL fetch tool with domain allowlist and retry."""

from typing import Any
from urllib.parse import urlparse

import httpx

from .protocol import ToolResult


class FetchTool:
    name = "web_url_fetch"
    description = "Fetch webpage snippet for fact-checking on trusted domains."

    def __init__(
        self,
        *,
        allowed_domains: list[str],
        timeout_seconds: float = 10.0,
        retries: int = 1,
    ) -> None:
        self._allowed_domains = allowed_domains
        self._timeout = timeout_seconds
        self._retries = retries

    def _is_allowed(self, target_url: str) -> bool:
        host = (urlparse(target_url).hostname or "").lower()
        return any(
            host == domain or host.endswith(f".{domain}") for domain in self._allowed_domains
        )

    async def run(self, **kwargs: Any) -> ToolResult:
        url = str(kwargs.get("url", "")).strip()
        if not url:
            return ToolResult(ok=False, content={}, error="url is required")
        if not self._is_allowed(url):
            return ToolResult(ok=False, content={}, error="domain is not allowed")

        last_error = ""
        for _ in range(self._retries + 1):
            try:
                async with httpx.AsyncClient(
                    timeout=self._timeout, follow_redirects=True
                ) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    text = response.text
                    snippet = " ".join(text.split())[:1200]
                    return ToolResult(
                        ok=True,
                        content={
                            "url": str(response.url),
                            "status": response.status_code,
                            "snippet": snippet,
                        },
                    )
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)

        return ToolResult(ok=False, content={}, error=last_error or "fetch failed")
