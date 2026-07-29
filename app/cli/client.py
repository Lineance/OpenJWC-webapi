from collections.abc import Iterator, Mapping
from typing import Any
from uuid import uuid4

import httpx

from app.cli.config import CliConfig


class ApiClient:
    """以统一鉴权和错误处理访问 OpenJWC HTTP API。"""

    def __init__(self, config: CliConfig) -> None:
        self._config = config

    @property
    def config(self) -> CliConfig:
        return self._config

    def _headers(self, *, admin: bool) -> dict[str, str]:
        token = self._config.admin_token if admin else self._config.token
        headers = {
            "Accept": "application/json",
            "X-Client-Version": self._config.client_version,
            "X-Request-ID": str(uuid4()),
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if self._config.device_id:
            headers["X-Device-ID"] = self._config.device_id
        return headers

    def request(
        self,
        method: str,
        path: str,
        *,
        admin: bool = False,
        params: Mapping[str, Any] | None = None,
        json_body: Any = None,
        form: Mapping[str, Any] | None = None,
    ) -> Any:
        with httpx.Client(
            base_url=self._config.base_url,
            timeout=self._config.timeout,
        ) as client:
            response = client.request(
                method,
                path,
                headers=self._headers(admin=admin),
                params=params,
                json=json_body,
                data=form,
            )
        response.raise_for_status()
        if not response.content:
            return None
        return response.json()

    def stream(
        self,
        method: str,
        path: str,
        *,
        json_body: Any,
    ) -> Iterator[str]:
        with httpx.Client(
            base_url=self._config.base_url,
            timeout=self._config.timeout,
        ) as client:
            with client.stream(
                method,
                path,
                headers=self._headers(admin=False),
                json=json_body,
            ) as response:
                response.raise_for_status()
                yield from response.iter_lines()
