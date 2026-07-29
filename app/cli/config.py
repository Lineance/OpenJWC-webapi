from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True, slots=True)
class CliConfig:
    """保存一次 CLI 调用所需的连接配置。"""

    base_url: str
    token: str | None
    admin_token: str | None
    device_id: str | None
    client_version: str
    timeout: float
    json_output: bool

    @classmethod
    def from_options(
        cls,
        *,
        base_url: str,
        token: str | None,
        admin_token: str | None,
        device_id: str | None,
        client_version: str,
        timeout: float,
        json_output: bool,
    ) -> Self:
        return cls(
            base_url=base_url.rstrip("/"),
            token=token,
            admin_token=admin_token,
            device_id=device_id,
            client_version=client_version,
            timeout=timeout,
            json_output=json_output,
        )
