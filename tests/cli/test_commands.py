from typing import Any

import pytest
from typer.testing import CliRunner

from app.cli.app import cli
from app.cli.client import ApiClient
from app.cli.config import CliConfig


def test_client_notice_command_maps_query_parameters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_request(self: ApiClient, method: str, path: str, **kwargs: Any) -> Any:
        calls.append({"method": method, "path": path, **kwargs})
        return {"msg": "成功", "data": {}}

    monkeypatch.setattr(ApiClient, "request", fake_request)
    result = CliRunner().invoke(
        cli,
        ["--json", "client-v1", "notices", "--page", "2", "--size", "5"],
    )
    assert result.exit_code == 0
    assert calls == [
        {
            "method": "GET",
            "path": "/api/v1/client/notices",
            "admin": False,
            "params": {"label": None, "page": 2, "size": 5},
            "json_body": None,
            "form": None,
        }
    ]


def test_admin_setting_command_builds_typed_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_request(self: ApiClient, method: str, path: str, **kwargs: Any) -> Any:
        calls.append({"method": method, "path": path, **kwargs})
        return {"msg": "成功", "data": {}}

    monkeypatch.setattr(ApiClient, "request", fake_request)
    result = CliRunner().invoke(
        cli,
        [
            "--admin-token",
            "token",
            "admin-v1",
            "settings-update",
            "--setting",
            "notices_auth=1",
        ],
    )
    assert result.exit_code == 0
    assert calls[0]["admin"] is True
    assert calls[0]["json_body"] == {
        "settings": [{"key": "notices_auth", "value": "1"}]
    }


def test_api_client_builds_client_and_admin_headers() -> None:
    config = CliConfig.from_options(
        base_url="http://localhost:8000/",
        token="client-token",
        admin_token="admin-token",
        device_id="device-id",
        client_version="test",
        timeout=1.0,
        json_output=True,
    )
    client = ApiClient(config)
    assert client.config.base_url == "http://localhost:8000"
    assert client._headers(admin=False)["Authorization"] == "Bearer client-token"
    assert client._headers(admin=True)["Authorization"] == "Bearer admin-token"
    assert client._headers(admin=False)["X-Device-ID"] == "device-id"
