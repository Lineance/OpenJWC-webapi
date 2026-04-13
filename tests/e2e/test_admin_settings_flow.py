from __future__ import annotations

import pytest
from httpx import AsyncClient


async def _admin_login(
    client: AsyncClient,
    username: str,
    password: str,
) -> str:
    response = await client.post(
        "/api/v1/admin/auth/login",
        data={"username": username, "password": password},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["msg"] == "登录成功"
    token = payload["data"]["token"]
    assert token
    return token


@pytest.mark.asyncio
async def test_admin_login_settings_update_and_effect(
    async_client: AsyncClient,
    admin_credentials: dict[str, str],
) -> None:
    token = await _admin_login(
        async_client,
        admin_credentials["username"],
        admin_credentials["password"],
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Client-Version": "e2e",
        "X-Request-Id": "e2e-admin-settings",
    }

    before_resp = await async_client.get("/api/v1/admin/settings", headers=headers)
    assert before_resp.status_code == 200
    before_payload = before_resp.json()
    settings_before = {
        item["key"]: item["value"] for item in before_payload["data"]["settings"]
    }
    assert "notices_auth" in settings_before

    update_resp = await async_client.put(
        "/api/v1/admin/settings",
        headers=headers,
        json={"settings": [{"key": "notices_auth", "value": "1"}]},
    )
    assert update_resp.status_code == 200
    assert "notices_auth" in update_resp.json()["msg"]

    after_resp = await async_client.get("/api/v1/admin/settings", headers=headers)
    assert after_resp.status_code == 200
    after_payload = after_resp.json()
    settings_after = {
        item["key"]: item["value"] for item in after_payload["data"]["settings"]
    }
    assert settings_after["notices_auth"] == "1"
