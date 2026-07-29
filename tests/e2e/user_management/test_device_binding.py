"""E2E测试：用户注册流程和管理员管理功能"""

from __future__ import annotations

import pytest

from httpx import AsyncClient

@pytest.mark.asyncio
async def test_client_device_token_binding(
    async_client: AsyncClient,
) -> None:
    """测试客户端Token与设备绑定功能"""

    await async_client.post(
        "/api/v2/client/auth/register",
        json={
            "username": "testuser_binding",
            "password_hash": "hashed_password_bind",
            "email": "testuser_binding@example.com",
        },
    )

    admin_login = await async_client.post(
        "/api/v1/admin/auth/login",
        data={"username": "e2e_admin", "password": "E2E@12345"},
    )
    admin_token = admin_login.json()["data"]["token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    list_resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers=admin_headers,
        params={"status": "pending", "page": 1, "size": 10},
    )
    registration = None
    for user in list_resp.json()["data"]["users"]:
        if user["username"] == "testuser_binding":
            registration = user
            break
    assert registration is not None

    await async_client.post(
        f"/api/v2/admin/user-registrations/{registration['id']}/review",
        headers=admin_headers,
        json={"action": "approved", "review": "批准"},
    )

    device1_id = "device-binding-1"
    login1_resp = await async_client.post(
        "/api/v2/client/auth/login",
        headers={"X-Device-ID": device1_id},
        json={
            "account": "testuser_binding",
            "password_hash": "hashed_password_bind",
            "device_name": "device-1",
        },
    )
    assert login1_resp.status_code == 200
    token1 = login1_resp.json()["data"]["token"]

    auth_headers = {
        "Authorization": f"Bearer {token1}",
        "X-Device-ID": device1_id,
    }
    resp = await async_client.get(
        "/api/v1/client/notices",
        headers=auth_headers,
    )
    assert resp.status_code == 200

    bad_headers = {
        "Authorization": f"Bearer {token1}",
        "X-Device-ID": "different-device-id",
    }
    resp = await async_client.get(
        "/api/v1/client/notices",
        headers=bad_headers,
    )
    assert resp.status_code == 401
    assert "设备ID不匹配" in resp.json()["detail"]
