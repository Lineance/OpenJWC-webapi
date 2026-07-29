"""E2E测试：用户注册流程和管理员管理功能"""

from __future__ import annotations

import pytest

from httpx import AsyncClient

@pytest.mark.asyncio
async def test_pagination_and_filtering(
    async_client: AsyncClient,
    admin_credentials: dict[str, str],
) -> None:
    """测试分页和状态筛选功能"""

    usernames = [f"user_{i}" for i in range(5)]
    for username in usernames:
        await async_client.post(
            "/api/v2/client/auth/register",
            json={
                "username": username,
                "password_hash": "hashed_password",
                "email": f"{username}@example.com",
            },
        )

    login_resp = await async_client.post(
        "/api/v1/admin/auth/login",
        data={
            "username": admin_credentials["username"],
            "password": admin_credentials["password"],
        },
    )
    admin_token = login_resp.json()["data"]["token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    page1_resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers=admin_headers,
        params={"page": 1, "size": 2},
    )
    assert page1_resp.status_code == 200
    page1_data = page1_resp.json()
    assert page1_data["data"]["total"] >= 5
    assert len(page1_data["data"]["users"]) <= 2

    pending_resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers=admin_headers,
        params={"status": "pending", "page": 1, "size": 10},
    )
    assert pending_resp.status_code == 200
    pending_users = pending_resp.json()["data"]["users"]
    assert all(u["status"] == "pending" for u in pending_users)

@pytest.mark.asyncio
async def test_admin_token_required(
    async_client: AsyncClient,
) -> None:
    """测试管理员接口需要有效的Token"""

    resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        params={"page": 1, "size": 10},
    )
    assert resp.status_code == 401

    resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers={"Authorization": "Bearer invalid_token"},
        params={"page": 1, "size": 10},
    )
    assert resp.status_code == 401
