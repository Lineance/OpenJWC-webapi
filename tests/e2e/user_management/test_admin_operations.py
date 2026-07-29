"""E2E测试：用户注册流程和管理员管理功能"""

from __future__ import annotations

import pytest

from httpx import AsyncClient

@pytest.mark.asyncio
async def test_admin_user_management(
    async_client: AsyncClient,
    admin_credentials: dict[str, str],
) -> None:
    """测试管理员管理用户功能："""

    register_resp = await async_client.post(
        "/api/v2/client/auth/register",
        json={
            "username": "testuser_manage",
            "password_hash": "hashed_password_789",
            "email": "testuser_manage@example.com",
        },
    )
    assert register_resp.status_code == 200

    login_resp = await async_client.post(
        "/api/v1/admin/auth/login",
        data={
            "username": admin_credentials["username"],
            "password": admin_credentials["password"],
        },
    )
    admin_token = login_resp.json()["data"]["token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    list_resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers=admin_headers,
        params={"status": "pending", "page": 1, "size": 10},
    )
    registration = None
    for user in list_resp.json()["data"]["users"]:
        if user["username"] == "testuser_manage":
            registration = user
            break
    assert registration is not None

    await async_client.post(
        f"/api/v2/admin/user-registrations/{registration['id']}/review",
        headers=admin_headers,
        json={"action": "approved", "review": "批准"},
    )

    users_resp = await async_client.get(
        "/api/v2/admin/users",
        headers=admin_headers,
        params={"page": 1, "size": 10},
    )
    assert users_resp.status_code == 200
    users_data = users_resp.json()
    assert users_data["msg"] == "获取成功"
    assert users_data["data"]["total"] >= 1

    user = None
    for u in users_data["data"]["users"]:
        if u["username"] == "testuser_manage":
            user = u
            break
    assert user is not None
    assert user["email"] == "testuser_manage@example.com"
    assert user["is_active"] == 1
    user_id = user["id"]

    active_users_resp = await async_client.get(
        "/api/v2/admin/users",
        headers=admin_headers,
        params={"page": 1, "size": 10, "is_active": True},
    )
    assert active_users_resp.status_code == 200
    active_users = [
        u for u in active_users_resp.json()["data"]["users"]
        if u["username"] == "testuser_manage"
    ]
    assert len(active_users) == 1

    deactivate_resp = await async_client.post(
        f"/api/v2/admin/users/{user_id}/status",
        headers=admin_headers,
        json={"is_active": False},
    )
    assert deactivate_resp.status_code == 200
    assert deactivate_resp.json()["msg"] == "修改成功"

    user_detail_resp = await async_client.get(
        "/api/v2/admin/users",
        headers=admin_headers,
        params={"page": 1, "size": 10, "is_active": False},
    )
    inactive_users = [
        u for u in user_detail_resp.json()["data"]["users"]
        if u["username"] == "testuser_manage"
    ]
    assert len(inactive_users) == 1

    device_id = "device-test-inactive"
    login_resp = await async_client.post(
        "/api/v2/client/auth/login",
        headers={"X-Device-ID": device_id},
        json={
            "account": "testuser_manage",
            "password_hash": "hashed_password_789",
            "device_name": "test-device",
        },
    )
    assert login_resp.status_code == 401
    assert "账号或密码错误" in login_resp.json()["detail"]

    activate_resp = await async_client.post(
        f"/api/v2/admin/users/{user_id}/status",
        headers=admin_headers,
        json={"is_active": True},
    )
    assert activate_resp.status_code == 200
    assert activate_resp.json()["msg"] == "修改成功"

    delete_resp = await async_client.delete(
        f"/api/v2/admin/users/{user_id}",
        headers=admin_headers,
    )
    assert delete_resp.status_code == 200
    assert delete_resp.json()["msg"] == "删除成功"

    users_after_delete = await async_client.get(
        "/api/v2/admin/users",
        headers=admin_headers,
        params={"page": 1, "size": 10},
    )
    deleted_users = [
        u for u in users_after_delete.json()["data"]["users"]
        if u["username"] == "testuser_manage"
    ]
    assert len(deleted_users) == 0
