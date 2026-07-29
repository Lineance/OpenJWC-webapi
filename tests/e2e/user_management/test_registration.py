"""E2E测试：用户注册流程和管理员管理功能"""

from __future__ import annotations

import pytest

from httpx import AsyncClient

@pytest.mark.asyncio
async def test_user_registration_flow(
    async_client: AsyncClient,
    admin_credentials: dict[str, str],
) -> None:
    """测试完整的用户注册流程："""

    register_resp = await async_client.post(
        "/api/v2/client/auth/register",
        json={
            "username": "testuser1",
            "password_hash": "hashed_password_123",
            "email": "testuser1@example.com",
        },
    )
    assert register_resp.status_code == 200
    assert "等待管理员审核" in register_resp.json()["msg"]

    duplicate_resp = await async_client.post(
        "/api/v2/client/auth/register",
        json={
            "username": "testuser1",
            "password_hash": "another_hash",
            "email": "another@example.com",
        },
    )
    assert duplicate_resp.status_code == 409
    assert "用户名已存在" in duplicate_resp.json()["detail"]

    duplicate_email_resp = await async_client.post(
        "/api/v2/client/auth/register",
        json={
            "username": "testuser2",
            "password_hash": "another_hash",
            "email": "testuser1@example.com",
        },
    )
    assert duplicate_email_resp.status_code == 409
    assert "邮箱已被注册" in duplicate_email_resp.json()["detail"]

    login_resp = await async_client.post(
        "/api/v1/admin/auth/login",
        data={
            "username": admin_credentials["username"],
            "password": admin_credentials["password"],
        },
    )
    assert login_resp.status_code == 200
    admin_token = login_resp.json()["data"]["token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    list_resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers=admin_headers,
        params={"status": "pending", "page": 1, "size": 10},
    )
    assert list_resp.status_code == 200
    list_data = list_resp.json()
    assert list_data["msg"] == "获取成功"
    assert list_data["data"]["total"] >= 1

    registration = None
    for user in list_data["data"]["users"]:
        if user["username"] == "testuser1":
            registration = user
            break
    assert registration is not None
    assert registration["email"] == "testuser1@example.com"
    assert registration["status"] == "pending"
    registration_id = registration["id"]

    detail_resp = await async_client.get(
        f"/api/v2/admin/user-registrations/{registration_id}",
        headers=admin_headers,
    )
    assert detail_resp.status_code == 200
    detail_data = detail_resp.json()
    assert detail_data["msg"] == "获取成功"
    assert detail_data["data"]["username"] == "testuser1"
    assert detail_data["data"]["email"] == "testuser1@example.com"
    assert detail_data["data"]["status"] == "pending"

    approve_resp = await async_client.post(
        f"/api/v2/admin/user-registrations/{registration_id}/review",
        headers=admin_headers,
        json={"action": "approved", "review": "审核通过"},
    )
    assert approve_resp.status_code == 200
    assert approve_resp.json()["msg"] == "审核成功"

    detail_after_approve = await async_client.get(
        f"/api/v2/admin/user-registrations/{registration_id}",
        headers=admin_headers,
    )
    assert detail_after_approve.status_code == 200
    assert detail_after_approve.json()["data"] is None

    device_id = "device-test-1"
    login_resp = await async_client.post(
        "/api/v2/client/auth/login",
        headers={"X-Device-ID": device_id},
        json={
            "account": "testuser1",
            "password_hash": "hashed_password_123",
            "device_name": "test-device",
        },
    )
    assert login_resp.status_code == 200
    login_data = login_resp.json()
    assert login_data["msg"] == "登录成功"
    assert "token" in login_data["data"]
    assert login_data["data"]["username"] == "testuser1"
    assert login_data["data"]["email"] == "testuser1@example.com"

@pytest.mark.asyncio
async def test_user_registration_rejection(
    async_client: AsyncClient,
    admin_credentials: dict[str, str],
) -> None:
    """测试管理员拒绝注册申请流程"""

    register_resp = await async_client.post(
        "/api/v2/client/auth/register",
        json={
            "username": "testuser_reject",
            "password_hash": "hashed_password_456",
            "email": "testuser_reject@example.com",
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
    assert login_resp.status_code == 200
    admin_token = login_resp.json()["data"]["token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    list_resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers=admin_headers,
        params={"status": "pending", "page": 1, "size": 10},
    )
    registration = None
    for user in list_resp.json()["data"]["users"]:
        if user["username"] == "testuser_reject":
            registration = user
            break
    assert registration is not None
    registration_id = registration["id"]

    reject_resp = await async_client.post(
        f"/api/v2/admin/user-registrations/{registration_id}/review",
        headers=admin_headers,
        json={"action": "rejected", "review": "资料不完整"},
    )
    assert reject_resp.status_code == 200
    assert reject_resp.json()["msg"] == "审核成功"

    detail_resp = await async_client.get(
        f"/api/v2/admin/user-registrations/{registration_id}",
        headers=admin_headers,
    )
    assert detail_resp.status_code == 200
    assert detail_resp.json()["data"]["status"] == "rejected"

    device_id = "device-test-reject"
    login_resp = await async_client.post(
        "/api/v2/client/auth/login",
        headers={"X-Device-ID": device_id},
        json={
            "account": "testuser_reject",
            "password_hash": "hashed_password_456",
            "device_name": "test-device",
        },
    )
    assert login_resp.status_code == 401
    assert "账号或密码错误" in login_resp.json()["detail"]
