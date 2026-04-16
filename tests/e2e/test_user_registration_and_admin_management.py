"""E2E测试：用户注册流程和管理员管理功能"""
from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_user_registration_flow(
    async_client: AsyncClient,
    admin_credentials: dict[str, str],
) -> None:
    """测试完整的用户注册流程：
    1. 用户提交注册申请
    2. 管理员登录获取Token
    3. 管理员查看待审核列表
    4. 管理员查看详细信息
    5. 管理员批准注册申请
    6. 用户登录验证
    """
    # ==================== 步骤1：用户提交注册申请 ====================
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

    # 重复注册同一用户名应该失败
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

    # 重复注册同一邮箱应该失败
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

    # ==================== 步骤2：管理员登录获取Token ====================
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

    # ==================== 步骤3：管理员查看待审核列表 ====================
    list_resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers=admin_headers,
        params={"status": "pending", "page": 1, "size": 10},
    )
    assert list_resp.status_code == 200
    list_data = list_resp.json()
    assert list_data["msg"] == "获取成功"
    assert list_data["data"]["total"] >= 1

    # 找到刚创建的注册请求
    registration = None
    for user in list_data["data"]["users"]:
        if user["username"] == "testuser1":
            registration = user
            break
    assert registration is not None
    assert registration["email"] == "testuser1@example.com"
    assert registration["status"] == "pending"
    registration_id = registration["id"]

    # ==================== 步骤4：管理员查看详细信息 ====================
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

    # ==================== 步骤5：管理员批准注册申请 ====================
    approve_resp = await async_client.post(
        f"/api/v2/admin/user-registrations/{registration_id}/review",
        headers=admin_headers,
        json={"action": "approved", "review": "审核通过"},
    )
    assert approve_resp.status_code == 200
    assert approve_resp.json()["msg"] == "审核成功"

    # 验证注册记录已被删除
    detail_after_approve = await async_client.get(
        f"/api/v2/admin/user-registrations/{registration_id}",
        headers=admin_headers,
    )
    assert detail_after_approve.status_code == 200
    assert detail_after_approve.json()["data"] is None

    # ==================== 步骤6：用户登录验证 ====================
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
    # 用户提交注册申请
    register_resp = await async_client.post(
        "/api/v2/client/auth/register",
        json={
            "username": "testuser_reject",
            "password_hash": "hashed_password_456",
            "email": "testuser_reject@example.com",
        },
    )
    assert register_resp.status_code == 200

    # 管理员登录
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

    # 获取注册请求ID
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

    # 管理员拒绝注册申请
    reject_resp = await async_client.post(
        f"/api/v2/admin/user-registrations/{registration_id}/review",
        headers=admin_headers,
        json={"action": "rejected", "review": "资料不完整"},
    )
    assert reject_resp.status_code == 200
    assert reject_resp.json()["msg"] == "审核成功"

    # 验证状态已更新为rejected
    detail_resp = await async_client.get(
        f"/api/v2/admin/user-registrations/{registration_id}",
        headers=admin_headers,
    )
    assert detail_resp.status_code == 200
    assert detail_resp.json()["data"]["status"] == "rejected"

    # 拒绝的用户应该无法登录
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


@pytest.mark.asyncio
async def test_admin_user_management(
    async_client: AsyncClient,
    admin_credentials: dict[str, str],
) -> None:
    """测试管理员管理用户功能：
    1. 注册并批准用户
    2. 获取用户列表
    3. 按状态筛选用户
    4. 设置用户激活状态
    5. 删除用户
    """
    # 注册并批准一个用户
    register_resp = await async_client.post(
        "/api/v2/client/auth/register",
        json={
            "username": "testuser_manage",
            "password_hash": "hashed_password_789",
            "email": "testuser_manage@example.com",
        },
    )
    assert register_resp.status_code == 200

    # 管理员登录
    login_resp = await async_client.post(
        "/api/v1/admin/auth/login",
        data={
            "username": admin_credentials["username"],
            "password": admin_credentials["password"],
        },
    )
    admin_token = login_resp.json()["data"]["token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    # 获取并批准注册请求
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

    # ==================== 获取用户列表 ====================
    users_resp = await async_client.get(
        "/api/v2/admin/users",
        headers=admin_headers,
        params={"page": 1, "size": 10},
    )
    assert users_resp.status_code == 200
    users_data = users_resp.json()
    assert users_data["msg"] == "获取成功"
    assert users_data["data"]["total"] >= 1

    # 找到刚创建的用户
    user = None
    for u in users_data["data"]["users"]:
        if u["username"] == "testuser_manage":
            user = u
            break
    assert user is not None
    assert user["email"] == "testuser_manage@example.com"
    assert user["is_active"] == 1
    user_id = user["id"]

    # ==================== 按状态筛选用户 ====================
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

    # ==================== 设置用户激活状态（禁用） ====================
    deactivate_resp = await async_client.post(
        f"/api/v2/admin/users/{user_id}/status",
        headers=admin_headers,
        json={"is_active": False},
    )
    assert deactivate_resp.status_code == 200
    assert deactivate_resp.json()["msg"] == "修改成功"

    # 验证用户已被禁用
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

    # 禁用用户应该无法登录
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

    # ==================== 重新激活用户 ====================
    activate_resp = await async_client.post(
        f"/api/v2/admin/users/{user_id}/status",
        headers=admin_headers,
        json={"is_active": True},
    )
    assert activate_resp.status_code == 200
    assert activate_resp.json()["msg"] == "修改成功"

    # ==================== 删除用户 ====================
    delete_resp = await async_client.delete(
        f"/api/v2/admin/users/{user_id}",
        headers=admin_headers,
    )
    assert delete_resp.status_code == 200
    assert delete_resp.json()["msg"] == "删除成功"

    # 验证用户已被删除
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


@pytest.mark.asyncio
async def test_pagination_and_filtering(
    async_client: AsyncClient,
    admin_credentials: dict[str, str],
) -> None:
    """测试分页和状态筛选功能"""
    # 创建多个注册请求
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

    # 管理员登录
    login_resp = await async_client.post(
        "/api/v1/admin/auth/login",
        data={
            "username": admin_credentials["username"],
            "password": admin_credentials["password"],
        },
    )
    admin_token = login_resp.json()["data"]["token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    # 测试分页
    page1_resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers=admin_headers,
        params={"page": 1, "size": 2},
    )
    assert page1_resp.status_code == 200
    page1_data = page1_resp.json()
    assert page1_data["data"]["total"] >= 5
    assert len(page1_data["data"]["users"]) <= 2

    # 测试状态筛选
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
    # 不带Token访问管理员接口应该失败
    resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        params={"page": 1, "size": 10},
    )
    assert resp.status_code == 401

    # 带无效Token访问应该失败
    resp = await async_client.get(
        "/api/v2/admin/user-registrations",
        headers={"Authorization": "Bearer invalid_token"},
        params={"page": 1, "size": 10},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_client_device_token_binding(
    async_client: AsyncClient,
) -> None:
    """测试客户端Token与设备绑定功能"""
    # 注册并批准用户
    await async_client.post(
        "/api/v2/client/auth/register",
        json={
            "username": "testuser_binding",
            "password_hash": "hashed_password_bind",
            "email": "testuser_binding@example.com",
        },
    )

    # 管理员批准
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

    # 设备1登录
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

    # 用设备1的Token访问需要鉴权的接口
    auth_headers = {
        "Authorization": f"Bearer {token1}",
        "X-Device-ID": device1_id,
    }
    resp = await async_client.get(
        "/api/v1/client/notices",
        headers=auth_headers,
    )
    assert resp.status_code == 200

    # 用设备1的Token但设备ID不匹配应该失败
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
