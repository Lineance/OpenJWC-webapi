from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.infrastructure.storage.lancedb import ArticleFields, get_article_repository
from app.infrastructure.storage.lancedb.schema import (
    CONTENT_EMBEDDING_DIM,
    TITLE_EMBEDDING_DIM,
)
from app.infrastructure.storage.sqlite.notice_repository import get_notice_repository


async def _register_and_login_client(
    client: AsyncClient,
    username: str,
    password_hash: str,
    device_id: str,
    admin_token: str | None = None,
) -> str:
    """注册并登录客户端用户（需要管理员审核）

    如果提供了admin_token，会自动进行管理员审核流程
    """
    # 提交注册申请
    register_resp = await client.post(
        "/api/v2/client/auth/register",
        json={
            "username": username,
            "password_hash": password_hash,
            "email": f"{username}@example.com",
        },
    )
    assert register_resp.status_code == 200
    assert "等待管理员审核" in register_resp.json()["msg"]

    # 如果提供了管理员token，进行审核
    if admin_token:
        # 获取待审核的注册请求
        list_resp = await client.get(
            "/api/v2/admin/user-registrations",
            headers={"Authorization": f"Bearer {admin_token}"},
            params={"status": "pending", "page": 1, "size": 10},
        )
        assert list_resp.status_code == 200

        # 找到对应的注册请求
        registration = None
        for user in list_resp.json()["data"]["users"]:
            if user["username"] == username:
                registration = user
                break
        assert registration is not None

        # 批准注册申请
        approve_resp = await client.post(
            f"/api/v2/admin/user-registrations/{registration['id']}/review",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"action": "approved", "review": "E2E测试自动批准"},
        )
        assert approve_resp.status_code == 200

    # 登录
    login_resp = await client.post(
        "/api/v2/client/auth/login",
        headers={"X-Device-ID": device_id},
        json={
            "account": username,
            "password_hash": password_hash,
            "device_name": "e2e-device",
        },
    )
    assert login_resp.status_code == 200
    assert login_resp.json()["msg"] == "登录成功"
    return login_resp.json()["data"]["token"]


@pytest.mark.asyncio
async def test_apikey_register_notices_and_semantic_search_flow(
    async_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    admin_credentials: dict[str, str],
) -> None:
    device_id = "device-e2e-1"

    # 获取管理员token用于审核
    admin_login_resp = await async_client.post(
        "/api/v1/admin/auth/login",
        data={
            "username": admin_credentials["username"],
            "password": admin_credentials["password"],
        },
    )
    assert admin_login_resp.status_code == 200
    admin_token = admin_login_resp.json()["data"]["token"]

    client_token = await _register_and_login_client(
        async_client,
        username="e2e_client_search",
        password_hash="hash-e2e-client",
        device_id=device_id,
        admin_token=admin_token,
    )

    client_headers = {
        "Authorization": f"Bearer {client_token}",
        "X-Device-ID": device_id,
    }

    register_resp = await async_client.post(
        "/api/v1/client/register", headers=client_headers
    )
    assert register_resp.status_code == 200
    assert register_resp.json()["msg"] == "设备注册成功"

    article_repo = get_article_repository()
    seeded_doc = {
        ArticleFields.NEWS_ID: "notice-e2e-001",
        ArticleFields.TITLE: "期中考试安排通知",
        ArticleFields.PUBLISH_DATE: "2026-04-01",
        ArticleFields.URL: "https://example.com/n1",
        ArticleFields.SOURCE_SITE: "教务",
        ArticleFields.TAGS: ["教务"],
        ArticleFields.CONTENT_TEXT: "请同学们按时参加期中考试。",
        ArticleFields.CONTENT_MARKDOWN: "请同学们按时参加期中考试。",
        ArticleFields.CRAWL_VERSION: 1,
        ArticleFields.TITLE_EMBEDDING: [0.0] * TITLE_EMBEDDING_DIM,
        ArticleFields.CONTENT_EMBEDDING: [0.0] * CONTENT_EMBEDDING_DIM,
        ArticleFields.METADATA: {
            "label": "教务",
            "detail_url": "https://example.com/n1",
            "is_page": True,
        },
        ArticleFields.ATTACHMENTS: ["https://example.com/att1.pdf"],
    }
    article_repo.add_one(seeded_doc)
    get_notice_repository().upsert_from_article(seeded_doc)

    notices_resp = await async_client.get(
        "/api/v1/client/notices",
        headers=client_headers,
        params={"page": 1, "size": 20},
    )
    assert notices_resp.status_code == 200
    notices_payload = notices_resp.json()
    assert notices_payload["msg"] == "获取成功"
    assert notices_payload["data"]["total_returned"] >= 1

    import app.api.v1.client.search as search_api

    class FakeRetrievalEngine:
        def search(self, query: str, mode: str, top_k: int) -> dict:
            return {
                "results": [
                    {
                        "news_id": "notice-e2e-001",
                        "title": "期中考试安排通知",
                        "publish_date": "2026-04-01",
                        "metadata": {
                            "detail_url": "https://example.com/n1",
                            "is_page": True,
                        },
                        "tags": ["教务"],
                        "_similarity": 0.92,
                    }
                ]
            }

    monkeypatch.setattr(search_api, "retrieval_engine", FakeRetrievalEngine())

    search_resp = await async_client.post(
        "/api/v1/client/notices/search",
        headers=client_headers,
        json={"query": "期中考试", "top_k": 5},
    )
    assert search_resp.status_code == 200
    search_payload = search_resp.json()
    assert search_payload["msg"] == "搜索成功"
    assert search_payload["data"]["total_found"] == 1
    first_result = search_payload["data"]["results"][0]
    assert first_result["id"] == "notice-e2e-001"
    assert first_result["title"] == "期中考试安排通知"
    assert first_result["similarity_score"] >= 0.3
