from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.infrastructure.storage.sqlite.sql_db_service import db


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
    return payload["data"]["token"]


@pytest.mark.asyncio
async def test_apikey_register_notices_and_semantic_search_flow(
    async_client: AsyncClient,
    admin_credentials: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin_token = await _admin_login(
        async_client,
        admin_credentials["username"],
        admin_credentials["password"],
    )
    admin_headers = {
        "Authorization": f"Bearer {admin_token}",
        "X-Client-Version": "e2e",
        "X-Request-Id": "e2e-apikey",
    }

    create_key_resp = await async_client.post(
        "/api/v1/admin/apikeys",
        headers=admin_headers,
        json={"owner_name": "smoke-user", "max_devices": 2},
    )
    assert create_key_resp.status_code == 200
    client_api_key = create_key_resp.json()["data"]["new_key"]
    assert client_api_key.startswith("sk-")

    client_headers = {
        "Authorization": f"Bearer {client_api_key}",
        "X-Device-ID": "device-e2e-1",
    }

    register_resp = await async_client.post(
        "/api/v1/client/register", headers=client_headers
    )
    assert register_resp.status_code == 200
    assert register_resp.json()["msg"] == "设备注册成功"

    db.insert_notice_from_dict(
        {
            "id": "notice-e2e-001",
            "label": "教务",
            "title": "期中考试安排通知",
            "date": "2026-04-01",
            "detail_url": "https://example.com/n1",
            "is_page": True,
            "content_text": "请同学们按时参加期中考试。",
            "attachment_urls": ["https://example.com/att1.pdf"],
        }
    )

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
        def semantic_search(
            self,
            query: str,
            field: str,
            min_similarity: float,
            top_k: int,
        ) -> dict:
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
                        "_similarity": max(min_similarity, 0.92),
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
