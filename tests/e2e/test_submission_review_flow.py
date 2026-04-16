from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.infrastructure.storage.lancedb import ArticleFields, get_article_repository
from app.infrastructure.storage.lancedb.schema import (
    CONTENT_EMBEDDING_DIM,
    TITLE_EMBEDDING_DIM,
)


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


async def _register_and_login_client(
    client: AsyncClient,
    username: str,
    password_hash: str,
    device_id: str,
) -> str:
    register_resp = await client.post(
        "/api/v2/client/auth/register",
        json={
            "username": username,
            "password_hash": password_hash,
            "email": f"{username}@example.com",
        },
    )
    assert register_resp.status_code == 200
    assert register_resp.json()["msg"] == "注册成功"

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
async def test_submission_review_and_notice_visibility_flow(
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
        "X-Request-Id": "e2e-submission",
    }

    create_key_resp = await async_client.post(
        "/api/v1/admin/apikeys",
        headers=admin_headers,
        json={"owner_name": "submitter", "max_devices": 1},
    )
    assert create_key_resp.status_code == 200
    device_id = "device-e2e-submit-1"
    client_token = await _register_and_login_client(
        async_client,
        username="e2e_client_submit",
        password_hash="hash-e2e-submit",
        device_id=device_id,
    )

    client_headers = {
        "Authorization": f"Bearer {client_token}",
        "X-Device-ID": device_id,
    }

    register_resp = await async_client.post(
        "/api/v1/client/register", headers=client_headers
    )
    assert register_resp.status_code == 200

    submit_resp = await async_client.post(
        "/api/v1/client/submissions",
        headers=client_headers,
        json={
            "label": "教务",
            "title": "关于实验课调课的通知",
            "date": "2026-04-10",
            "detail_url": "https://example.com/submission",
            "is_page": True,
            "content": {
                "text": "本周实验课调整到周四晚上，请同学关注。",
                "attachment_urls": ["https://example.com/a1.pdf"],
            },
        },
    )
    assert submit_resp.status_code == 200
    assert submit_resp.json()["msg"] == "提交成功"

    admin_list_resp = await async_client.get(
        "/api/v1/admin/submissions",
        headers=admin_headers,
        params={"page": 1, "size": 20, "status": "pending"},
    )
    assert admin_list_resp.status_code == 200
    pending_items = admin_list_resp.json()["data"]["notices"]
    assert pending_items

    target = next(
        item for item in pending_items if item["title"] == "关于实验课调课的通知"
    )
    submission_id = target["id"]

    import app.application.submission.submission_service as submission_service

    class DummyPipelineResult:
        status = "success"
        message = "ok"

    class DummyPipeline:
        def process_one(self, doc):
            payload = dict(doc)
            payload[ArticleFields.TITLE_EMBEDDING] = [0.0] * TITLE_EMBEDDING_DIM
            payload[ArticleFields.CONTENT_EMBEDDING] = [0.0] * CONTENT_EMBEDDING_DIM
            get_article_repository().add_one(payload)
            return DummyPipelineResult()

    monkeypatch.setattr(submission_service, "submission_pipeline", DummyPipeline())

    review_resp = await async_client.post(
        f"/api/v1/admin/submissions/{submission_id}/review",
        headers=admin_headers,
        json={"action": "approved", "review": "内容属实"},
    )
    assert review_resp.status_code == 200
    assert review_resp.json()["msg"] == "修改成功"

    my_submissions_resp = await async_client.get(
        "/api/v1/client/submissions/my",
        headers=client_headers,
    )
    assert my_submissions_resp.status_code == 200
    my_items = my_submissions_resp.json()["data"]["notices"]
    assert any(
        item["id"] == submission_id and item["status"] == "approved"
        for item in my_items
    )

    notices_resp = await async_client.get(
        "/api/v1/client/notices", headers=client_headers
    )
    assert notices_resp.status_code == 200
    notices = notices_resp.json()["data"]["notices"]
    assert any(item["id"] == submission_id for item in notices)
