from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_chat_missing_authorization_returns_401(async_client):
    response = await async_client.post(
        "/api/v1/client/chat",
        json={"user_query": "hello", "stream": False, "history": []},
        headers={"X-Device-ID": "device-test-001"},
    )

    assert response.status_code == 401
    body = response.json()
    assert body["detail"] == "未提供认证信息"
