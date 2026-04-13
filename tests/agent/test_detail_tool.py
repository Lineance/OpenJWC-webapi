import pytest
from app.infrastructure.agent.tools.detail import DetailTool


class FakeRepo:
    def __init__(self, record: dict | None) -> None:
        self._record = record

    def get(self, news_id: str):
        if self._record and self._record.get("news_id") == news_id:
            return self._record
        return None


@pytest.mark.asyncio
async def test_detail_tool_returns_article_payload() -> None:
    repo = FakeRepo(
        {
            "news_id": "20240408_test",
            "title": "测试通知",
            "publish_date": "2026-04-08",
            "url": "https://example.com/article",
            "source_site": "jwc",
            "author": "管理员",
            "tags": ["通知"],
            "attachments": ["a.pdf"],
            "content_markdown": "# 标题\n正文内容",
            "content_text": "正文内容",
            "metadata": '{"dept":"jwc"}',
        }
    )
    tool = DetailTool(repo, content_chars=1000)

    result = await tool.run(news_id="20240408_test")

    assert result.ok is True
    assert result.content["news_id"] == "20240408_test"
    assert result.content["title"] == "测试通知"
    assert result.content["url"] == "https://example.com/article"
    assert result.content["tags"] == ["通知"]
    assert result.content["attachments"] == ["a.pdf"]
    assert result.content["metadata"]["dept"] == "jwc"
    assert result.content["content_text"] == "正文内容"


@pytest.mark.asyncio
async def test_detail_tool_requires_news_id() -> None:
    tool = DetailTool(FakeRepo(None))

    result = await tool.run()

    assert result.ok is False
    assert "news_id is required" in (result.error or "")
