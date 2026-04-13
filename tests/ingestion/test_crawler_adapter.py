"""CrawlerAdapter 字段对齐测试"""

from app.infrastructure.ingestion.adapters.crawler import CrawlerAdapter
from app.infrastructure.ingestion.validators import DocumentValidator
from app.infrastructure.storage.lancedb import ArticleFields


def test_convert_rust_crawler_payload_alignment() -> None:
    adapter = CrawlerAdapter()
    validator = DocumentValidator()

    rust_payload = {
        "id": "abc123",
        "label": "最新动态",
        "title": "测试标题",
        "date": "2026-04-02",
        "detail_url": "https://jwc.seu.edu.cn/2026/0402/c21676a000001/page.htm",
        "is_page": True,
        "content": {
            "text": "这是一段来自 Rust 爬虫的正文。",
            "attachment_urls": [
                "https://jwc.seu.edu.cn/_upload/article/files/test.pdf",
            ],
        },
    }

    converted = adapter.convert_one(rust_payload)

    assert converted[ArticleFields.NEWS_ID] == "abc123"
    assert converted[ArticleFields.URL] == rust_payload["detail_url"]
    assert converted[ArticleFields.PUBLISH_DATE] is not None
    assert converted[ArticleFields.CONTENT_TEXT].startswith("这是一段")
    assert (
        converted[ArticleFields.ATTACHMENTS]
        == rust_payload["content"]["attachment_urls"]
    )
    assert "最新动态" in converted[ArticleFields.TAGS]

    validation = validator.validate(converted)
    assert validation.is_valid is True


def test_convert_python_crawler_payload_keeps_existing_fields() -> None:
    adapter = CrawlerAdapter()
    validator = DocumentValidator()

    python_payload = {
        "success": True,
        "url": "https://jwc.seu.edu.cn/2026/0228/c21678a556262/page.htm",
        "title": "Python 爬虫标题",
        "publish_date": "2026-02-28",
        "author": "教务处",
        "content": "这是一段长度足够的正文内容，用于通过最小长度校验。",
        "markdown": "# Python 爬虫标题\n\n这是一段长度足够的正文内容，用于通过最小长度校验。",
        "source": "教务处网站",
        "metadata": {"word_count": 123},
    }

    converted = adapter.convert_one(python_payload)

    assert converted[ArticleFields.URL] == python_payload["url"]
    assert converted[ArticleFields.SOURCE_SITE] == python_payload["source"]
    assert converted[ArticleFields.CONTENT_MARKDOWN].startswith("# Python")
    assert isinstance(converted[ArticleFields.METADATA], dict)
    assert converted[ArticleFields.METADATA]["word_count"] == 123

    validation = validator.validate(converted)
    assert validation.is_valid is True
