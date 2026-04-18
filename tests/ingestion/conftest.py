"""Ingestion Tests Configuration"""

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

# 确保项目根路径可用
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


@pytest.fixture
def mock_embedder():
    """模拟 Embedder"""
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.ingestion.pipeline.get_embedder",
            lambda: MagicMock(),
        )
        yield


@pytest.fixture
def mock_repository():
    """模拟 ArticleRepository"""
    mock = MagicMock()
    mock.add_one.return_value = True
    mock.add.return_value = True
    return mock


@pytest.fixture
def mock_validator():
    """模拟 DocumentValidator"""
    mock = MagicMock()
    result = MagicMock()
    result.is_valid = True
    result.errors = []
    mock.validate.return_value = result
    return mock


@pytest.fixture
def mock_tag_matcher():
    """模拟 TagMatcher"""
    mock = MagicMock()
    mock.match_tags.return_value = ["标签1", "标签2"]
    return mock


@pytest.fixture
def sample_raw_document() -> dict[str, Any]:
    """示例原始文档"""
    return {
        "news_id": "test_doc_001",
        "title": "测试文档标题",
        "url": "https://example.com/test-doc",
        "content_markdown": "# 测试标题\n\n这是测试内容。",
        "content_text": "测试文档标题 测试内容",
        "publish_date": "2024-05-20T10:00:00Z",
        "source_site": "测试站点",
        "author": "测试作者",
        "tags": ["初始标签"],
        "metadata": {"key": "value"},
    }


@pytest.fixture
def sample_normalized_document() -> dict[str, Any]:
    """示例标准化后的文档"""
    from datetime import datetime, timezone

    return {
        "news_id": "test_doc_001",
        "title": "测试文档标题",
        "url": "https://example.com/test-doc",
        "publish_date": datetime(2024, 5, 20, 10, 0, 0, tzinfo=timezone.utc),
        "source_site": "测试站点",
        "author": "测试作者",
        "tags": ["初始标签"],
        "content_markdown": "# 测试标题\n\n这是测试内容。",
        "content_text": "测试文档标题 测试内容",
        "crawl_version": 1,
        "last_updated": datetime.now(timezone.utc),
        "metadata": '{"key": "value"}',
    }


