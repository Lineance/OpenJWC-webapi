"""Retrieval Tests Configuration"""

import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator
from unittest.mock import MagicMock

import pytest

# 确保项目根路径可用
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


# =============================================================================
# Fixtures: 临时目录和数据库
# =============================================================================


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """创建临时目录，测试后自动清理"""
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_db_path(temp_dir: str) -> str:
    """创建临时数据库路径"""
    return str(Path(temp_dir) / "test_retrieval_db.lance")


# =============================================================================
# Fixtures: Mock 外部依赖
# =============================================================================


@pytest.fixture
def mock_embedder() -> MagicMock:
    """模拟 RetrievalEmbedder"""
    mock = MagicMock()
    mock.embed_query.return_value = ([0.1] * 384, [0.1] * 1024)
    mock.embed_titles.return_value = [[0.1] * 384]
    mock.embed_contents.return_value = [[0.1] * 1024]
    return mock


# =============================================================================
# Fixtures: 测试数据
# =============================================================================


def _make_sample_article(news_id: str, title: str, content: str, source_site: str = "测试站点") -> dict[str, Any]:
    """创建示例文章数据（含 embeddings 和 last_updated）"""
    return {
        "news_id": news_id,
        "title": title,
        "url": f"https://example.com/{news_id}",
        "content_markdown": f"# {title}\n\n{content}",
        "content_text": content,
        "publish_date": datetime(2026, 3, 19, 10, 0, 0, tzinfo=timezone.utc),
        "source_site": source_site,
        "author": "测试作者",
        "tags": ["测试标签"],
        "crawl_version": 1,
        "last_updated": datetime.now(timezone.utc),
        "title_embedding": [0.1] * 384,
        "content_embedding": [0.1] * 1024,
        "metadata": "{}",
    }


@pytest.fixture
def sample_article() -> dict[str, Any]:
    """示例文章数据"""
    return _make_sample_article(
        "test_article_001",
        "东南大学计算机学院",
        "东南大学 计算机学院 是 一个 学院",
    )


@pytest.fixture
def sample_articles() -> list[dict[str, Any]]:
    """批量示例文章数据"""
    return [
        _make_sample_article(
            f"test_article_{i:03d}",
            f"测试文章标题 {i}",
            f"测试 内容 段落 {i}",
            "jwc" if i % 2 == 0 else "news",
        )
        for i in range(1, 6)
    ]


