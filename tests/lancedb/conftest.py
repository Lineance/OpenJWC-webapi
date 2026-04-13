"""
测试配置和共享 Fixtures

提供所有测试共享的配置、fixtures 和工具函数。
"""

import shutil

# 添加项目根目录到 Python 路径
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# =============================================================================
# Fixtures: 临时目录和数据库
# =============================================================================


@pytest.fixture
def temp_dir() -> Generator[str]:
    """
    创建临时目录，测试后自动清理

    Yields:
        临时目录路径
    """
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_db_path(temp_dir: str) -> str:
    """
    创建临时数据库路径

    Args:
        temp_dir: 临时目录

    Returns:
        临时数据库路径
    """
    return str(Path(temp_dir) / "test_db.lance")


# =============================================================================
# Fixtures: Mock 外部依赖
# =============================================================================


@pytest.fixture
def mock_embedder() -> Generator[Mock]:
    """
    模拟嵌入器，避免实际调用嵌入模型

    Yields:
        模拟的嵌入器对象
    """
    with patch("app.infrastructure.ingestion.embedder.SentenceTransformer") as mock_transformer:
        mock_instance = Mock()
        mock_instance.encode.return_value = [[0.1] * 384]
        mock_transformer.return_value = mock_instance

        with patch("app.infrastructure.ingestion.embedder.QuantizedEmbedder") as mock_quantized:
            mock_quantized_instance = Mock()
            mock_quantized_instance.embed_contents.return_value = [[0.1] * 1024]
            mock_quantized.return_value = mock_quantized_instance

            yield mock_transformer


@pytest.fixture
def mock_sentence_transformer() -> Generator[Mock]:
    """
    模拟 SentenceTransformer 模型

    Yields:
        模拟的 SentenceTransformer
    """
    with patch("sentence_transformers.SentenceTransformer") as mock_st:
        mock_instance = Mock()
        mock_instance.encode.return_value = [[0.1] * 384]
        mock_st.return_value = mock_instance
        yield mock_st


# =============================================================================
# Fixtures: 测试数据
# =============================================================================


@pytest.fixture
def sample_article_data() -> dict[str, Any]:
    """示例文章数据"""
    return {
        "news_id": "test_article_001",
        "title": "测试文章标题",
        "url": "https://example.com/test-article",
        "content_markdown": "# 测试标题\n\n测试内容段落。",
        "content_text": "测试标题 测试内容段落。",
        "publish_date": "2026-03-19T10:00:00Z",
        "source_site": "测试站点",
        "author": "测试作者",
        "tags": ["测试标签1", "测试标签2"],
        "crawl_version": 1,
        "metadata": {"test_key": "test_value"},
    }


@pytest.fixture
def sample_tag_data() -> dict[str, Any]:
    """示例标签数据"""
    return {
        "name": "测试标签",
        "description": "这是一个用于测试的标签",
        "category": "test",
        "embedding": [0.1] * 1024,
    }


@pytest.fixture
def sample_batch_articles() -> list[dict[str, Any]]:
    """批量示例文章数据"""
    return [
        {
            "news_id": f"test_article_{i:03d}",
            "title": f"测试文章标题 {i}",
            "url": f"https://example.com/test-article-{i}",
            "content_markdown": f"# 测试标题 {i}\n\n测试内容段落 {i}。",
            "content_text": f"测试标题 {i} 测试内容段落 {i}。",
            "publish_date": f"2026-03-{19 + i:02d}T10:00:00Z",
            "source_site": "测试站点",
            "author": "测试作者",
            "tags": [f"标签{i}"],
        }
        for i in range(1, 6)
    ]


# =============================================================================
# Fixtures: 数据库连接
# =============================================================================


@pytest.fixture
def db_connection(temp_db_path: str) -> Generator[Any, None, None]:
    """创建临时数据库连接"""
    from app.infrastructure.storage.lancedb.connection import LanceDBConnection

    LanceDBConnection.reset()

    conn = LanceDBConnection(temp_db_path)

    yield conn

    LanceDBConnection.reset()


@pytest.fixture
def initialized_db(db_connection: Any) -> Generator[Any, None, None]:
    """已初始化的数据库（包含articles表）"""
    db_connection.create_articles_table(exist_ok=True)

    yield db_connection


# =============================================================================
# Fixtures: 仓库和组件
# =============================================================================


@pytest.fixture
def article_repository(initialized_db: Any) -> Generator[Any, None, None]:
    """文章仓库实例"""
    from app.infrastructure.storage.lancedb.repository import ArticleRepository

    repo = ArticleRepository()
    yield repo


@pytest.fixture
def tag_repository(initialized_db: Any) -> Generator[Any, None, None]:
    """标签仓库实例"""
    from app.infrastructure.storage.lancedb.tag_repository import TagRepository

    repo = TagRepository()
    yield repo


# =============================================================================
# 工具函数
# =============================================================================


def create_test_embedding(dim: int, value: float = 0.1) -> list[float]:
    """创建测试向量"""
    return [value] * dim


def assert_dict_contains(actual: dict[str, Any], expected: dict[str, Any]) -> None:
    """断言实际字典包含预期字典的所有键值对"""
    for key, expected_value in expected.items():
        assert key in actual, f"Missing key: {key}"
        assert actual[key] == expected_value, (
            f"Mismatch for key {key}: expected {expected_value}, got {actual[key]}"
        )


# =============================================================================
# 测试配置
# =============================================================================


def pytest_configure(config: Any) -> None:
    """配置pytest"""
    config.addinivalue_line("markers", "integration: 标记为集成测试（需要数据库）")
    config.addinivalue_line("markers", "slow: 标记为慢速测试")
    config.addinivalue_line("markers", "unit: 标记为单位测试（快速、隔离）")

