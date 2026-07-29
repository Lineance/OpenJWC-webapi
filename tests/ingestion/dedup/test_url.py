"""Dedup 单元测试"""

from unittest.mock import MagicMock

class TestUrlHash:
    """URL 哈希测试"""

    def test_url_hash_empty(self) -> None:
        """测试空 URL"""
        from app.infrastructure.ingestion.dedup import url_hash

        result = url_hash("")
        assert result == ""

    def test_url_hash_normal(self) -> None:
        """测试正常 URL 哈希"""
        from app.infrastructure.ingestion.dedup import url_hash

        result = url_hash("https://example.com/article/1")
        assert len(result) == 32

    def test_url_hash_same_content(self) -> None:
        """测试相同内容产生相同哈希"""
        from app.infrastructure.ingestion.dedup import url_hash

        hash1 = url_hash("https://example.com/article/1")
        hash2 = url_hash("https://example.com/article/1")
        assert hash1 == hash2

    def test_url_hash_different_urls(self) -> None:
        """测试不同 URL 产生不同哈希"""
        from app.infrastructure.ingestion.dedup import url_hash

        hash1 = url_hash("https://example.com/article/1")
        hash2 = url_hash("https://example.com/article/2")
        assert hash1 != hash2

class TestNormalizeUrl:
    """URL 规范化测试"""

    def test_normalize_empty(self) -> None:
        """测试空 URL"""
        from app.infrastructure.ingestion.dedup import normalize_url

        result = normalize_url("")
        assert result == ""

    def test_normalize_lowercase(self) -> None:
        """测试转小写"""
        from app.infrastructure.ingestion.dedup import normalize_url

        result = normalize_url("HTTPS://EXAMPLE.COM/ARTICLE")
        assert result == "https://example.com/article"

    def test_normalize_strip_trailing_slash(self) -> None:
        """测试移除末尾斜杠"""
        from app.infrastructure.ingestion.dedup import normalize_url

        result = normalize_url("https://example.com/article/")
        assert result == "https://example.com/article"

    def test_normalize_remove_tracking_params(self) -> None:
        """测试移除跟踪参数"""
        from app.infrastructure.ingestion.dedup import normalize_url

        result = normalize_url("https://example.com/article?utm_source=test&ref=twitter")
        assert "utm_source" not in result
        assert "ref" not in result

    def test_normalize_remove_empty_query(self) -> None:
        """测试移除空查询字符串"""
        from app.infrastructure.ingestion.dedup import normalize_url

        result = normalize_url("https://example.com/article?")
        assert result == "https://example.com/article"

    def test_normalize_preserve_important_params(self) -> None:
        """测试保留重要参数"""
        from app.infrastructure.ingestion.dedup import normalize_url

        result = normalize_url("https://example.com/article?id=123&page=1")
        assert "id=123" in result
        assert "page=1" in result
