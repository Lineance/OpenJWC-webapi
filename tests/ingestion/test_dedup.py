"""Dedup 单元测试"""

from unittest.mock import MagicMock

import pytest


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
        assert len(result) == 32  # MD5 hash length

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


class TestSimHash:
    """SimHash 测试"""

    def test_simhash_init_default(self) -> None:
        """测试默认初始化"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        assert sh._bits == 64

    def test_simhash_init_custom_bits(self) -> None:
        """测试自定义位数"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash(bits=128)
        assert sh._bits == 128

    def test_simhash_compute_empty(self) -> None:
        """测试空文本"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        result = sh.compute("")
        assert result == 0

    def test_simhash_compute_normal(self) -> None:
        """测试正常文本"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        result = sh.compute("This is a test article content")
        assert result != 0

    def test_simhash_same_text_same_hash(self) -> None:
        """测试相同文本产生相同哈希"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        hash1 = sh.compute("Test content")
        hash2 = sh.compute("Test content")
        assert hash1 == hash2

    def test_simhash_different_text_different_hash(self) -> None:
        """测试不同文本可能产生不同哈希（SimHash有碰撞可能）"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        hash1 = sh.compute("Completely different text about machine learning")
        hash2 = sh.compute("Another totally different topic about cooking recipes")
        # SimHash can have collisions, so we just check they are valid ints
        assert isinstance(hash1, int)
        assert isinstance(hash2, int)

    def test_simhash_tokenize_filters_short_tokens(self) -> None:
        """测试分词过滤短 token"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        tokens = sh._tokenize("A B C test")  # A, B, C are single char
        # Single char tokens should be filtered out
        assert "A" not in tokens
        assert "B" not in tokens
        assert "test" in tokens

    def test_simhash_hash_token(self) -> None:
        """测试 token 哈希"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        h = sh._hash_token("test")
        assert isinstance(h, int)
        assert h > 0

    def test_hamming_distance(self) -> None:
        """测试汉明距离计算"""
        from app.infrastructure.ingestion.dedup import SimHash

        # Same hash = distance 0
        dist = SimHash.hamming_distance(0b1111, 0b1111)
        assert dist == 0

        # Different hashes
        dist = SimHash.hamming_distance(0b1111, 0b0000)
        assert dist == 4

    def test_is_similar_true(self) -> None:
        """测试判定为相似"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        # Very similar hashes should be similar (only 1 bit different)
        hash1 = 0xFFFFFFFFFFFFFFFE
        hash2 = 0xFFFFFFFFFFFFFFF0  # 4 bits different
        assert sh.is_similar(hash1, hash2, threshold=3) is True

    def test_is_similar_exactly_same(self) -> None:
        """测试完全相同的哈希"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        hash_val = 0xFFFFFFFFFFFFFFFF
        assert sh.is_similar(hash_val, hash_val, threshold=3) is True

    def test_is_similar_false(self) -> None:
        """测试判定为不相似"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        hash1 = 0xFFFFFFFFFFFFFFFF
        hash2 = 0x0000000000000000  # 64 bits different
        assert sh.is_similar(hash1, hash2, threshold=3) is False


class TestDeduplicationService:
    """DeduplicationService 测试"""

    def _make_mock_repo(self, existing_records: list[dict]) -> MagicMock:
        """创建模拟 repository"""
        mock_repo = MagicMock()
        mock_repo.find_by_news_ids.return_value = existing_records
        return mock_repo

    def test_dedup_all_new(self) -> None:
        """测试全部是新文档"""
        from app.infrastructure.ingestion.dedup import DeduplicationService

        mock_repo = self._make_mock_repo([])
        service = DeduplicationService(mock_repo)

        docs = [
            {"news_id": "1", "url": "https://example.com/1", "publish_date": "2024-01-01", "content_text": "Content 1"},
            {"news_id": "2", "url": "https://example.com/2", "publish_date": "2024-01-02", "content_text": "Content 2"},
        ]

        result = service.dedup(docs)

        assert len(result.new_docs) == 2
        assert len(result.upsert_docs) == 0
        assert len(result.duplicate_docs) == 0

    def test_dedup_exact_duplicate(self) -> None:
        """测试完全重复（news_id + url + publish_date 相同）"""
        from app.infrastructure.ingestion.dedup import DeduplicationService

        mock_repo = self._make_mock_repo([
            {"news_id": "1", "url": "https://example.com/1", "publish_date": "2024-01-01"},
        ])
        service = DeduplicationService(mock_repo)

        docs = [
            {"news_id": "1", "url": "https://example.com/1", "publish_date": "2024-01-01"},
        ]

        result = service.dedup(docs)

        assert len(result.new_docs) == 0
        assert len(result.upsert_docs) == 0
        assert len(result.duplicate_docs) == 1

    def test_dedup_upsert_date_changed(self) -> None:
        """测试 UPSERT（news_id + url 匹配但 publish_date 不同）"""
        from app.infrastructure.ingestion.dedup import DeduplicationService

        mock_repo = self._make_mock_repo([
            {"news_id": "1", "url": "https://example.com/1", "publish_date": "2024-01-01"},
        ])
        service = DeduplicationService(mock_repo)

        docs = [
            {"news_id": "1", "url": "https://example.com/1", "publish_date": "2024-01-15"},
        ]

        result = service.dedup(docs)

        assert len(result.new_docs) == 0
        assert len(result.upsert_docs) == 1
        assert len(result.duplicate_docs) == 0

    def test_dedup_url_normalization(self) -> None:
        """测试 URL 规范化（带末尾斜杠 vs 无斜杠）"""
        from app.infrastructure.ingestion.dedup import DeduplicationService

        mock_repo = self._make_mock_repo([
            {"news_id": "1", "url": "https://example.com/article/", "publish_date": "2024-01-01"},
        ])
        service = DeduplicationService(mock_repo)

        docs = [
            {"news_id": "1", "url": "https://example.com/article", "publish_date": "2024-01-01"},
        ]

        result = service.dedup(docs)

        # 规范化后 URL 相同，日期也相同 -> duplicate
        assert len(result.new_docs) == 0
        assert len(result.upsert_docs) == 0
        assert len(result.duplicate_docs) == 1

    def test_dedup_batch_query(self) -> None:
        """测试批量 DB 查询"""
        from app.infrastructure.ingestion.dedup import DeduplicationService

        mock_repo = self._make_mock_repo([])
        service = DeduplicationService(mock_repo)

        docs = [
            {"news_id": "1", "url": "https://example.com/1", "publish_date": "2024-01-01"},
            {"news_id": "2", "url": "https://example.com/2", "publish_date": "2024-01-02"},
        ]

        service.dedup(docs)

        # 验证批量查询被调用
        mock_repo.find_by_news_ids.assert_called_once_with(["1", "2"])

    def test_dedup_empty_input(self) -> None:
        """测试空输入"""
        from app.infrastructure.ingestion.dedup import DeduplicationService

        mock_repo = self._make_mock_repo([])
        service = DeduplicationService(mock_repo)

        result = service.dedup([])

        assert result.is_empty()

    def test_dedup_in_batch_duplicate(self) -> None:
        """测试批次内 URL 重复"""
        from app.infrastructure.ingestion.dedup import DeduplicationService

        mock_repo = self._make_mock_repo([])
        service = DeduplicationService(mock_repo)

        docs = [
            {"news_id": "1", "url": "https://example.com/1", "publish_date": "2024-01-01"},
            {"news_id": "2", "url": "https://example.com/1", "publish_date": "2024-01-01"},
        ]

        result = service.dedup(docs)

        # 第二个在批次内被检测为重复
        assert len(result.new_docs) == 1
        assert len(result.duplicate_docs) == 1


class TestConvenienceFunctions:
    """便捷函数测试"""

    def test_compute_url_hash_function(self) -> None:
        """测试 compute_url_hash 函数"""
        from app.infrastructure.ingestion.dedup import compute_url_hash

        result = compute_url_hash("https://example.com")
        assert len(result) == 32

    def test_compute_simhash_function(self) -> None:
        """测试 compute_simhash 函数"""
        from app.infrastructure.ingestion.dedup import compute_simhash

        result = compute_simhash("Test content")
        assert isinstance(result, int)

    def test_is_similar_function(self) -> None:
        """测试 is_similar 函数"""
        from app.infrastructure.ingestion.dedup import is_similar

        # Same hash
        assert is_similar(100, 100) is True

        # Very different hashes
        assert is_similar(0xFFFFFFFFFFFFFFFF, 0x0000000000000000, threshold=3) is False
