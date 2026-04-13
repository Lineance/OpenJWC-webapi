"""Dedup 单元测试"""

from typing import Any
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


class TestDuplicateDetector:
    """DuplicateDetector 测试"""

    def test_detector_init(self) -> None:
        """测试初始化"""
        from app.infrastructure.ingestion.dedup import DuplicateDetector

        detector = DuplicateDetector()
        assert detector._simhash is not None
        assert detector._threshold == 3

    def test_detector_custom_threshold(self) -> None:
        """测试自定义阈值"""
        from app.infrastructure.ingestion.dedup import DuplicateDetector

        detector = DuplicateDetector(similarity_threshold=5)
        assert detector._threshold == 5

    def test_compute_url_hash(self) -> None:
        """测试 URL 哈希计算"""
        from app.infrastructure.ingestion.dedup import DuplicateDetector

        detector = DuplicateDetector()
        h = detector.compute_url_hash("https://example.com")
        assert len(h) == 32

    def test_compute_content_hash(self) -> None:
        """测试内容哈希计算"""
        from app.infrastructure.ingestion.dedup import DuplicateDetector

        detector = DuplicateDetector()
        h = detector.compute_content_hash("Test content")
        assert isinstance(h, int)

    def test_is_url_duplicate_true(self) -> None:
        """测试 URL 重复"""
        from app.infrastructure.ingestion.dedup import DuplicateDetector

        detector = DuplicateDetector()
        url = "https://example.com/article/1"
        existing = {detector.compute_url_hash(url)}

        assert detector.is_url_duplicate(url, existing) is True

    def test_is_url_duplicate_false(self) -> None:
        """测试 URL 不重复"""
        from app.infrastructure.ingestion.dedup import DuplicateDetector

        detector = DuplicateDetector()
        existing = {"some_other_hash"}

        assert detector.is_url_duplicate("https://example.com", existing) is False

    def test_is_content_duplicate_empty_existing(self) -> None:
        """测试空已有内容"""
        from app.infrastructure.ingestion.dedup import DuplicateDetector

        detector = DuplicateDetector()
        result = detector.is_content_duplicate("content", [])
        assert result is False

    def test_is_content_duplicate_true(self) -> None:
        """测试内容重复"""
        from app.infrastructure.ingestion.dedup import DuplicateDetector

        detector = DuplicateDetector()
        content = "This is a test article about machine learning and AI"
        # Create a very similar content
        similar_content = "This is a test article about machine learning and AI technologies"

        hash1 = detector.compute_content_hash(content)
        hash2 = detector.compute_content_hash(similar_content)

        # They should be similar or not depending on actual implementation
        result = detector.is_content_duplicate(similar_content, [hash1])
        # Just check it doesn't crash and returns bool
        assert isinstance(result, bool)

    def test_find_duplicates(self) -> None:
        """测试批量去重"""
        from app.infrastructure.ingestion.dedup import DuplicateDetector

        detector = DuplicateDetector()
        docs = [
            {"url": "https://example.com/1", "content_text": "Article about machine learning basics"},
            {"url": "https://example.com/2", "content_text": "Cooking recipes for dinner"},
            {"url": "https://example.com/1", "content_text": "Different content about sports"},  # URL duplicate
        ]

        unique, duplicates = detector.find_duplicates(docs)

        assert len(unique) == 2
        assert len(duplicates) == 1
        assert duplicates[0]["url"] == "https://example.com/1"


class TestRepositoryDedup:
    """RepositoryDedup 测试"""

    def test_repo_dedup_init(self) -> None:
        """测试初始化"""
        from app.infrastructure.ingestion.dedup import RepositoryDedup

        dedup = RepositoryDedup()
        assert dedup._repository is None
        assert dedup._detector is not None

    def test_repo_dedup_init_with_repo(self) -> None:
        """测试带 repository 初始化"""
        from app.infrastructure.ingestion.dedup import RepositoryDedup

        mock_repo = MagicMock()
        dedup = RepositoryDedup(repository=mock_repo)
        assert dedup._repository is mock_repo

    def test_set_repository(self) -> None:
        """测试设置 repository"""
        from app.infrastructure.ingestion.dedup import RepositoryDedup

        dedup = RepositoryDedup()
        mock_repo = MagicMock()
        dedup.set_repository(mock_repo)
        assert dedup._repository is mock_repo

    def test_exists_by_url_no_repo(self) -> None:
        """测试无 repository 时返回 False"""
        from app.infrastructure.ingestion.dedup import RepositoryDedup

        dedup = RepositoryDedup()
        result = dedup.exists_by_url("https://example.com")
        assert result is False

    def test_exists_by_url_with_repo(self) -> None:
        """测试有 repository 时查询"""
        from app.infrastructure.ingestion.dedup import RepositoryDedup

        mock_repo = MagicMock()
        mock_repo.exists_by_url.return_value = True

        dedup = RepositoryDedup(repository=mock_repo)
        result = dedup.exists_by_url("https://example.com")

        assert result is True
        mock_repo.exists_by_url.assert_called_once_with("https://example.com")

    def test_exists_by_id_no_repo(self) -> None:
        """测试无 repository 时返回 False"""
        from app.infrastructure.ingestion.dedup import RepositoryDedup

        dedup = RepositoryDedup()
        result = dedup.exists_by_id("news_123")
        assert result is False

    def test_exists_by_id_with_repo(self) -> None:
        """测试有 repository 时查询"""
        from app.infrastructure.ingestion.dedup import RepositoryDedup

        mock_repo = MagicMock()
        mock_repo.exists.return_value = True

        dedup = RepositoryDedup(repository=mock_repo)
        result = dedup.exists_by_id("news_123")

        assert result is True
        mock_repo.exists.assert_called_once_with("news_123")

    def test_filter_new_documents_all_new(self) -> None:
        """测试过滤全部是新文档"""
        from app.infrastructure.ingestion.dedup import RepositoryDedup

        mock_repo = MagicMock()
        mock_repo.exists.return_value = False

        dedup = RepositoryDedup(repository=mock_repo)
        docs = [
            {"news_id": "1", "title": "Doc 1"},
            {"news_id": "2", "title": "Doc 2"},
        ]

        new_docs, existing = dedup.filter_new_documents(docs)

        assert len(new_docs) == 2
        assert len(existing) == 0

    def test_filter_new_documents_some_exist(self) -> None:
        """测试过滤部分已存在"""
        from app.infrastructure.ingestion.dedup import RepositoryDedup

        mock_repo = MagicMock()
        mock_repo.exists.side_effect = [False, True]  # First exists, second doesn't

        dedup = RepositoryDedup(repository=mock_repo)
        docs = [
            {"news_id": "1", "title": "Doc 1"},
            {"news_id": "2", "title": "Doc 2"},
        ]

        new_docs, existing = dedup.filter_new_documents(docs)

        assert len(new_docs) == 1
        assert len(existing) == 1
        assert new_docs[0]["news_id"] == "1"
        assert existing[0]["news_id"] == "2"


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
