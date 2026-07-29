"""Dedup 单元测试"""

from unittest.mock import MagicMock

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

        assert is_similar(100, 100) is True

        assert is_similar(0xFFFFFFFFFFFFFFFF, 0x0000000000000000, threshold=3) is False
