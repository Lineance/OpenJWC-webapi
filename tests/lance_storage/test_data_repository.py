"""
测试数据仓库功能

测试 ArticleRepository 的 CRUD 操作。
"""

import threading
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from app.infrastructure.storage.lancedb.connection import get_connection
from app.infrastructure.storage.lancedb.repository import (
    ArticleRepository,
    _safe_publish_date_str,
)


class TestArticleRepository:
    """ArticleRepository 测试类"""

    def test_repository_initialization(self, initialized_db: Any) -> None:
        """测试仓库初始化"""
        repo = ArticleRepository()
        assert repo is not None
        assert repo.table is not None

    def test_repository_initialization_with_db_path(self, temp_db_path: Any) -> None:
        """测试带数据库路径的初始化"""
        repo = ArticleRepository(db_path=temp_db_path)
        assert repo is not None

    def test_add_one_success(
        self, article_repository: Any, sample_article_data: dict[str, Any]
    ) -> None:
        """测试添加单条记录成功"""
        # 确保数据包含必要的向量字段
        sample_article_data["title_embedding"] = [0.1] * 384
        sample_article_data["content_embedding"] = [0.1] * 1024

        result = article_repository.add_one(sample_article_data)
        assert result is True

    def test_add_one_failure(self, article_repository: Any) -> None:
        """测试添加失败"""
        result = article_repository.add_one({})
        assert result is False

    def test_add_one_invalid_data(self, article_repository: Any) -> None:
        """测试添加无效数据"""
        invalid_data = {
            "news_id": "test",
            # 缺少必需字段
        }
        result = article_repository.add_one(invalid_data)
        assert result is False

    def test_add_batch_empty_list(self, article_repository: Any) -> None:
        """测试批量添加空列表"""
        result = article_repository.add([])
        assert result == 0

    def test_add_batch_success(
        self, article_repository: Any, sample_batch_articles: list[dict[str, Any]]
    ) -> None:
        """测试批量添加成功"""
        # 添加向量字段
        for article in sample_batch_articles:
            article["title_embedding"] = [0.1] * 384
            article["content_embedding"] = [0.1] * 1024

        result = article_repository.add(sample_batch_articles)
        assert result == 5

    def test_get_existing_article(
        self, article_repository: Any, sample_article_data: dict[str, Any]
    ) -> None:
        """测试获取已存在的文章"""
        sample_article_data["title_embedding"] = [0.1] * 384
        sample_article_data["content_embedding"] = [0.1] * 1024
        article_repository.add_one(sample_article_data)

        result = article_repository.get(sample_article_data["news_id"])
        assert result is not None
        assert result["news_id"] == sample_article_data["news_id"]

    def test_get_nonexistent_article(self, article_repository: Any) -> None:
        """测试获取不存在的文章"""
        result = article_repository.get("nonexistent_id")
        assert result is None

    def test_count_success(self, article_repository: Any) -> None:
        """测试获取总数"""
        count = article_repository.count()
        assert count >= 0

    def test_exists_true(
        self, article_repository: Any, sample_article_data: dict[str, Any]
    ) -> None:
        """测试exists返回True"""
        sample_article_data["title_embedding"] = [0.1] * 384
        sample_article_data["content_embedding"] = [0.1] * 1024
        article_repository.add_one(sample_article_data)

        assert article_repository.exists(sample_article_data["news_id"]) is True

    def test_exists_false(self, article_repository: Any) -> None:
        """测试exists返回False"""
        assert article_repository.exists("nonexistent") is False

    def test_exists_by_url_true(
        self, article_repository: Any, sample_article_data: dict[str, Any]
    ) -> None:
        """测试按URL检查存在-True"""
        sample_article_data["title_embedding"] = [0.1] * 384
        sample_article_data["content_embedding"] = [0.1] * 1024
        article_repository.add_one(sample_article_data)

        assert article_repository.exists_by_url(sample_article_data["url"]) is True

    def test_exists_by_url_false(self, article_repository: Any) -> None:
        """测试按URL检查存在-False"""
        assert article_repository.exists_by_url("https://nonexistent.com") is False

    def test_get_latest_success(self, article_repository: Any) -> None:
        """测试获取最新记录"""
        results = article_repository.get_latest(limit=5)
        assert isinstance(results, list)

    def test_get_oldest_success(self, article_repository: Any) -> None:
        """测试获取最旧记录"""
        results = article_repository.get_oldest(limit=5)
        assert isinstance(results, list)

    def test_list_for_notices_fallback_when_order_path_fails(
        self,
        article_repository: Any,
        sample_article_data: dict[str, Any],
        monkeypatch: Any,
    ) -> None:
        """当 article_order 路径异常时，应回退到直接扫描而不是返回空列表。"""
        sample_article_data["title_embedding"] = [0.1] * 384
        sample_article_data["content_embedding"] = [0.1] * 1024
        article_repository.add_one(sample_article_data)

        def _raise_sync_error() -> Any:
            raise RuntimeError("simulated order table failure")

        monkeypatch.setattr(
            article_repository, "_get_synced_connection", _raise_sync_error
        )

        total, notices = article_repository.list_for_notices(
            limit=20, offset=0, label=None
        )

        assert total >= 1
        assert any(item["id"] == sample_article_data["news_id"] for item in notices)

    def test_list_for_notices_concurrent_reads_and_rebuilds(
        self,
        article_repository: Any,
        sample_batch_articles: list[dict[str, Any]],
    ) -> None:
        """并发读取 notices 与重建 article_order 时，应保持可读并且不抛出未处理异常。"""
        for article in sample_batch_articles:
            article["title_embedding"] = [0.1] * 384
            article["content_embedding"] = [0.1] * 1024

        inserted = article_repository.add(sample_batch_articles)
        assert inserted == len(sample_batch_articles)

        conn = get_connection()
        conn.rebuild_article_order()

        reader_threads = 4
        reader_rounds = 80
        rebuilder_threads = 2
        rebuild_rounds = 20

        stats: Counter[str] = Counter()
        errors: list[Exception] = []
        state_lock = threading.Lock()

        def reader() -> None:
            for _ in range(reader_rounds):
                try:
                    total, notices = article_repository.list_for_notices(
                        limit=20,
                        offset=0,
                        label=None,
                    )
                    with state_lock:
                        if total > 0 and notices:
                            stats["ok"] += 1
                        else:
                            stats["empty"] += 1
                except Exception as exc:  # pragma: no cover
                    with state_lock:
                        errors.append(exc)

        def rebuilder() -> None:
            for _ in range(rebuild_rounds):
                try:
                    conn.rebuild_article_order()
                    with state_lock:
                        stats["rebuild_ok"] += 1
                except Exception as exc:  # pragma: no cover
                    with state_lock:
                        errors.append(exc)

        workers = [threading.Thread(target=reader) for _ in range(reader_threads)] + [
            threading.Thread(target=rebuilder) for _ in range(rebuilder_threads)
        ]

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        assert not errors
        assert stats["ok"] > 0

        final_total, final_notices = article_repository.list_for_notices(
            limit=20,
            offset=0,
            label=None,
        )
        assert final_total >= len(sample_batch_articles)
        assert len(final_notices) > 0


class TestNoticeDateFormat:
    """Notices 日期格式回归测试"""

    def test_safe_publish_date_str_datetime(self) -> None:
        dt = datetime(2026, 4, 15, 8, 30, 0, tzinfo=timezone.utc)
        assert _safe_publish_date_str(dt) == "2026-04-15"

    def test_safe_publish_date_str_iso_string(self) -> None:
        assert _safe_publish_date_str("2026-04-15T08:30:00+00:00") == "2026-04-15"

    def test_safe_publish_date_str_date_string(self) -> None:
        assert _safe_publish_date_str("2026-04-15") == "2026-04-15"
