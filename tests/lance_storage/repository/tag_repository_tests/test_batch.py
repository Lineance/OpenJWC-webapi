"""Tag Repository Integration Tests - 真实实现测试"""

from datetime import datetime

from app.infrastructure.storage.lancedb.tag_repository import TagRepository

from app.infrastructure.storage.lancedb.tag_schema import TagRecord

def _make_tag(
    tag_id: str,
    name: str,
    description: str = "测试描述",
    category: str = "test",
    embedding_dim: int = 1024,
) -> TagRecord:
    """创建测试用 TagRecord"""
    return TagRecord(
        tag_id=tag_id,
        name=name,
        description=description,
        category=category,
        embedding=[0.1] * embedding_dim,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )

class TestTagRepositoryBatch:
    """批量操作测试"""

    def test_clear_all(self, temp_db_path: str) -> None:
        """测试清空所有标签"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tags = [_make_tag(f"tag_{i:03d}", f"标签{i}") for i in range(3)]
        repo.add_batch(tags)
        assert repo.count() == 3

        result = repo.clear_all()
        assert result is True
        assert repo.count() == 0

    def test_get_all_embeddings(self, temp_db_path: str) -> None:
        """测试获取所有 embeddings"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tags = [
            _make_tag("tag_001", "标签1"),
            _make_tag("tag_002", "标签2"),
        ]
        repo.add_batch(tags)

        embeddings = repo.get_all_embeddings()
        assert len(embeddings) == 2
        assert all(len(emb) == 2 for emb in embeddings)

class TestTagRepositoryStats:
    """统计功能测试"""

    def test_count_by_category(self, temp_db_path: str) -> None:
        """测试按分类计数"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tags = [
            _make_tag("tag_001", "标签1", category="tech"),
            _make_tag("tag_002", "标签2", category="tech"),
            _make_tag("tag_003", "标签3", category="edu"),
        ]
        repo.add_batch(tags)

        counts = repo.count_by_category()
        assert counts.get("tech", 0) == 2
        assert counts.get("edu", 0) == 1

    def test_exists(self, temp_db_path: str) -> None:
        """测试标签是否存在"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tag = _make_tag("tag_001", "测试")
        repo.add_one(tag)

        assert repo.exists("tag_001") is True
        assert repo.exists("nonexistent") is False
