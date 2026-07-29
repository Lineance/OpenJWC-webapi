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

class TestTagRepositoryRealCrud:
    """TagRepository 真实 CRUD 测试"""

    def test_add_and_get(self, temp_db_path: str) -> None:
        """测试添加和获取标签"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tag = _make_tag("tag_001", "测试标签")
        result = repo.add_one(tag)
        assert result is True

        retrieved = repo.get("tag_001")
        assert retrieved is not None
        assert retrieved.name == "测试标签"

    def test_add_batch(self, temp_db_path: str) -> None:
        """测试批量添加"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tags = [
            _make_tag(f"tag_{i:03d}", f"标签{i}")
            for i in range(1, 6)
        ]
        count = repo.add_batch(tags)
        assert count == 5

        assert repo.count() == 5

    def test_get_by_name(self, temp_db_path: str) -> None:
        """测试按名称获取"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tag = _make_tag("tag_001", "UniqueTagName")
        repo.add_one(tag)

        retrieved = repo.get_by_name("UniqueTagName")
        assert retrieved is not None
        assert retrieved.tag_id == "tag_001"

    def test_update(self, temp_db_path: str) -> None:
        """测试更新标签"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tag = _make_tag("tag_001", "原始名称")
        repo.add_one(tag)

        result = repo.update("tag_001", {"name": "新名称", "description": "新描述"})
        assert result is True

        updated = repo.get("tag_001")
        assert updated is not None
        assert updated.name == "新名称"

    def test_delete_not_supported(self, temp_db_path: str) -> None:
        """测试删除标签（ LanceDB 不支持直接删除）"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tag = _make_tag("tag_001", "待删除")
        repo.add_one(tag)
        assert repo.count() == 1

        result = repo.delete("tag_001")

        assert result is False

    def test_count(self, temp_db_path: str) -> None:
        """测试计数"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        assert repo.count() == 0

        tags = [_make_tag(f"tag_{i:03d}", f"标签{i}") for i in range(3)]
        repo.add_batch(tags)
        assert repo.count() == 3
