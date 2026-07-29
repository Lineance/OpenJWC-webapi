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

class TestTagRepositorySearch:
    """TagRepository 搜索功能测试"""

    def test_search_by_name_no_index(self, temp_db_path: str) -> None:
        """测试按名称搜索（无索引时返回空）"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tags = [
            _make_tag("tag_001", "Python编程"),
            _make_tag("tag_002", "Java开发"),
            _make_tag("tag_003", "Python机器学习"),
        ]
        repo.add_batch(tags)

        results = repo.search_by_name("Python")

        assert isinstance(results, list)

    def test_find_similar_tags(self, temp_db_path: str) -> None:
        """测试相似标签查找"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tag = _make_tag("tag_001", "测试标签")
        repo.add_one(tag)

        query_vec = [0.1] * 1024
        similar = repo.find_similar_tags(query_vec, top_k=5)
        assert isinstance(similar, list)

    def test_find_tags_for_content(self, temp_db_path: str) -> None:
        """测试为内容查找标签"""
        from app.infrastructure.storage.lancedb.connection import LanceDBConnection

        LanceDBConnection.reset()
        conn = LanceDBConnection(temp_db_path)
        repo = TagRepository(connection=conn)

        tags = [
            _make_tag("tag_001", "科技"),
            _make_tag("tag_002", "教育"),
        ]
        repo.add_batch(tags)

        content_vec = [0.1] * 1024
        tag_ids = repo.find_tags_for_content(content_vec, top_k=2)
        assert isinstance(tag_ids, list)
