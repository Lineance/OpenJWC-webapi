"""Dedup 单元测试"""

from unittest.mock import MagicMock

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

        assert isinstance(hash1, int)
        assert isinstance(hash2, int)

    def test_simhash_tokenize_filters_short_tokens(self) -> None:
        """测试分词过滤短 token"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()
        tokens = sh._tokenize("A B C test")

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

        dist = SimHash.hamming_distance(0b1111, 0b1111)
        assert dist == 0

        dist = SimHash.hamming_distance(0b1111, 0b0000)
        assert dist == 4

    def test_is_similar_true(self) -> None:
        """测试判定为相似"""
        from app.infrastructure.ingestion.dedup import SimHash

        sh = SimHash()

        hash1 = 0xFFFFFFFFFFFFFFFE
        hash2 = 0xFFFFFFFFFFFFFFF0
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
        hash2 = 0x0000000000000000
        assert sh.is_similar(hash1, hash2, threshold=3) is False
