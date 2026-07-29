from __future__ import annotations

from app.infrastructure.ingestion.dedup import (
    Any,
    DEFAULT_SIMHASH_ENABLED,
    SIMHASH_BITS,
    SIMHASH_DISTANCE_THRESHOLD,
    compute_url_hash,
    dataclass,
    field,
    hashlib,
    logger,
    logging,
    normalize_url,
    re,
    url_hash,
)

class SimHash:
    """SimHash 内容指纹算法"""

    def __init__(self, bits: int = SIMHASH_BITS) -> None:
        """初始化 SimHash"""
        self._bits = bits

    def compute(self, text: str) -> int:
        """计算文本的 SimHash 值"""
        if not text:
            return 0

        tokens = self._tokenize(text)
        if not tokens:
            return 0

        v = [0] * self._bits
        for token in tokens:
            token_hash = self._hash_token(token)
            for i in range(self._bits):
                if token_hash & (1 << i):
                    v[i] += 1
                else:
                    v[i] -= 1

        fingerprint = 0
        for i in range(self._bits):
            if v[i] > 0:
                fingerprint |= 1 << i

        return fingerprint

    def _tokenize(self, text: str) -> list[str]:
        """简单分词"""
        text = re.sub(r"[^\w\s]", " ", text)
        tokens = text.split()
        return [t for t in tokens if len(t) >= 2]

    def _hash_token(self, token: str) -> int:
        """计算单个 token 的哈希"""
        h = hashlib.md5(token.encode("utf-8")).hexdigest()
        raw_value = int(h, 16)
        modulus = 2 ** int(self._bits)
        return int(raw_value % modulus)

    @staticmethod
    def hamming_distance(hash1: int, hash2: int) -> int:
        """计算两个 SimHash 的汉明距离"""
        x = hash1 ^ hash2
        distance = 0
        while x:
            distance += 1
            x &= x - 1
        return distance

    def is_similar(
        self,
        hash1: int,
        hash2: int,
        threshold: int = SIMHASH_DISTANCE_THRESHOLD,
    ) -> bool:
        """判断两个 SimHash 是否相似"""
        return self.hamming_distance(hash1, hash2) <= threshold
