"""
Deduplication - URL 和内容去重检测

提供基于 URL 哈希和内容 SimHash 的去重功能。

Responsibilities:
    - URL 哈希去重
    - SimHash 内容相似度检测
    - 与 LanceDB 集成的重复检查
"""

import hashlib
import logging
import re
from typing import Any, Protocol

logger = logging.getLogger(__name__)


# =============================================================================
# 配置
# =============================================================================

# SimHash 配置
SIMHASH_BITS = 64
SIMHASH_DISTANCE_THRESHOLD = 3  # 汉明距离阈值，小于此值视为重复


# =============================================================================
# URL 哈希
# =============================================================================


def url_hash(url: str) -> str:
    """
    计算 URL 的哈希值

    Args:
        url: URL 字符串

    Returns:
        MD5 哈希值 (32 位十六进制字符串)
    """
    if not url:
        return ""
    # 规范化 URL
    normalized = normalize_url(url)
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()  # noqa: S324


def normalize_url(url: str) -> str:
    """
    规范化 URL

    - 移除末尾斜杠
    - 转小写
    - 移除常见跟踪参数

    Args:
        url: 原始 URL

    Returns:
        规范化后的 URL
    """
    if not url:
        return ""

    # 转小写
    url = url.lower().strip()

    # 移除末尾斜杠
    url = url.rstrip("/")

    # 移除常见跟踪参数
    url = re.sub(r"[?&](utm_\w+|ref|source|from)=[^&]*", "", url)

    # 移除空的查询字符串
    url = re.sub(r"\?$", "", url)

    return url


# =============================================================================
# SimHash 实现
# =============================================================================


class SimHash:
    """
    SimHash 内容指纹算法

    用于快速检测内容相似度，适用于文本去重。
    汉明距离小于阈值的文档被视为重复或近似重复。
    """

    def __init__(self, bits: int = SIMHASH_BITS) -> None:
        """
        初始化 SimHash

        Args:
            bits: 哈希位数
        """
        self._bits = bits

    def compute(self, text: str) -> int:
        """
        计算文本的 SimHash 值

        Args:
            text: 输入文本

        Returns:
            SimHash 值 (整数)
        """
        if not text:
            return 0

        # 分词 (简单按空白和标点分割)
        tokens = self._tokenize(text)
        if not tokens:
            return 0

        # 初始化向量
        v = [0] * self._bits

        # 计算每个 token 的贡献
        for token in tokens:
            # 计算 token 的哈希
            token_hash = self._hash_token(token)

            # 更新向量
            for i in range(self._bits):
                if token_hash & (1 << i):
                    v[i] += 1
                else:
                    v[i] -= 1

        # 生成 SimHash
        fingerprint = 0
        for i in range(self._bits):
            if v[i] > 0:
                fingerprint |= 1 << i

        return fingerprint

    def _tokenize(self, text: str) -> list[str]:
        """
        简单分词

        Args:
            text: 输入文本

        Returns:
            token 列表
        """
        # 移除标点符号
        text = re.sub(r"[^\w\s]", " ", text)
        # 按空白分割
        tokens = text.split()
        # 过滤短 token
        return [t for t in tokens if len(t) >= 2]

    def _hash_token(self, token: str) -> int:
        """
        计算单个 token 的哈希

        Args:
            token: 单个词语

        Returns:
            哈希值
        """
        h = hashlib.md5(token.encode("utf-8")).hexdigest()  # noqa: S324
        raw_value = int(h, 16)
        modulus = 2 ** int(self._bits)
        return int(raw_value % modulus)

    @staticmethod
    def hamming_distance(hash1: int, hash2: int) -> int:
        """
        计算两个 SimHash 的汉明距离

        Args:
            hash1: 第一个 SimHash
            hash2: 第二个 SimHash

        Returns:
            汉明距离
        """
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
        """
        判断两个 SimHash 是否相似

        Args:
            hash1: 第一个 SimHash
            hash2: 第二个 SimHash
            threshold: 汉明距离阈值

        Returns:
            是否相似
        """
        return self.hamming_distance(hash1, hash2) <= threshold


# =============================================================================
# 去重检测器
# =============================================================================


class DuplicateDetector:
    """
    去重检测器

    整合 URL 哈希和 SimHash 两种去重方法

    Usage:
        >>> detector = DuplicateDetector()
        >>> detector.is_url_duplicate("https://example.com/news/1", existing_urls)
        >>> detector.is_content_duplicate("新闻内容...", existing_hashes)
    """

    def __init__(
        self,
        simhash_bits: int = SIMHASH_BITS,
        similarity_threshold: int = SIMHASH_DISTANCE_THRESHOLD,
    ) -> None:
        """
        初始化去重检测器

        Args:
            simhash_bits: SimHash 位数
            similarity_threshold: 相似度阈值
        """
        self._simhash = SimHash(bits=simhash_bits)
        self._threshold = similarity_threshold

    def compute_url_hash(self, url: str) -> str:
        """计算 URL 哈希"""
        return url_hash(url)

    def compute_content_hash(self, content: str) -> int:
        """计算内容 SimHash"""
        return self._simhash.compute(content)

    def is_url_duplicate(self, url: str, existing_hashes: set[str]) -> bool:
        """
        检查 URL 是否重复

        Args:
            url: 待检查的 URL
            existing_hashes: 已有 URL 哈希集合

        Returns:
            是否重复
        """
        h = self.compute_url_hash(url)
        return h in existing_hashes

    def is_content_duplicate(
        self,
        content: str,
        existing_hashes: list[int],
    ) -> bool:
        """
        检查内容是否与已有内容相似

        Args:
            content: 待检查的内容
            existing_hashes: 已有内容 SimHash 列表

        Returns:
            是否存在相似内容
        """
        if not existing_hashes:
            return False

        content_hash = self.compute_content_hash(content)

        for existing in existing_hashes:
            if self._simhash.is_similar(content_hash, existing, self._threshold):
                return True

        return False

    def find_duplicates(
        self,
        documents: list[dict[str, Any]],
        url_key: str = "url",
        content_key: str = "content_text",
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        从文档列表中找出重复和非重复文档

        Args:
            documents: 文档列表
            url_key: URL 字段名
            content_key: 内容字段名

        Returns:
            (唯一文档列表, 重复文档列表)
        """
        unique = []
        duplicates = []
        seen_urls: set[str] = set()
        seen_hashes: list[int] = []

        for doc in documents:
            url = doc.get(url_key, "")
            content = doc.get(content_key, "")

            # 检查 URL 重复
            url_h = self.compute_url_hash(url)
            if url_h in seen_urls:
                duplicates.append(doc)
                continue

            # 检查内容重复
            if content and self.is_content_duplicate(content, seen_hashes):
                duplicates.append(doc)
                continue

            # 标记为已见
            seen_urls.add(url_h)
            if content:
                seen_hashes.append(self.compute_content_hash(content))

            unique.append(doc)

        logger.info(f"Dedup result: {len(unique)} unique, {len(duplicates)} duplicates")
        return unique, duplicates


# =============================================================================
# 与 Repository 集成
# =============================================================================


class _RepositoryLike(Protocol):
    def exists_by_url(self, url: str) -> bool: ...

    def exists(self, news_id: str) -> bool: ...


class RepositoryDedup:
    """
    与 LanceDB Repository 集成的去重检测器

    在写入前检查数据库中是否已存在相同记录
    """

    def __init__(self, repository: _RepositoryLike | None = None) -> None:
        """
        初始化

        Args:
            repository: ArticleRepository 实例
        """
        self._repository = repository
        self._detector = DuplicateDetector()

    def set_repository(self, repository: _RepositoryLike | None) -> None:
        """设置 Repository"""
        self._repository = repository

    def exists_by_url(self, url: str) -> bool:
        """
        检查 URL 是否已存在于数据库

        Args:
            url: URL 字符串

        Returns:
            是否存在
        """
        if not self._repository:
            return False
        return bool(self._repository.exists_by_url(url))

    def exists_by_id(self, news_id: str) -> bool:
        """
        检查 news_id 是否已存在于数据库

        Args:
            news_id: 新闻 ID

        Returns:
            是否存在
        """
        if not self._repository:
            return False
        return bool(self._repository.exists(news_id))

    def filter_new_documents(
        self,
        documents: list[dict[str, Any]],
        id_key: str = "news_id",
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        过滤出新文档和已存在的文档

        Args:
            documents: 文档列表
            id_key: ID 字段名

        Returns:
            (新文档列表, 已存在文档列表)
        """
        new_docs = []
        existing_docs = []

        for doc in documents:
            doc_id = doc.get(id_key)
            if doc_id and self.exists_by_id(doc_id):
                existing_docs.append(doc)
            else:
                new_docs.append(doc)

        logger.info(f"Filter result: {len(new_docs)} new, {len(existing_docs)} existing")
        return new_docs, existing_docs


# =============================================================================
# 便捷函数
# =============================================================================


def compute_url_hash(url: str) -> str:
    """计算 URL 哈希"""
    return url_hash(url)


def compute_simhash(content: str) -> int:
    """计算内容 SimHash"""
    return SimHash().compute(content)


def is_similar(hash1: int, hash2: int, threshold: int = SIMHASH_DISTANCE_THRESHOLD) -> bool:
    """判断两个 SimHash 是否相似"""
    return SimHash.hamming_distance(hash1, hash2) <= threshold
