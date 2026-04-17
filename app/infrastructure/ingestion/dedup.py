"""
Deduplication - URL 和内容去重检测

提供基于 URL 哈希和内容 SimHash 的去重工具函数和统一去重服务。

Responsibilities:
    - URL 规范化
    - URL 哈希计算
    - SimHash 内容相似度检测
    - 三岔分类去重服务（NEW / UPSERT / DUPLICATE）
"""

import hashlib
import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# =============================================================================
# 配置
# =============================================================================

SIMHASH_BITS = 64
SIMHASH_DISTANCE_THRESHOLD = 3
DEFAULT_SIMHASH_ENABLED = False

# =============================================================================
# URL 规范化与哈希
# =============================================================================


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

    url = url.lower().strip()
    url = url.rstrip("/")
    url = re.sub(r"[?&](utm_\w+|ref|source|from)=[^&]*", "", url)
    url = re.sub(r"\?$", "", url)

    return url


def url_hash(url: str) -> str:
    """
    计算 URL 的哈希值（规范化后 MD5 前16位）

    Args:
        url: URL 字符串

    Returns:
        MD5 哈希值 (32 位十六进制字符串)
    """
    if not url:
        return ""
    normalized = normalize_url(url)
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()  # noqa: S324


def compute_url_hash(url: str) -> str:
    """计算 URL 哈希（规范化后）"""
    return url_hash(url)


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
        h = hashlib.md5(token.encode("utf-8")).hexdigest()  # noqa: S324
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


def compute_simhash(content: str) -> int:
    """计算内容 SimHash"""
    return SimHash().compute(content)


def is_similar(hash1: int, hash2: int, threshold: int = SIMHASH_DISTANCE_THRESHOLD) -> bool:
    """判断两个 SimHash 是否相似"""
    return SimHash.hamming_distance(hash1, hash2) <= threshold


# =============================================================================
# 统一去重服务
# =============================================================================


@dataclass
class DedupResult:
    """
    三岔去重结果

    Attributes:
        new_docs: news_id 不存在 → INSERT
        upsert_docs: news_id+url 匹配但 publish_date 不同 → UPDATE
        duplicate_docs: news_id+url+publish_date 全匹配 → 跳过
    """

    new_docs: list[dict[str, Any]] = field(default_factory=list)
    upsert_docs: list[dict[str, Any]] = field(default_factory=list)
    duplicate_docs: list[dict[str, Any]] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.new_docs or self.upsert_docs or self.duplicate_docs)


class DeduplicationService:
    """
    统一去重服务

    Features:
        - 单次调用完成全部去重逻辑
        - 批量 DB 查询（无逐条查询）
        - 三岔分类：NEW / UPSERT / DUPLICATE
        - 可选 SimHash（默认关闭）

    Usage:
        >>> service = DeduplicationService(repository)
        >>> result = service.dedup(documents)
        >>> # result.new_docs → insert
        >>> # result.upsert_docs → upsert
        >>> # result.duplicate_docs → skip
    """

    def __init__(
        self,
        repository: Any,  # ArticleRepository
        simhash_enabled: bool = DEFAULT_SIMHASH_ENABLED,
        simhash_bits: int = SIMHASH_BITS,
        simhash_threshold: int = SIMHASH_DISTANCE_THRESHOLD,
    ) -> None:
        self._repository = repository
        self._simhash_enabled = simhash_enabled
        self._simhash = SimHash(bits=simhash_bits) if simhash_enabled else None
        self._simhash_threshold = simhash_threshold

        self._news_id_key = "news_id"
        self._url_key = "url"
        self._publish_date_key = "publish_date"
        self._content_key = "content_text"

    def dedup(
        self,
        documents: list[dict[str, Any]],
        news_id_key: str | None = None,
        url_key: str | None = None,
        publish_date_key: str | None = None,
        content_key: str | None = None,
    ) -> DedupResult:
        """
        三岔去重

        Args:
            documents: 文档列表（标准化后）
            news_id_key: news_id 字段名
            url_key: url 字段名
            publish_date_key: publish_date 字段名
            content_key: content_text 字段名

        Returns:
            DedupResult (new_docs, upsert_docs, duplicate_docs)
        """
        if news_id_key is not None:
            self._news_id_key = news_id_key
        if url_key is not None:
            self._url_key = url_key
        if publish_date_key is not None:
            self._publish_date_key = publish_date_key
        if content_key is not None:
            self._content_key = content_key

        if not documents:
            return DedupResult()

        batch_unique, batch_duplicates = self._in_batch_dedup(documents)

        if not batch_unique:
            return DedupResult(
                new_docs=[], upsert_docs=[], duplicate_docs=batch_duplicates
            )

        new_docs, upsert_docs, db_duplicates = self._db_dedup_three_way(batch_unique)
        all_duplicates = batch_duplicates + db_duplicates

        logger.info(
            f"Dedup result: new={len(new_docs)}, upsert={len(upsert_docs)}, "
            f"duplicate={len(all_duplicates)}"
        )

        return DedupResult(
            new_docs=new_docs,
            upsert_docs=upsert_docs,
            duplicate_docs=all_duplicates,
        )

    # -------------------------------------------------------------------------
    # 批次内去重
    # -------------------------------------------------------------------------

    def _in_batch_dedup(
        self,
        documents: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        批次内去重：URL hash + 可选 SimHash

        Returns:
            (unique_docs, duplicate_docs)
        """
        unique = []
        duplicates = []
        seen_url_hashes: set[str] = set()
        seen_simhashes: list[int] = []

        for doc in documents:
            url = doc.get(self._url_key, "")
            content = doc.get(self._content_key, "")

            url_h = url_hash(url)
            if url_h in seen_url_hashes:
                duplicates.append(doc)
                continue

            if self._simhash_enabled and content:
                content_h = self._simhash.compute(content)  # type: ignore
                for existing_h in seen_simhashes:
                    if self._simhash.is_similar(  # type: ignore
                        content_h, existing_h, self._simhash_threshold
                    ):
                        duplicates.append(doc)
                        break
                else:
                    seen_simhashes.append(content_h)

            seen_url_hashes.add(url_h)
            unique.append(doc)

        return unique, duplicates

    # -------------------------------------------------------------------------
    # DB 三岔分类
    # -------------------------------------------------------------------------

    def _db_dedup_three_way(
        self,
        documents: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """
        批量 DB 查询 + 三岔分类

        - news_id 不存在 → NEW
        - news_id 存在，normalize(url) 相同，publish_date 相同 → DUPLICATE
        - news_id 存在，normalize(url) 相同，publish_date 不同 → UPSERT

        Returns:
            (new_docs, upsert_docs, duplicate_docs)
        """
        news_ids = [
            doc.get(self._news_id_key)
            for doc in documents
            if doc.get(self._news_id_key)
        ]

        if not news_ids:
            return documents, [], []

        existing_records = self._repository.find_by_news_ids(news_ids)
        existing_by_id: dict[str, dict[str, Any]] = {
            rec[self._news_id_key]: rec for rec in existing_records
        }

        new_docs = []
        upsert_docs = []
        duplicate_docs = []

        for doc in documents:
            news_id = doc.get(self._news_id_key)
            if not news_id:
                new_docs.append(doc)
                continue

            existing = existing_by_id.get(news_id)
            if existing is None:
                new_docs.append(doc)
                continue

            incoming_url_norm = normalize_url(doc.get(self._url_key) or "")
            existing_url_norm = normalize_url(existing.get(self._url_key) or "")

            if incoming_url_norm != existing_url_norm:
                new_docs.append(doc)
                continue

            dates_match = self._dates_match(
                doc.get(self._publish_date_key),
                existing.get(self._publish_date_key),
            )

            if dates_match:
                duplicate_docs.append(doc)
            else:
                upsert_docs.append(doc)

        return new_docs, upsert_docs, duplicate_docs

    @staticmethod
    def _dates_match(date1: Any, date2: Any) -> bool:
        """比对两个日期是否相同（处理 None 和 datetime 对象）"""
        if date1 is None and date2 is None:
            return True
        if date1 is None or date2 is None:
            return False
        dt1 = date1.isoformat() if hasattr(date1, "isoformat") else str(date1)
        dt2 = date2.isoformat() if hasattr(date2, "isoformat") else str(date2)
        return dt1 == dt2
