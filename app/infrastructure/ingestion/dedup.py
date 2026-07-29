"""Deduplication - URL 和内容去重检测"""

import hashlib
import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

SIMHASH_BITS = 64
SIMHASH_DISTANCE_THRESHOLD = 3
DEFAULT_SIMHASH_ENABLED = False

def normalize_url(url: str) -> str:
    """规范化 URL"""
    if not url:
        return ""

    url = url.lower().strip()
    url = url.rstrip("/")
    url = re.sub(r"[?&](utm_\w+|ref|source|from)=[^&]*", "", url)
    url = re.sub(r"\?$", "", url)

    return url

def url_hash(url: str) -> str:
    """计算 URL 的哈希值（规范化后 MD5 前16位）"""
    if not url:
        return ""
    normalized = normalize_url(url)
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()

def compute_url_hash(url: str) -> str:
    """计算 URL 哈希（规范化后）"""
    return url_hash(url)

from app.infrastructure.ingestion.dedup_parts.simhash import (
    SimHash,
)

def compute_simhash(content: str) -> int:
    """计算内容 SimHash"""
    return SimHash().compute(content)

def is_similar(hash1: int, hash2: int, threshold: int = SIMHASH_DISTANCE_THRESHOLD) -> bool:
    """判断两个 SimHash 是否相似"""
    return SimHash.hamming_distance(hash1, hash2) <= threshold

from app.infrastructure.ingestion.dedup_parts.result import (
    DedupResult,
)
from app.infrastructure.ingestion.dedup_parts.service import (
    DeduplicationService,
)
