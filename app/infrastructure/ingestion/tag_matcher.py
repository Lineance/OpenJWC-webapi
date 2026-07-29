"""Tag Matcher - 标签向量匹配器"""

import logging
import math
from typing import Any

import numpy as np

from app.infrastructure.storage.lancedb.tag_repository import TagRepository, get_tag_repository
from app.infrastructure.storage.lancedb.tag_schema import TAG_EMBEDDING_DIM

logger = logging.getLogger(__name__)
from app.infrastructure.ingestion.tag_matcher_mixins.models import (
    TagMatchingConfig,
    VectorSimilarity,
)

from .tag_matcher_mixins.tag_embedding_mixin import TagEmbeddingMixin
from .tag_matcher_mixins.single_tag_match_mixin import SingleTagMatchMixin
from .tag_matcher_mixins.batch_tag_match_mixin import BatchTagMatchMixin

class TagMatcher(TagEmbeddingMixin, SingleTagMatchMixin, BatchTagMatchMixin):
    """标签向量匹配器"""

    def __init__(
        self,
        tag_repository: TagRepository | None = None,
        strict: bool = True,
        threshold: float | None = None,
        max_tags: int = TagMatchingConfig.MAX_TAGS_PER_ARTICLE,
        similarity_method: str = TagMatchingConfig.SIMILARITY_METHOD,
        enable_cache: bool = TagMatchingConfig.ENABLE_CACHE,
    ) -> None:
        """初始化标签匹配器"""
        self._repo = tag_repository or get_tag_repository()
        self._strict = strict
        self._threshold = threshold or (
            TagMatchingConfig.STRICT_THRESHOLD if strict else TagMatchingConfig.RELAXED_THRESHOLD
        )
        self._max_tags = max_tags
        self._similarity_method = similarity_method
        self._enable_cache = enable_cache

        self._tag_cache: list[tuple[str, list[float]]] | None = None
        self._cache_timestamp: float = 0.0

        logger.info(
            f"TagMatcher initialized: strict={strict}, threshold={self._threshold:.2f}, "
            f"max_tags={max_tags}, method={similarity_method}"
        )

def get_tag_matcher(
    strict: bool = True,
    threshold: float | None = None,
    max_tags: int = TagMatchingConfig.MAX_TAGS_PER_ARTICLE,
) -> TagMatcher:
    """获取标签匹配器实例"""
    return TagMatcher(
        strict=strict,
        threshold=threshold,
        max_tags=max_tags,
    )

def match_content_tags(content_embedding: list[float], strict: bool = True) -> list[str]:
    """快速匹配内容标签"""
    matcher = get_tag_matcher(strict=strict)
    return matcher.match_tags(content_embedding)

def batch_match_content_tags(
    content_embeddings: list[list[float]], strict: bool = True
) -> list[list[str]]:
    """批量快速匹配内容标签"""
    matcher = get_tag_matcher(strict=strict)
    return matcher.match_batch(content_embeddings)
