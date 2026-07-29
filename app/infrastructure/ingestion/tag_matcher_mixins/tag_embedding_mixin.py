from __future__ import annotations

from app.infrastructure.ingestion.tag_matcher import (
    Any,
    TAG_EMBEDDING_DIM,
    TagMatchingConfig,
    TagRepository,
    VectorSimilarity,
    get_tag_repository,
    logger,
    logging,
    math,
    np,
)

class TagEmbeddingMixin:
    """封装 TagMatcher 的单一职责方法。"""

    def _get_tag_embeddings(self) -> list[tuple[str, list[float]]]:
        """获取所有标签的向量表示"""

        if self._enable_cache and self._tag_cache is not None:
            import time

            current_time = time.time()
            if current_time - self._cache_timestamp < TagMatchingConfig.CACHE_TTL:
                logger.debug("Using cached tag embeddings")
                return self._tag_cache

        try:

            tag_embeddings = self._repo.get_all_embeddings()

            valid_embeddings = []
            for tag_id, embedding in tag_embeddings:
                if embedding and len(embedding) == TAG_EMBEDDING_DIM:
                    valid_embeddings.append((tag_id, embedding))
                else:
                    logger.warning(
                        f"Invalid embedding for tag {tag_id}: dimension={len(embedding) if embedding else 'empty'}"
                    )

            if self._enable_cache:
                self._tag_cache = valid_embeddings
                import time

                self._cache_timestamp = time.time()
                logger.debug(f"Cached {len(valid_embeddings)} tag embeddings")

            return valid_embeddings
        except Exception as e:
            logger.error(f"Failed to get tag embeddings: {e}")
            return []

    def clear_cache(self) -> None:
        """清空标签向量缓存"""
        self._tag_cache = None
        self._cache_timestamp = 0.0
        logger.debug("Tag cache cleared")

    def update_config(
        self,
        strict: bool | None = None,
        threshold: float | None = None,
        max_tags: int | None = None,
        similarity_method: str | None = None,
    ) -> None:
        """更新匹配配置"""
        if strict is not None:
            self._strict = strict
            if threshold is None:
                self._threshold = (
                    TagMatchingConfig.STRICT_THRESHOLD
                    if strict
                    else TagMatchingConfig.RELAXED_THRESHOLD
                )

        if threshold is not None:
            self._threshold = threshold

        if max_tags is not None:
            self._max_tags = max_tags

        if similarity_method is not None:
            self._similarity_method = similarity_method

        logger.info(
            f"TagMatcher config updated: strict={self._strict}, threshold={self._threshold:.2f}, "
            f"max_tags={self._max_tags}, method={self._similarity_method}"
        )

    def get_config(self) -> dict[str, Any]:
        """获取当前配置"""
        return {
            "strict": self._strict,
            "threshold": self._threshold,
            "max_tags": self._max_tags,
            "similarity_method": self._similarity_method,
            "enable_cache": self._enable_cache,
        }
