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

class SingleTagMatchMixin:
    """封装 TagMatcher 的单一职责方法。"""

    def match_tags(self, content_embedding: list[float]) -> list[str]:
        """为单个内容向量匹配标签"""

        if not content_embedding or len(content_embedding) != TAG_EMBEDDING_DIM:
            logger.warning(
                f"Invalid content embedding dimension: {len(content_embedding) if content_embedding else 'empty'}"
            )
            return []

        try:

            tag_embeddings = self._get_tag_embeddings()
            if not tag_embeddings:
                logger.warning("No tag embeddings available")
                return []

            similarities = []
            for tag_id, tag_embedding in tag_embeddings:
                similarity = VectorSimilarity.compute_similarity(
                    content_embedding, tag_embedding, self._similarity_method
                )

                if similarity >= self._threshold:
                    similarities.append((tag_id, similarity))

            similarities.sort(key=lambda x: x[1], reverse=True)

            matched_tags = [tag_id for tag_id, _ in similarities[: self._max_tags]]

            logger.debug(
                f"Matched {len(matched_tags)} tags for content embedding "
                f"(threshold={self._threshold:.2f})"
            )

            return matched_tags

        except Exception as e:
            logger.error(f"Failed to match tags: {e}")
            return []

    def match_tags_with_scores(self, content_embedding: list[float]) -> list[tuple[str, float]]:
        """为单个内容向量匹配标签并返回相似度分数"""

        if not content_embedding or len(content_embedding) != TAG_EMBEDDING_DIM:
            logger.warning(
                f"Invalid content embedding dimension: {len(content_embedding) if content_embedding else 'empty'}"
            )
            return []

        try:

            tag_embeddings = self._get_tag_embeddings()
            if not tag_embeddings:
                logger.warning("No tag embeddings available")
                return []

            similarities = []
            for tag_id, tag_embedding in tag_embeddings:
                similarity = VectorSimilarity.compute_similarity(
                    content_embedding, tag_embedding, self._similarity_method
                )

                if similarity >= self._threshold:
                    similarities.append((tag_id, similarity))

            similarities.sort(key=lambda x: x[1], reverse=True)

            return similarities[: self._max_tags]

        except Exception as e:
            logger.error(f"Failed to match tags with scores: {e}")
            return []
