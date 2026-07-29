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

class BatchTagMatchMixin:
    """封装 TagMatcher 的单一职责方法。"""

    def match_batch(self, content_embeddings: list[list[float]]) -> list[list[str]]:
        """批量匹配标签"""
        if not content_embeddings:
            return []

        valid_indices = []
        valid_embeddings = []
        for i, embedding in enumerate(content_embeddings):
            if embedding and len(embedding) == TAG_EMBEDDING_DIM:
                valid_indices.append(i)
                valid_embeddings.append(embedding)
            else:
                logger.warning(
                    f"Invalid embedding at index {i}: dimension={len(embedding) if embedding else 'empty'}"
                )

        if not valid_embeddings:
            return [[] for _ in range(len(content_embeddings))]

        try:

            tag_embeddings = self._get_tag_embeddings()
            if not tag_embeddings:
                logger.warning("No tag embeddings available")
                return [[] for _ in range(len(content_embeddings))]

            all_matched_tags = []
            for embedding in valid_embeddings:
                matched_tags = []
                for tag_id, tag_embedding in tag_embeddings:
                    similarity = VectorSimilarity.compute_similarity(
                        embedding, tag_embedding, self._similarity_method
                    )

                    if similarity >= self._threshold:
                        matched_tags.append((tag_id, similarity))

                matched_tags.sort(key=lambda x: x[1], reverse=True)
                tag_ids = [tag_id for tag_id, _ in matched_tags[: self._max_tags]]
                all_matched_tags.append(tag_ids)

            final_results: list[list[str]] = [[] for _ in range(len(content_embeddings))]
            for idx, tags in zip(valid_indices, all_matched_tags, strict=False):
                final_results[idx] = tags

            logger.debug(
                f"Batch matched tags for {len(valid_embeddings)} embeddings "
                f"(threshold={self._threshold:.2f})"
            )

            return final_results

        except Exception as e:
            logger.error(f"Failed to batch match tags: {e}")
            return [[] for _ in range(len(content_embeddings))]

    def match_batch_with_scores(
        self, content_embeddings: list[list[float]]
    ) -> list[list[tuple[str, float]]]:
        """批量匹配标签并返回相似度分数"""
        if not content_embeddings:
            return []

        valid_indices = []
        valid_embeddings = []
        for i, embedding in enumerate(content_embeddings):
            if embedding and len(embedding) == TAG_EMBEDDING_DIM:
                valid_indices.append(i)
                valid_embeddings.append(embedding)
            else:
                logger.warning(
                    f"Invalid embedding at index {i}: dimension={len(embedding) if embedding else 'empty'}"
                )

        if not valid_embeddings:
            return [[] for _ in range(len(content_embeddings))]

        try:

            tag_embeddings = self._get_tag_embeddings()
            if not tag_embeddings:
                logger.warning("No tag embeddings available")
                return [[] for _ in range(len(content_embeddings))]

            all_matched_tags = []
            for embedding in valid_embeddings:
                matched_tags = []
                for tag_id, tag_embedding in tag_embeddings:
                    similarity = VectorSimilarity.compute_similarity(
                        embedding, tag_embedding, self._similarity_method
                    )

                    if similarity >= self._threshold:
                        matched_tags.append((tag_id, similarity))

                matched_tags.sort(key=lambda x: x[1], reverse=True)
                all_matched_tags.append(matched_tags[: self._max_tags])

            final_results: list[list[tuple[str, float]]] = [
                [] for _ in range(len(content_embeddings))
            ]
            for idx, tags in zip(valid_indices, all_matched_tags, strict=False):
                final_results[idx] = tags

            return final_results

        except Exception as e:
            logger.error(f"Failed to batch match tags with scores: {e}")
            return [[] for _ in range(len(content_embeddings))]
