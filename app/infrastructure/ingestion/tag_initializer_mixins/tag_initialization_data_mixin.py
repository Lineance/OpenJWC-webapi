from __future__ import annotations

from app.infrastructure.ingestion.tag_initializer import (
    Any,
    Path,
    TagConfigLoader,
    TagRecord,
    argparse,
    datetime,
    get_embedder,
    get_tag_repository,
    logger,
    logging,
    sys,
    yaml,
)

class TagInitializationDataMixin:
    """封装 TagInitializer 的单一职责方法。"""

    def _generate_tag_embeddings(
        self, tag_definitions: list[dict[str, Any]]
    ) -> list[TagRecord]:
        """为标签定义生成向量嵌入"""
        tag_records = []

        for tag_def in tag_definitions:
            try:

                tag_id = tag_def["id"]
                tag_name = tag_def["name"]
                tag_description = tag_def.get("description", "")
                category = tag_def.get("category", "general")

                description_text = f"{tag_name}: {tag_description}"

                embedding = self._embedder.embed_contents([description_text])[0]

                now = datetime.now()

                tag_record = TagRecord(
                    tag_id=tag_id,
                    name=tag_name,
                    description=tag_description,
                    category=category,
                    embedding=embedding,
                    created_at=now,
                    updated_at=now,
                )

                tag_records.append(tag_record)
                logger.debug(f"Generated embedding for tag: {tag_name}")

            except Exception as e:
                logger.error(
                    f"Failed to generate embedding for tag {tag_def.get('id', 'unknown')}: {e}"
                )

        logger.info(f"Generated embeddings for {len(tag_records)} tags")
        return tag_records

    def _save_tags(self, tag_records: list[TagRecord]) -> int:
        """保存标签到数据库"""
        if not tag_records:
            return 0

        try:

            saved_count = int(self._repository.add_batch(tag_records))

            if saved_count == len(tag_records):
                logger.info(f"Successfully saved all {saved_count} tags")
            else:
                logger.warning(
                    f"Partial save: {saved_count}/{len(tag_records)} tags saved"
                )

            return saved_count
        except Exception as e:
            logger.error(f"Failed to save tags: {e}")
            return 0
