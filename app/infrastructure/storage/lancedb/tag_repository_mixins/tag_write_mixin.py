from __future__ import annotations

from app.infrastructure.storage.lancedb.tag_repository import (
    Any,
    LanceDBConnection,
    RepositorySystemError,
    TAGS_TABLE_NAME,
    TAG_EMBEDDING_DIM,
    TagFields,
    TagIndexConfig,
    TagRecord,
    annotations,
    datetime,
    get_connection,
    get_tag_schema,
    lancedb,
    logger,
    logging,
)

class TagWriteMixin:
    """封装 TagRepository 的单一职责方法。"""

    def add_one(self, tag_record: TagRecord) -> bool:
        """添加单个标签"""
        try:
            data = tag_record.to_dict()
            self._table.add([data])
            logger.debug(f"Added tag: {tag_record.tag_id} - {tag_record.name}")
            return True
        except (OSError, PermissionError, IOError) as e:
            logger.error(f"Failed to add tag {tag_record.tag_id}: {e}")
            raise RepositorySystemError(f"Failed to add tag: {e}") from e
        except Exception as e:
            logger.error(f"Failed to add tag {tag_record.tag_id}: {e}")
            return False

    def add_batch(self, tag_records: list[TagRecord]) -> int:
        """批量添加标签"""
        if not tag_records:
            return 0

        try:
            data_list = [tag.to_dict() for tag in tag_records]
            self._table.add(data_list)
            logger.info(f"Added {len(tag_records)} tags")
            return len(tag_records)
        except Exception as e:
            logger.error(f"Failed to add tags: {e}")
            return 0

    def get(self, tag_id: str) -> TagRecord | None:
        """根据 ID 获取标签"""
        try:
            results = (
                self._table.search()
                .where(f"{TagFields.TAG_ID} = '{tag_id}'")
                .limit(1)
                .to_list()
            )
            return TagRecord.from_dict(results[0]) if results else None
        except Exception as e:
            logger.error(f"Failed to get tag {tag_id}: {e}")
            return None

    def get_by_name(self, name: str) -> TagRecord | None:
        """根据名称获取标签"""
        try:

            results = (
                self._table.search()
                .where(f"{TagFields.NAME} = '{name}'")
                .limit(1)
                .to_list()
            )
            return TagRecord.from_dict(results[0]) if results else None
        except Exception as e:
            logger.error(f"Failed to get tag by name '{name}': {e}")
            return None

    def update(self, tag_id: str, updates: dict[str, Any]) -> bool:
        """更新标签"""
        try:

            update_data = updates.copy()
            update_data[TagFields.TAG_ID] = tag_id
            update_data[TagFields.UPDATED_AT] = datetime.now()

            self._table.merge_insert(
                TagFields.TAG_ID
            ).when_matched_update_all().execute([update_data])
            logger.debug(f"Updated tag: {tag_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update tag {tag_id}: {e}")
            return False

    def update_record(self, tag_record: TagRecord) -> bool:
        """更新 TagRecord"""
        return self.update(tag_record.tag_id, tag_record.to_dict())

    def delete(self, tag_id: str) -> bool:
        """删除标签"""
        try:

            logger.warning(
                f"LanceDB doesn't support direct deletion, tag {tag_id} marked for cleanup"
            )

            return self.update(tag_id, {"deleted": True})
        except Exception as e:
            logger.error(f"Failed to delete tag {tag_id}: {e}")
            return False
