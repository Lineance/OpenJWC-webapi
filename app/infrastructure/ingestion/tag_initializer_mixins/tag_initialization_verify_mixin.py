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

class TagInitializationVerifyMixin:
    """封装 TagInitializer 的单一职责方法。"""

    def _create_indices(self) -> bool:
        """创建标签表的索引"""
        try:
            success = bool(self._repository.create_indices())

            if success:
                logger.info("Successfully created tag indices")
            else:
                logger.warning("Failed to create tag indices")

            return success
        except Exception as e:
            logger.error(f"Failed to create indices: {e}")
            return False

    def _verify_initialization(self, expected_count: int) -> bool:
        """验证初始化结果"""
        try:
            actual_count = self._repository.count()

            if actual_count >= expected_count:
                logger.info(f"Verification passed: {actual_count} tags in database")
                return True
            else:
                logger.warning(
                    f"Verification failed: expected {expected_count}, found {actual_count} tags"
                )
                return False
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return False

    def get_statistics(self) -> dict[str, Any]:
        """获取初始化统计信息"""
        try:
            total_count = self._repository.count()
            category_counts = self._repository.count_by_category()

            return {
                "total_tags": total_count,
                "categories": category_counts,
                "config_path": self.config_path,
            }
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}
