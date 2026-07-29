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

class TagInitializationRunMixin:
    """封装 TagInitializer 的单一职责方法。"""

    def run(self) -> bool:
        """执行标签初始化流程"""
        try:

            config = self._loader.load_config(self.config_path)

            tag_definitions = self._loader.parse_tags(config)

            if not tag_definitions:
                logger.warning("No tags found in configuration")
                return False

            if self.clear_existing:
                self._clear_existing_tags()

            tag_records = self._generate_tag_embeddings(tag_definitions)

            saved_count = self._save_tags(tag_records)

            if self.create_indices and saved_count > 0:
                self._create_indices()

            success = self._verify_initialization(saved_count)

            logger.info(f"Tag initialization completed: {saved_count} tags saved")
            return success

        except Exception as e:
            logger.error(f"Tag initialization failed: {e}")
            return False

    def _clear_existing_tags(self) -> bool:
        """清空现有标签"""
        try:

            existing_count = self._repository.count()
            if existing_count == 0:
                logger.info("No existing tags to clear")
                return True

            logger.warning(f"Clearing {existing_count} existing tags")

            success = bool(self._repository.clear_all())

            if success:
                logger.info(f"Successfully cleared {existing_count} tags")
            else:
                logger.error("Failed to clear existing tags")

            return success
        except Exception as e:
            logger.error(f"Failed to clear existing tags: {e}")
            return False
