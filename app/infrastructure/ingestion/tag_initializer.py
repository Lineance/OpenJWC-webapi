"""
Tag Initializer - 标签系统初始化脚本

从配置文件加载预定义标签，生成向量表示，并初始化数据库。

Responsibilities:
    - 加载预定义标签配置 (YAML)
    - 为标签描述生成向量嵌入
    - 初始化 TagRepository 数据库
    - 创建向量索引和全文索引
    - 验证系统完整性

Usage:
    >>> python -m backend.ingestion.tag_initializer
    >>> # 或从命令行
    >>> python tag_initializer.py --config config/tags.yaml
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.infrastructure.ingestion.embedder_provider import get_embedder
from app.infrastructure.storage.lancedb.tag_repository import get_tag_repository
from app.infrastructure.storage.lancedb.tag_schema import TagRecord

logger = logging.getLogger(__name__)


# =============================================================================
# 配置加载器
# =============================================================================


class TagConfigLoader:
    """标签配置加载器"""

    @staticmethod
    def load_config(config_path: str) -> dict[str, Any]:
        """
        加载标签配置文件

        Args:
            config_path: 配置文件路径

        Returns:
            配置字典
        """
        try:
            with open(config_path, encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
                config = loaded if isinstance(loaded, dict) else {}
                logger.info(f"Loaded tag config from {config_path}")
                return config
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise

    @staticmethod
    def parse_tags(config: dict[str, Any]) -> list[dict[str, Any]]:
        """
        解析标签配置

        Args:
            config: 配置字典

        Returns:
            标签定义列表
        """
        tags_raw = config.get("tags", [])
        manual_tags_raw = config.get("manual_tags", [])
        tags = tags_raw if isinstance(tags_raw, list) else []
        manual_tags = manual_tags_raw if isinstance(manual_tags_raw, list) else []

        # 合并自动和手动标签
        all_tags = [t for t in (tags + manual_tags) if isinstance(t, dict)]
        logger.info(
            f"Parsed {len(all_tags)} tags ({len(tags)} auto, {len(manual_tags)} manual)"
        )
        return all_tags


# =============================================================================
# 标签初始化器
# =============================================================================


class TagInitializer:
    """
    标签系统初始化器

    加载配置、生成向量、初始化数据库。
    """

    def __init__(
        self,
        config_path: str = "config/tags.yaml",
        clear_existing: bool = False,
        create_indices: bool = True,
    ):
        """
        初始化标签初始化器

        Args:
            config_path: 配置文件路径
            clear_existing: 是否清空现有标签
            create_indices: 是否创建索引
        """
        self.config_path = config_path
        self.clear_existing = clear_existing
        self.create_indices = create_indices

        # 初始化组件
        self._loader = TagConfigLoader()
        self._repository = get_tag_repository()
        self._embedder = get_embedder()

        logger.info(
            f"TagInitializer initialized: config={config_path}, "
            f"clear={clear_existing}, create_indices={create_indices}"
        )

    def run(self) -> bool:
        """
        执行标签初始化流程

        Returns:
            是否成功
        """
        try:
            # 1. 加载配置
            config = self._loader.load_config(self.config_path)

            # 2. 解析标签
            tag_definitions = self._loader.parse_tags(config)

            if not tag_definitions:
                logger.warning("No tags found in configuration")
                return False

            # 3. 清空现有标签（如果配置要求）
            if self.clear_existing:
                self._clear_existing_tags()

            # 4. 为标签生成向量嵌入
            tag_records = self._generate_tag_embeddings(tag_definitions)

            # 5. 批量保存标签到数据库
            saved_count = self._save_tags(tag_records)

            # 6. 创建索引
            if self.create_indices and saved_count > 0:
                self._create_indices()

            # 7. 验证初始化结果
            success = self._verify_initialization(saved_count)

            logger.info(f"Tag initialization completed: {saved_count} tags saved")
            return success

        except Exception as e:
            logger.error(f"Tag initialization failed: {e}")
            return False

    def _clear_existing_tags(self) -> bool:
        """清空现有标签"""
        try:
            # 检查是否有现有标签
            existing_count = self._repository.count()
            if existing_count == 0:
                logger.info("No existing tags to clear")
                return True

            logger.warning(f"Clearing {existing_count} existing tags")

            # LanceDB 不支持直接清空，我们可以通过删除并重建表来实现
            # 或者使用标记删除策略
            success = bool(self._repository.clear_all())

            if success:
                logger.info(f"Successfully cleared {existing_count} tags")
            else:
                logger.error("Failed to clear existing tags")

            return success
        except Exception as e:
            logger.error(f"Failed to clear existing tags: {e}")
            return False

    def _generate_tag_embeddings(
        self, tag_definitions: list[dict[str, Any]]
    ) -> list[TagRecord]:
        """
        为标签定义生成向量嵌入

        Args:
            tag_definitions: 标签定义列表

        Returns:
            TagRecord 列表
        """
        tag_records = []

        for tag_def in tag_definitions:
            try:
                # 创建标签描述文本（用于生成向量）
                tag_id = tag_def["id"]
                tag_name = tag_def["name"]
                tag_description = tag_def.get("description", "")
                category = tag_def.get("category", "general")

                # 使用名称和描述生成向量文本
                # 为了提高匹配准确性，我们使用完整的标签描述
                description_text = f"{tag_name}: {tag_description}"

                # 生成向量嵌入（使用内容向量化模型）
                embedding = self._embedder.embed_contents([description_text])[0]

                # 创建时间戳
                now = datetime.now()

                # 创建 TagRecord
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
        """
        保存标签到数据库

        Args:
            tag_records: TagRecord 列表

        Returns:
            成功保存的数量
        """
        if not tag_records:
            return 0

        try:
            # 批量添加标签
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
        """
        验证初始化结果

        Args:
            expected_count: 预期的标签数量

        Returns:
            验证是否成功
        """
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


# =============================================================================
# 命令行接口
# =============================================================================


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="Initialize tag system with predefined tags"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/tags.yaml",
        help="Path to tag configuration file (default: config/tags.yaml)",
    )
    parser.add_argument(
        "--clear", action="store_true", help="Clear existing tags before initialization"
    )
    parser.add_argument("--no-indices", action="store_true", help="Skip index creation")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--stats", action="store_true", help="Show statistics after initialization"
    )
    return parser.parse_args()


def main() -> int:
    """主函数"""
    args = parse_args()

    # 配置日志
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 检查配置文件是否存在
    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        return 1

    try:
        # 初始化标签系统
        initializer = TagInitializer(
            config_path=str(config_path),
            clear_existing=args.clear,
            create_indices=not args.no_indices,
        )

        # 执行初始化
        success = initializer.run()

        # 显示统计信息
        if args.stats and success:
            stats = initializer.get_statistics()
            print("\n=== Tag Initialization Statistics ===")
            print(f"Total tags: {stats.get('total_tags', 0)}")
            print("Category distribution:")
            for category, count in stats.get("categories", {}).items():
                print(f"  - {category}: {count}")

        return 0 if success else 1

    except Exception as e:
        logger.error(f"Tag initialization failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
