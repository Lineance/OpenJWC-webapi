"""Tag Initializer - 标签系统初始化脚本"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.infrastructure.ingestion.embedder_provider import get_embedder
from app.infrastructure.storage.lancedb.tag_repository import get_tag_repository
from app.infrastructure.storage.lancedb.tag_schema import TagRecord

logger = logging.getLogger(__name__)

class TagConfigLoader:
    """标签配置加载器"""

    @staticmethod
    def load_config(config_path: str) -> dict[str, Any]:
        """加载标签配置文件"""
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
        """解析标签配置"""
        tags_raw = config.get("tags", [])
        manual_tags_raw = config.get("manual_tags", [])
        tags = tags_raw if isinstance(tags_raw, list) else []
        manual_tags = manual_tags_raw if isinstance(manual_tags_raw, list) else []

        all_tags = [t for t in (tags + manual_tags) if isinstance(t, dict)]
        logger.info(
            f"Parsed {len(all_tags)} tags ({len(tags)} auto, {len(manual_tags)} manual)"
        )
        return all_tags

from .tag_initializer_mixins.tag_initialization_run_mixin import TagInitializationRunMixin
from .tag_initializer_mixins.tag_initialization_data_mixin import TagInitializationDataMixin
from .tag_initializer_mixins.tag_initialization_verify_mixin import TagInitializationVerifyMixin

class TagInitializer(TagInitializationRunMixin, TagInitializationDataMixin, TagInitializationVerifyMixin):
    """标签系统初始化器"""

    def __init__(
        self,
        config_path: str = "config/tags.yaml",
        clear_existing: bool = False,
        create_indices: bool = True,
    ) -> None:
        """初始化标签初始化器"""
        self.config_path = config_path
        self.clear_existing = clear_existing
        self.create_indices = create_indices

        self._loader = TagConfigLoader()
        self._repository = get_tag_repository()
        self._embedder = get_embedder()

        logger.info(
            f"TagInitializer initialized: config={config_path}, "
            f"clear={clear_existing}, create_indices={create_indices}"
        )

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

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        return 1

    try:

        initializer = TagInitializer(
            config_path=str(config_path),
            clear_existing=args.clear,
            create_indices=not args.no_indices,
        )

        success = initializer.run()

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
