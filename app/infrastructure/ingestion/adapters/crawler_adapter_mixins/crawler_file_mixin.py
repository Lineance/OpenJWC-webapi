from __future__ import annotations

from app.infrastructure.ingestion.adapters.crawler import (
    Any,
    ArticleFields,
    DEFAULT_VALUES,
    FIELD_MAPPING,
    datetime,
    extract_first_sentence,
    json,
    logger,
    logging,
    normalize_datetime,
)

class CrawlerFileMixin:
    """封装 CrawlerAdapter 的单一职责方法。"""

    def convert_batch(
        self, raw_data_list: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """批量转换爬虫数据"""
        return [self.convert_one(raw) for raw in raw_data_list]

    def load_from_file(self, filepath: str) -> list[dict[str, Any]]:
        """从 JSON 文件加载并转换爬虫数据"""
        try:
            with open(filepath, encoding="utf-8") as f:
                raw_data = json.load(f)

            if isinstance(raw_data, dict):

                raw_data = [raw_data]
            elif not isinstance(raw_data, list):
                raise ValueError(
                    f"Invalid JSON format: expected list or dict, got {type(raw_data)}"
                )

            return self.convert_batch(raw_data)

        except Exception as e:
            logger.error(f"Failed to load from file {filepath}: {e}")
            raise

    def save_to_file(
        self,
        articles: list[dict[str, Any]],
        filepath: str,
        indent: int = 2,
    ) -> None:
        """将标准化数据保存到 JSON 文件"""
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(articles, f, ensure_ascii=False, indent=indent, default=str)
            logger.info(f"Saved {len(articles)} articles to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save to file {filepath}: {e}")
            raise
