"""
Crawler Adapter - 爬虫数据适配器

将爬虫模块的输出转换为标准化的新闻数据格式。

Responsibilities:
    - 解析爬虫 JSON 输出
    - 转换为标准 Article 格式
    - 处理字段映射和默认值
    - 批量转换
"""

import json
import logging
from datetime import datetime
from typing import Any

from app.infrastructure.storage.lancedb import ArticleFields

from ..normalizers import extract_first_sentence, normalize_datetime

logger = logging.getLogger(__name__)


# =============================================================================
# 字段映射配置
# =============================================================================

# 爬虫字段到标准字段的映射
FIELD_MAPPING = {
    # 必填字段
    "id": ArticleFields.NEWS_ID,
    "title": ArticleFields.TITLE,
    "url": ArticleFields.URL,
    # content 和 markdown 都映射到 content_markdown（兼容不同爬虫输出格式）
    "content": ArticleFields.CONTENT_MARKDOWN,
    "markdown": ArticleFields.CONTENT_MARKDOWN,
    # 可选字段
    "publish_date": ArticleFields.PUBLISH_DATE,
    "source": ArticleFields.SOURCE_SITE,
    "author": ArticleFields.AUTHOR,
    "tags": ArticleFields.TAGS,
    "metadata": ArticleFields.METADATA,
}

# 默认值
DEFAULT_VALUES = {
    ArticleFields.SOURCE_SITE: "未知来源",
    ArticleFields.AUTHOR: "未知作者",
    ArticleFields.TAGS: [],
    ArticleFields.CRAWL_VERSION: 1,
}


# =============================================================================
# 爬虫数据适配器
# =============================================================================


class CrawlerAdapter:
    """
    爬虫数据适配器

    将爬虫模块的原始输出转换为标准化的新闻数据格式。

    Usage:
        >>> adapter = CrawlerAdapter()
        >>> articles = adapter.convert_batch(crawler_data)
        >>> # 或从文件读取
        >>> articles = adapter.load_from_file("crawler_output.json")
    """

    def __init__(
        self,
        field_mapping: dict[str, str] | None = None,
        default_values: dict[str, Any] | None = None,
    ):
        """
        初始化适配器

        Args:
            field_mapping: 字段映射配置
            default_values: 默认值配置
        """
        self._field_mapping = field_mapping or FIELD_MAPPING
        self._default_values = default_values or DEFAULT_VALUES

    def convert_one(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """
        转换单条爬虫数据

        Args:
            raw_data: 原始爬虫数据

        Returns:
            标准化的新闻数据
        """
        result = {}

        # 应用字段映射
        for src_field, dst_field in self._field_mapping.items():
            if src_field in raw_data:
                value = raw_data[src_field]

                # 特殊处理日期字段
                if dst_field == ArticleFields.PUBLISH_DATE:
                    value = normalize_datetime(value)

                result[dst_field] = value

        # Title回退功能：如果没有title，从内容中提取第一句作为标题
        if ArticleFields.TITLE not in result or not result[ArticleFields.TITLE]:
            content_markdown = result.get(ArticleFields.CONTENT_MARKDOWN, "")
            if content_markdown:
                # 从markdown内容中提取第一句作为标题
                fallback_title = extract_first_sentence(
                    content_markdown, is_markdown=True, max_title_length=100
                )
                if fallback_title:
                    result[ArticleFields.TITLE] = fallback_title
                    logger.info(f"使用回退标题: {fallback_title[:50]}...")

        # 应用默认值
        for field, default in self._default_values.items():
            if field not in result or result[field] is None:
                result[field] = default

        # 生成 news_id (如果不存在)
        if ArticleFields.NEWS_ID not in result or not result[ArticleFields.NEWS_ID]:
            result[ArticleFields.NEWS_ID] = self._generate_news_id(result)

        # 设置时间戳
        result[ArticleFields.LAST_UPDATED] = datetime.now()

        # content_text 现在是可选字段，不再自动生成
        # 如果外部需要纯文本，可以在使用时进行转换

        return result

    def convert_batch(self, raw_data_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        批量转换爬虫数据

        Args:
            raw_data_list: 原始爬虫数据列表

        Returns:
            标准化的新闻数据列表
        """
        return [self.convert_one(raw) for raw in raw_data_list]

    def load_from_file(self, filepath: str) -> list[dict[str, Any]]:
        """
        从 JSON 文件加载并转换爬虫数据

        Args:
            filepath: JSON 文件路径

        Returns:
            标准化的新闻数据列表
        """
        try:
            with open(filepath, encoding="utf-8") as f:
                raw_data = json.load(f)

            if isinstance(raw_data, dict):
                # 如果是单个对象，包装成列表
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
        """
        将标准化数据保存到 JSON 文件

        Args:
            articles: 标准化新闻数据列表
            filepath: 输出文件路径
            indent: JSON 缩进
        """
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(articles, f, ensure_ascii=False, indent=indent, default=str)
            logger.info(f"Saved {len(articles)} articles to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save to file {filepath}: {e}")
            raise

    # =========================================================================
    # 辅助方法
    # =========================================================================

    def _generate_news_id(self, data: dict[str, Any]) -> str:
        """
        生成新闻 ID

        基于 URL 哈希生成唯一的新闻 ID（用户要求：id由url通过hash）

        Args:
            data: 标准化数据

        Returns:
            新闻 ID (16字符MD5哈希)
        """
        import hashlib

        url = data.get(ArticleFields.URL, "")
        if not url:
            # 如果URL为空，使用标题作为备选
            title = data.get(ArticleFields.TITLE, "")
            url = title or str(datetime.now().timestamp())

        # 直接使用URL的MD5哈希，截取前16个字符
        return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]  # noqa: S324

    def validate_conversion(self, raw_data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
                验证转换是否成功

                Args:
        1
                    raw_data: 原始爬虫数据

                Returns:
                    (是否成功, 错误信息列表)
        """
        errors = []

        # 检查必填字段
        required_fields = ["title", "url", "content"]
        errors = [
            f"Missing required field: {field}"
            for field in required_fields
            if field not in raw_data or not raw_data[field]
        ]

        # 检查 URL 格式
        if "url" in raw_data:
            from ..validators import validate_url

            if not validate_url(raw_data["url"]):
                errors.append("Invalid URL format")

        return len(errors) == 0, errors


# =============================================================================
# 便捷函数
# =============================================================================


def convert_crawler_data(raw_data: dict[str, Any]) -> dict[str, Any]:
    """
    快速转换爬虫数据

    Args:
        raw_data: 原始爬虫数据

    Returns:
        标准化的新闻数据
    """
    adapter = CrawlerAdapter()
    return adapter.convert_one(raw_data)


def load_crawler_file(filepath: str) -> list[dict[str, Any]]:
    """
    快速从文件加载爬虫数据

    Args:
        filepath: JSON 文件路径

    Returns:
        标准化的新闻数据列表
    """
    adapter = CrawlerAdapter()
    return adapter.load_from_file(filepath)


def save_articles_file(articles: list[dict[str, Any]], filepath: str) -> None:
    """
    快速保存文章数据到文件

    Args:
        articles: 文章数据列表
        filepath: 输出文件路径
    """
    adapter = CrawlerAdapter()
    adapter.save_to_file(articles, filepath)
