"""Crawler Adapter - 爬虫数据适配器"""

import json
import logging
from datetime import datetime
from typing import Any

from app.infrastructure.storage.lancedb import ArticleFields

from ..normalizers import extract_first_sentence, normalize_datetime

logger = logging.getLogger(__name__)

FIELD_MAPPING = {

    "id": ArticleFields.NEWS_ID,
    "title": ArticleFields.TITLE,
    "url": ArticleFields.URL,
    "detail_url": ArticleFields.URL,

    "content": ArticleFields.CONTENT_MARKDOWN,
    "markdown": ArticleFields.CONTENT_MARKDOWN,

    "publish_date": ArticleFields.PUBLISH_DATE,
    "date": ArticleFields.PUBLISH_DATE,
    "source": ArticleFields.SOURCE_SITE,
    "source_site": ArticleFields.SOURCE_SITE,
    "author": ArticleFields.AUTHOR,
    "tags": ArticleFields.TAGS,
    "metadata": ArticleFields.METADATA,
}

DEFAULT_VALUES = {
    ArticleFields.SOURCE_SITE: "未知来源",
    ArticleFields.AUTHOR: "未知作者",
    ArticleFields.TAGS: [],
    ArticleFields.CRAWL_VERSION: 1,
}

from .crawler_adapter_mixins.crawler_conversion_mixin import CrawlerConversionMixin
from .crawler_adapter_mixins.crawler_file_mixin import CrawlerFileMixin
from .crawler_adapter_mixins.crawler_validation_mixin import CrawlerValidationMixin

class CrawlerAdapter(CrawlerConversionMixin, CrawlerFileMixin, CrawlerValidationMixin):
    """爬虫数据适配器"""

    def __init__(
        self,
        field_mapping: dict[str, str] | None = None,
        default_values: dict[str, Any] | None = None,
    ) -> None:
        """初始化适配器"""
        self._field_mapping = field_mapping or FIELD_MAPPING
        self._default_values = default_values or DEFAULT_VALUES

def convert_crawler_data(raw_data: dict[str, Any]) -> dict[str, Any]:
    """快速转换爬虫数据"""
    adapter = CrawlerAdapter()
    return adapter.convert_one(raw_data)

def load_crawler_file(filepath: str) -> list[dict[str, Any]]:
    """快速从文件加载爬虫数据"""
    adapter = CrawlerAdapter()
    return adapter.load_from_file(filepath)

def save_articles_file(articles: list[dict[str, Any]], filepath: str) -> None:
    """快速保存文章数据到文件"""
    adapter = CrawlerAdapter()
    adapter.save_to_file(articles, filepath)
