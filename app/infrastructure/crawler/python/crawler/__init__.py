"""
配置化爬虫模块
基于crawl4ai的配置驱动爬虫，支持自动增量更新和多格式输出
"""

__version__ = "1.0.0"
__author__ = "Crawler Module"
__description__ = "基于crawl4ai的配置化爬虫模块，支持自动增量更新"

from .article_url_crawler import ArticleUrlCrawler
from .list_incremental_crawler import ListIncrementalCrawler

__all__ = ["ArticleUrlCrawler", "ListIncrementalCrawler"]
