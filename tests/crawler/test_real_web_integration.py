from __future__ import annotations

from typing import TYPE_CHECKING

import httpx
import pytest
from crawl4ai import CacheMode

from app.infrastructure.crawler.python_crawler.article_url_crawler import (
    ArticleUrlCrawler,
)

if TYPE_CHECKING:
    from pathlib import Path


REAL_LIST_URL = "https://jwc.seu.edu.cn/jwxx/list.htm"
REAL_ARTICLE_URL = "https://jwc.seu.edu.cn/2026/0228/c21678a556262/page.htm"
REAL_ARTICLE_PATH = "/2026/0228/c21678a556262/page.htm"


pytestmark = pytest.mark.real_web


@pytest.mark.asyncio
async def test_real_list_page_contains_target_article_link() -> None:
    list_pages = [REAL_LIST_URL] + [
        REAL_LIST_URL.replace("list.htm", f"list{page}.htm") for page in range(2, 11)
    ]

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        found = False
        for page_url in list_pages:
            response = await client.get(page_url)
            if response.status_code != 200:
                continue
            if REAL_ARTICLE_URL in response.text or REAL_ARTICLE_PATH in response.text:
                found = True
                break

    # Real-world assertion: target article is discoverable from the first paginated list pages.
    assert found is True


@pytest.mark.asyncio
async def test_real_article_content_fetch(tmp_path: Path) -> None:
    crawler = ArticleUrlCrawler(cache_base_directory=str(tmp_path / "cache"))

    async with crawler:
        _, run_config, _ = crawler.load_config(target=[REAL_ARTICLE_URL])
        run_config.cache_mode = CacheMode.ENABLED
        run_config.check_cache_freshness = True
        result = await crawler.crawl_article(REAL_ARTICLE_URL, run_config=run_config)

    assert result["url"] == REAL_ARTICLE_URL
    assert result["success"] is True
    assert len(result.get("markdown", "")) > 100 or len(result.get("content", "")) > 200
