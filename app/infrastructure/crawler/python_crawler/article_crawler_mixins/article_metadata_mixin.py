from __future__ import annotations

from app.infrastructure.crawler.python_crawler.article_url_crawler import (
    Any,
    AsyncWebCrawler,
    BFSDeepCrawlStrategy,
    BeautifulSoup,
    BrowserConfig,
    CrawlerRunConfig,
    Path,
    datetime,
    importlib,
    logging,
    normalize_crawler_overrides,
    re,
    yaml,
)

class ArticleMetadataMixin:
    """封装 ArticleUrlCrawler 的单一职责方法。"""

    def _extract_metadata(
        self,
        html_content: str,
        title_selectors: list[str] | None = None,
        date_selectors: list[str] | None = None,
        author_selectors: list[str] | None = None,
    ) -> dict[str, str]:
        if not html_content:
            return {"title": "", "date": "", "author": ""}

        soup = BeautifulSoup(html_content, "html.parser")
        result_data = {"title": "", "date": "", "author": ""}

        if title_selectors:
            for selector in title_selectors:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text:

                        text = re.sub(r"\s+", " ", text)
                        result_data["title"] = text
                        break

        if not result_data["title"]:
            title_tag = soup.find("title")
            if title_tag:
                text = title_tag.get_text(strip=True)
                if text:
                    text = re.sub(r"\s+", " ", text)
                    result_data["title"] = text

        if date_selectors:
            for selector in date_selectors:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text:

                        text = re.sub(r"\s+", " ", text)
                        result_data["date"] = text
                        break

        if author_selectors:
            for selector in author_selectors:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text:

                        text = re.sub(r"\s+", " ", text)
                        result_data["author"] = text
                        break

        return result_data

    def _extract_title_from_content(self, markdown_content: str) -> str:
        """Looks for patterns like 【报告题目】 or # heading at the start."""
        if not markdown_content:
            return ""

        lines = markdown_content.strip().split("\n")

        for line in lines[:5]:
            line = line.strip()

            match = re.search(r"【[^】]+】(.+)", line)
            if match:
                title = match.group(1).strip()
                if title and len(title) > 2:
                    return title

            match2 = re.search(r"【(.+)】", line)
            if match2:
                title = match2.group(1).strip()

                if title and len(title) > 3:
                    return title

        for line in lines[:3]:
            line = line.strip()
            if line.startswith("#"):

                title = re.sub(r"^#+\s*", "", line).strip()
                if title and len(title) > 2:
                    return title

        return ""
