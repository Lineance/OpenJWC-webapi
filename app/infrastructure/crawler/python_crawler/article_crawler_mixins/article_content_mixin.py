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

class ArticleContentMixin:
    """封装 ArticleUrlCrawler 的单一职责方法。"""

    def _clean_markdown_content(
        self,
        markdown_text: str,
        title: str = "",
        date: str = "",
    ) -> str:
        if not markdown_text:
            return markdown_text

        lines = markdown_text.split("\n")
        result_lines = []
        skip_until_content = True
        found_content = False

        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            if skip_until_content:

                if title and title in stripped:
                    i += 1
                    continue
                if date and date in stripped and i <= 4:
                    i += 1
                    continue

                if re.match(r"^\s*[\|\-]+\s*$", stripped):
                    i += 1
                    continue

                if not stripped:
                    i += 1
                    continue

                skip_until_content = False
                found_content = True

            if found_content:

                if stripped.startswith("|  |"):

                    j = i + 1
                    nav_count = 0
                    while j < len(lines) and lines[j].strip().startswith("|"):
                        nav_count += 1
                        j += 1

                    if nav_count > 3:
                        i = j
                        continue

                if stripped == date or stripped == f"|  {date}  |":
                    i += 1
                    continue

            result_lines.append(line)
            i += 1

        return "\n".join(result_lines).strip()
