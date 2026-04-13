import importlib
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling.bfs_strategy import BFSDeepCrawlStrategy

if __package__:
    _crawl4ai_config_utils = importlib.import_module(".crawl4ai_config_utils", __package__)
else:
    _crawl4ai_config_utils = importlib.import_module("crawl4ai_config_utils")

normalize_crawler_overrides = _crawl4ai_config_utils.normalize_crawler_overrides


class ArticleUrlCrawler:
    """Crawl one or more article URLs and return normalized result objects."""

    def __init__(
        self,
        config_dir: str | None = None,
        cache_base_directory: str | None = None,
    ) -> None:
        self.base_script_path = Path(__file__).resolve().parent
        if config_dir is None:
            self.config_dir = self.base_script_path.parent / "config_data"
        else:
            self.config_dir = Path(config_dir).resolve()

        self.cache_base_directory = (
            Path(cache_base_directory).resolve()
            if cache_base_directory
            else self.base_script_path.parent / "tmp" / "crawl4ai_cache"
        )
        self.cache_base_directory.mkdir(parents=True, exist_ok=True)

        self.browser_config: BrowserConfig | None = None
        self.crawler_config: CrawlerRunConfig | None = None
        self._crawler_instance: AsyncWebCrawler | None = None

        self.logger = self._setup_logger()

    async def __aenter__(self) -> "ArticleUrlCrawler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def start(self) -> None:
        if self._crawler_instance is None:
            self._crawler_instance = AsyncWebCrawler(
                config=self.browser_config,
                base_directory=str(self.cache_base_directory),
            )
            await self._crawler_instance.start()
            self.logger.info("Crawl4AI article crawler started")

    async def close(self) -> None:
        if self._crawler_instance is not None:
            await self._crawler_instance.close()
            self._crawler_instance = None
            self.logger.info("Crawl4AI article crawler closed")

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("article_url_crawler")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(fmt)
            # Fix Windows GBK encoding issue with Unicode characters
            console_handler.stream.reconfigure(encoding="utf-8", errors="replace")
            logger.addHandler(console_handler)
        return logger

    def _load_yaml_config(self, filepath: Path) -> dict[str, Any]:
        with open(filepath, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}

    def _create_crawler_config(self, config_data: dict[str, Any]) -> CrawlerRunConfig:
        config_data = normalize_crawler_overrides(dict(config_data), self.logger)

        return CrawlerRunConfig(**config_data)

    def _init_configs(self) -> None:
        browser_config_path = self.config_dir / "browser.yaml"
        crawler_config_path = self.config_dir / "crawler.yaml"

        if browser_config_path.exists():
            browser_data = self._load_yaml_config(browser_config_path)
            self.browser_config = BrowserConfig(**browser_data.get("browser", {}))
        else:
            self.browser_config = BrowserConfig()

        if crawler_config_path.exists():
            crawler_data = self._load_yaml_config(crawler_config_path)
            self.crawler_config = self._create_crawler_config(crawler_data.get("crawler", {}))
        else:
            self.crawler_config = CrawlerRunConfig()

    def load_website_config(self, website_name: str) -> dict[str, Any]:
        config_path = self.config_dir / "websites" / f"{website_name}.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"Website config not found: {config_path}")
        return self._load_yaml_config(config_path)

    def _merge_crawler_configs(
        self,
        base_config: CrawlerRunConfig,
        overrides: dict[str, Any],
    ) -> CrawlerRunConfig:
        merged_config = base_config.clone()
        overrides = normalize_crawler_overrides(dict(overrides), self.logger)
        deep_crawl_config = overrides.get("deep_crawl_strategy")

        for key, value in overrides.items():
            if key == "deep_crawl_strategy":
                continue
            if hasattr(merged_config, key):
                setattr(merged_config, key, value)

        if deep_crawl_config and isinstance(deep_crawl_config, dict):
            if deep_crawl_config.get("enabled", False):
                merged_config.deep_crawl_strategy = BFSDeepCrawlStrategy(
                    max_depth=deep_crawl_config.get("max_depth", 3),
                    max_pages=deep_crawl_config.get("max_pages", 100),
                    include_external=not deep_crawl_config.get("same_domain_only", True),
                )
            else:
                merged_config.deep_crawl_strategy = None

        return merged_config

    def load_config(
        self,
        target: str | list[str],
        is_website_config: bool = False,
        override_config: dict[str, Any] | None = None,
    ) -> tuple[list[str], CrawlerRunConfig, BrowserConfig]:
        self._init_configs()

        urls_to_crawl: list[str]
        run_config = self.crawler_config.clone() if self.crawler_config else CrawlerRunConfig()
        browser_config = self.browser_config.clone() if self.browser_config else BrowserConfig()

        if is_website_config:
            if not isinstance(target, str):
                raise TypeError("target must be website name str when is_website_config=True")
            website_cfg = self.load_website_config(target).get("website", {})
            urls_to_crawl = website_cfg.get("start_urls", [])
            overrides = website_cfg.get("overrides", {})
            # Try both "article_crawler" (website-specific) and "crawler" (generic) keys
            article_overrides = overrides.get("article_crawler", overrides.get("crawler", {}))
            run_config = self._merge_crawler_configs(run_config, article_overrides)
            for key, value in overrides.get("browser", {}).items():
                if hasattr(browser_config, key):
                    setattr(browser_config, key, value)
        else:
            urls_to_crawl = [target] if isinstance(target, str) else target
            if override_config:
                run_config = self._merge_crawler_configs(
                    run_config, override_config.get("crawler", {})
                )
                for key, value in override_config.get("browser", {}).items():
                    if hasattr(browser_config, key):
                        setattr(browser_config, key, value)

        return urls_to_crawl, run_config, browser_config

    async def crawl_article(
        self,
        url: str,
        run_config: CrawlerRunConfig,
    ) -> dict[str, Any]:
        if not self._crawler_instance:
            await self.start()

        crawler = self._crawler_instance
        if crawler is None:
            raise RuntimeError("Crawler instance is not initialized")

        # Check if css_selector is configured for two-pass extraction
        css_selector = getattr(run_config, "css_selector", None)

        if css_selector:
            # Two-pass approach:
            # 1. First pass: NO css_selector to get full HTML with title, date
            # 2. Second pass: specific css_selector for content-only markdown
            meta_only_config = CrawlerRunConfig(
                page_timeout=getattr(run_config, "page_timeout", 30000),
                cache_mode=getattr(run_config, "cache_mode", None),
                check_cache_freshness=getattr(run_config, "check_cache_freshness", False),
            )
            result_with_meta = await crawler.arun(url=url, config=meta_only_config)

            # Extract title and date from first pass HTML
            inner = result_with_meta._results[0] if hasattr(result_with_meta, "_results") else result_with_meta
            full_html = inner.html if hasattr(inner, "html") else ""

            title = ""
            publish_date = ""

            if full_html:
                metadata = self._extract_metadata(
                    full_html,
                    title_selectors=[
                        ".Article_Title",
                        ".News-title",
                        "h1",
                        ".article-title",
                        "[class*=title]",
                    ],
                    date_selectors=[".Article_PublishDate", ".publish-date", ".date", "time"],
                    author_selectors=[".author", ".Article_Author", ".writer"],
                )
                title = metadata.get("title", "")
                publish_date = metadata.get("date", "")

            # Second pass: content-only with css_selector
            # Note: markdown_generator is NOT passed here because crawl4ai doesn't use it
            # when css_selector is set. We manually generate markdown from html instead.
            content_only_config = CrawlerRunConfig(
                css_selector=css_selector,
                page_timeout=getattr(run_config, "page_timeout", 30000),
                cache_mode=getattr(run_config, "cache_mode", None),
                check_cache_freshness=getattr(run_config, "check_cache_freshness", False),
            )
            result_content = await crawler.arun(url=url, config=content_only_config)

            # Combine results - use title/date from first pass, content from second
            combined_result = result_content._results[0] if hasattr(result_content, "_results") else result_content

            # Manually generate markdown from html using TablePreservingMarkdownGenerator
            # This ensures rowspan/colspan tables are preserved as raw HTML
            raw_html = getattr(combined_result, "html", "") or ""
            md_generator = getattr(run_config, "markdown_generator", None)
            generated_markdown = ""
            if raw_html and md_generator:
                try:
                    # Extract content using css_selector from raw_html
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(raw_html, "html.parser")
                    selected = soup.select_one(css_selector)
                    if selected:
                        content_html = str(selected)
                    else:
                        content_html = raw_html

                    # Generate markdown using our custom generator
                    generated_markdown = md_generator.generate(content_html, source_url=url)
                    if hasattr(generated_markdown, 'markdown'):
                        generated_markdown = generated_markdown.markdown
                except Exception as e:
                    self.logger.warning(f"Failed to generate markdown: {e}")

            if hasattr(combined_result, "html") and full_html:
                # Put the full HTML back so _format_result can use it
                combined_result.html = full_html
            # Store pre-extracted title/date for _format_result via result attributes
            if title or publish_date:
                combined_result._pre_extracted_title = title
                combined_result._pre_extracted_date = publish_date

            # Get formatted result from _format_result
            formatted = self._format_result(combined_result)

            # Override markdown with our generated markdown if available
            if generated_markdown:
                formatted["markdown"] = generated_markdown

            return formatted
        else:
            # Single pass without css_selector
            res = await crawler.arun(url=url, config=run_config)
            return self._format_result(res)

    async def crawl_articles(
        self,
        urls: list[str],
        run_config: CrawlerRunConfig,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for url in urls:
            try:
                results.append(await self.crawl_article(url=url, run_config=run_config))
            except Exception as exc:
                self.logger.error("Failed to crawl article %s: %s", url, exc)
                results.append(
                    {
                        "success": False,
                        "url": url,
                        "title": "",
                        "publish_date": "",
                        "author": "",
                        "error": str(exc),
                        "markdown": "",
                        "metadata": {
                            "crawled_at": datetime.now().isoformat(),
                            "word_count": 0,
                            "is_pdf": False,
                            "depth": 0,
                        },
                        "pdf_size": 0,
                    }
                )
        return results

    def _format_result(self, result: Any) -> dict[str, Any]:
        if isinstance(result, dict):
            defaults = {
                "success": False,
                "url": "",
                "title": "",
                "publish_date": "",
                "author": "",
                "content": "",
                "markdown": "",
                "error": result.get("error", "Unknown internal error"),
                "metadata": {
                    "crawled_at": datetime.now().isoformat(),
                    "word_count": 0,
                    "is_pdf": False,
                    "depth": 0,
                },
                "pdf_size": 0,
            }
            for key, value in defaults.items():
                result.setdefault(key, value)
            return result

        markdown_raw = ""
        markdown_fit = ""
        markdown_citations = ""
        markdown_refs = ""

        markdown_obj = getattr(result, "markdown", None)
        if markdown_obj:
            markdown_raw = getattr(markdown_obj, "raw_markdown", "") or ""
            markdown_fit = getattr(markdown_obj, "fit_markdown", "") or ""
            markdown_citations = getattr(markdown_obj, "markdown_with_citations", "") or ""
            markdown_refs = getattr(markdown_obj, "references_markdown", "") or ""

        if not markdown_raw and hasattr(result, "markdown_v2") and result.markdown_v2:
            markdown_raw = getattr(result.markdown_v2, "raw_markdown", "") or ""
        if not markdown_raw and isinstance(markdown_obj, str):
            markdown_raw = markdown_obj

        url = getattr(result, "url", "")
        success = getattr(result, "success", False)
        error_msg = getattr(result, "error_message", None)

        # Trust crawl4ai's markdown output - no need to re-process
        markdown_result = getattr(result, "markdown", "") or ""
        content_result = getattr(result, "content", "") or ""

        # Use pre-extracted title/publish_date from two-pass approach if available
        pre_extracted_title = getattr(result, "_pre_extracted_title", "") or ""
        pre_extracted_date = getattr(result, "_pre_extracted_date", "") or ""

        formatted = {
            "success": success,
            "url": url,
            "title": pre_extracted_title,
            "publish_date": pre_extracted_date,
            "author": "",
            "content": content_result,
            "markdown": markdown_result,
            "raw_markdown": markdown_raw,
            "fit_markdown": markdown_fit,
            "markdown_with_citations": markdown_citations,
            "references_markdown": markdown_refs,
            "metadata": {
                "crawled_at": datetime.now().isoformat(),
                "word_count": getattr(result, "word_count", 0) or 0,
                "is_pdf": getattr(result, "pdf", None) is not None
                or (url and url.lower().endswith(".pdf")),
                "depth": getattr(result, "depth", 0) or 0,
                "cache_status": getattr(result, "cache_status", ""),
            },
            "pdf_size": len(result.pdf) if getattr(result, "pdf", None) else 0,
        }

        if not success:
            formatted["error"] = error_msg or "Crawl failed without specific error message"

        # Extract metadata using CSS selectors from raw HTML (which contains full page)
        # Skip if we already have title/date from two-pass approach
        raw_html = getattr(result, "html", "") or ""
        if raw_html and not pre_extracted_title and not pre_extracted_date:
            metadata = self._extract_metadata(
                raw_html,
                title_selectors=[
                    ".Article_Title",
                    ".News-title",
                    "h1",
                    ".article-title",
                    "[class*=title]",
                ],
                date_selectors=[".Article_PublishDate", ".publish-date", ".date", "time"],
                author_selectors=[".author", ".Article_Author", ".writer"],
            )
            # Only use extracted title if result.title is empty
            if not formatted.get("title") and metadata.get("title"):
                formatted["title"] = metadata.get("title")
            # Use extracted date if available
            if metadata.get("date"):
                formatted["publish_date"] = metadata.get("date")
            # Use extracted author if available
            if metadata.get("author"):
                formatted["author"] = metadata.get("author")

        # If we extracted title and/or date from HTML, clean the markdown
        # to remove title/date prefix that appears when using target_elements
        if formatted.get("title") or formatted.get("publish_date"):
            markdown_text = str(formatted.get("markdown", ""))
            cleaned_markdown = self._clean_markdown_content(
                markdown_text,
                title=formatted.get("title", ""),
                date=formatted.get("publish_date", ""),
            )
            formatted["markdown"] = cleaned_markdown
            formatted["content"] = cleaned_markdown

        # If title is still empty, try to extract from markdown content
        # This is more reliable when CSS selector filters out title elements
        if not formatted.get("title") and formatted.get("markdown"):
            markdown_text = str(formatted.get("markdown", ""))
            title_from_content = self._extract_title_from_content(markdown_text)
            if title_from_content:
                formatted["title"] = title_from_content
            else:
                # Last resort: use first sentence of markdown as title
                # Split by Chinese period 。 or English period .
                sentences = re.split(r'[。.]', markdown_text)
                if sentences:
                    first_sentence = sentences[0].strip()
                    # Skip if it's just an image link or heading
                    if first_sentence and not first_sentence.startswith("!["):
                        # Limit title length to 100 chars
                        if len(first_sentence) > 100:
                            first_sentence = first_sentence[:100] + "..."
                        formatted["title"] = first_sentence

        # If date is empty, try to extract from markdown content
        # JWC articles often have dates like "5月28日" in the content
        if not formatted.get("publish_date") and formatted.get("markdown"):
            markdown_text = str(formatted.get("markdown", ""))
            # Try to find date pattern like "X月X日" or "XXXX年XX月XX日"
            date_match = re.search(r'(\d{1,2}年)?\d{1,2}月\d{1,2}[日号]', markdown_text)
            if date_match:
                formatted["publish_date"] = date_match.group(0)

        return formatted

    def _extract_metadata(
        self,
        html_content: str,
        title_selectors: list[str] | None = None,
        date_selectors: list[str] | None = None,
        author_selectors: list[str] | None = None,
    ) -> dict[str, str]:
        """
        Extract title, date, and author from HTML using CSS selectors.

        Args:
            html_content: Raw HTML content to parse
            title_selectors: List of CSS selectors for title (tried in order)
            date_selectors: List of CSS selectors for date (tried in order)
            author_selectors: List of CSS selectors for author (tried in order)

        Returns:
            Dict with extracted title, date, author (empty string if not found)
        """
        if not html_content:
            return {"title": "", "date": "", "author": ""}

        soup = BeautifulSoup(html_content, "html.parser")
        result_data = {"title": "", "date": "", "author": ""}

        # Extract title
        if title_selectors:
            for selector in title_selectors:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text:
                        # Clean up the title - remove excessive whitespace
                        text = re.sub(r"\s+", " ", text)
                        result_data["title"] = text
                        break

        # Fallback: try <title> tag if no title found via CSS selectors
        if not result_data["title"]:
            title_tag = soup.find("title")
            if title_tag:
                text = title_tag.get_text(strip=True)
                if text:
                    text = re.sub(r"\s+", " ", text)
                    result_data["title"] = text

        # Extract date
        if date_selectors:
            for selector in date_selectors:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text:
                        # Clean up the date
                        text = re.sub(r"\s+", " ", text)
                        result_data["date"] = text
                        break

        # Extract author
        if author_selectors:
            for selector in author_selectors:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    if text:
                        # Clean up the author
                        text = re.sub(r"\s+", " ", text)
                        result_data["author"] = text
                        break

        return result_data

    def _extract_title_from_content(self, markdown_content: str) -> str:
        """
        Extract title from markdown content if not found via CSS selector.
        Looks for patterns like 【报告题目】 or # heading at the start.

        Args:
            markdown_content: Markdown content

        Returns:
            Extracted title or empty string
        """
        if not markdown_content:
            return ""

        lines = markdown_content.strip().split("\n")

        # Pattern 1: 【题目】标题 at the beginning (e.g., 【报告题目】完美神奇的数字6)
        for line in lines[:5]:
            line = line.strip()
            # Match 【类别】标题 pattern - title is after the first 】
            match = re.search(r"【[^】]+】(.+)", line)
            if match:
                title = match.group(1).strip()
                if title and len(title) > 2:
                    return title
            # Also try simple 【】 pattern if no category
            match2 = re.search(r"【(.+)】", line)
            if match2:
                title = match2.group(1).strip()
                # Skip if it looks like a category (short) rather than a title
                if title and len(title) > 3:
                    return title

        # Pattern 2: Markdown heading # at the beginning
        for line in lines[:3]:
            line = line.strip()
            if line.startswith("#"):
                # Remove markdown heading syntax
                title = re.sub(r"^#+\s*", "", line).strip()
                if title and len(title) > 2:
                    return title

        return ""

    def _clean_markdown_content(
        self,
        markdown_text: str,
        title: str = "",
        date: str = "",
    ) -> str:
        """
        Clean markdown by removing title/date prefix and navigation tables.

        When target_elements is used, the markdown includes title/date in table
        format at the beginning, followed by navigation tables. This method
        removes those and keeps only the article content.

        Args:
            markdown_text: The markdown text to clean
            title: The title to remove (if present in markdown)
            date: The date to remove (if present in markdown)

        Returns:
            Cleaned markdown text
        """
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

            # Skip title/date prefix lines at the beginning
            if skip_until_content:
                # Check if this is a title/date line
                if title and title in stripped:
                    i += 1
                    continue
                if date and date in stripped and i <= 4:
                    i += 1
                    continue
                # Skip separator lines
                if re.match(r"^\s*[\|\-]+\s*$", stripped):
                    i += 1
                    continue
                # Skip empty lines
                if not stripped:
                    i += 1
                    continue
                # Found first content line
                skip_until_content = False
                found_content = True

            # After finding content, skip navigation tables (lines starting with | |)
            if found_content:
                # Check if this is a navigation-related table row
                # Navigation tables typically have patterns like:
                # "|  text  |" followed by list items
                if stripped.startswith("|  |"):
                    # Check if next few lines are list items (navigation)
                    j = i + 1
                    nav_count = 0
                    while j < len(lines) and lines[j].strip().startswith("|"):
                        nav_count += 1
                        j += 1
                    # If many consecutive table rows, it's navigation
                    if nav_count > 3:
                        i = j
                        continue

                # Skip lines that are just dates (duplicates)
                if stripped == date or stripped == f"|  {date}  |":
                    i += 1
                    continue

            result_lines.append(line)
            i += 1

        return "\n".join(result_lines).strip()
