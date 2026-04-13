import argparse
import asyncio
import importlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

# Fix Windows console GBK encoding issue for Unicode characters (✓, ✗, etc.)
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

if __package__:
    _article_mod = importlib.import_module(".article_url_crawler", __package__)
    _list_mod = importlib.import_module(".list_incremental_crawler", __package__)
else:
    _article_mod = importlib.import_module("article_url_crawler")
    _list_mod = importlib.import_module("list_incremental_crawler")

ArticleUrlCrawler = _article_mod.ArticleUrlCrawler
ListIncrementalCrawler = _list_mod.ListIncrementalCrawler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List URL incremental crawler e2e")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--list-url", help="List page url, e.g. https://jwc.seu.edu.cn/jwxx/list.htm"
    )
    source_group.add_argument(
        "--website", help="Website config name under config/websites, e.g. jwc"
    )
    parser.add_argument("--config-dir", default=None, help="Crawler config directory")
    parser.add_argument("--max-pages", type=int, default=31, help="Max list pages")
    parser.add_argument("--state-file", default=None, help="Incremental state file path")
    parser.add_argument("--cache-dir", default=None, help="Crawl4ai cache base directory")
    parser.add_argument("--output", default=None, help="Optional output json file")
    parser.add_argument(
        "--list-crawler-overrides",
        default=None,
        help="JSON object overrides for list crawl CrawlerRunConfig",
    )
    parser.add_argument(
        "--article-crawler-overrides",
        default=None,
        help="JSON object overrides for article crawl CrawlerRunConfig",
    )
    parser.add_argument(
        "--browser-overrides",
        default=None,
        help="JSON object overrides for BrowserConfig",
    )
    parser.add_argument(
        "--include-pattern",
        action="append",
        default=None,
        help="Regex include pattern for article urls (repeatable)",
    )
    parser.add_argument(
        "--exclude-pattern",
        action="append",
        default=None,
        help="Regex exclude pattern for article urls (repeatable)",
    )
    return parser.parse_args()


def _parse_json_overrides(raw: str | None, arg_name: str) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON for {arg_name}: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"{arg_name} must be a JSON object")
    return data


async def run_e2e(args: argparse.Namespace) -> dict[str, Any]:
    start = time.time()
    list_crawler_overrides = _parse_json_overrides(
        args.list_crawler_overrides, "--list-crawler-overrides"
    )
    article_crawler_overrides = _parse_json_overrides(
        args.article_crawler_overrides, "--article-crawler-overrides"
    )
    browser_overrides = _parse_json_overrides(args.browser_overrides, "--browser-overrides")

    source_mode = "website" if args.website else "list_url"
    lists_summary: list[dict[str, Any]] = []
    website_overrides: dict[str, Any] = {}

    async with ListIncrementalCrawler(
        config_dir=args.config_dir,
        cache_base_directory=args.cache_dir,
        state_file=args.state_file,
    ) as list_crawler:
        if args.website:
            batch_result = await list_crawler.crawl_website_incremental(
                website_name=args.website,
                max_pages=args.max_pages,
                include_patterns=args.include_pattern,
                exclude_patterns=args.exclude_pattern,
                list_crawler_overrides=list_crawler_overrides,
                article_crawler_overrides=article_crawler_overrides,
                browser_overrides=browser_overrides,
            )
            incremental_urls = batch_result["incremental_urls"]
            lists_summary = batch_result["lists"]
            website_overrides = batch_result.get("article_overrides", {})
        else:
            incremental_urls = await list_crawler.crawl_list_incremental(
                list_url=args.list_url,
                max_pages=args.max_pages,
                include_patterns=args.include_pattern,
                exclude_patterns=args.exclude_pattern,
            )
            lists_summary = [
                {
                    "list_url": args.list_url,
                    "incremental_count": len(incremental_urls),
                    "state_file": args.state_file,
                    "incremental_urls": incremental_urls,
                }
            ]
            website_overrides = {
                "crawler": article_crawler_overrides,
                "browser": browser_overrides,
            }

        # Load website config to get source name
    source_name = None
    if args.website:
        website_cfg_path = None
        # repo_root is parents[3] because we need D:\SEU-WuHub not D:\SEU-WuHub\backend
        repo_root = Path(__file__).resolve().parents[3]
        config_dir = Path(args.config_dir).resolve() if args.config_dir else repo_root / "config_data"
        candidates = [
            config_dir / "websites" / f"{args.website}.yaml",
            repo_root / "config" / "websites" / f"{args.website}.yaml",
        ]
        for p in candidates:
            if p.exists():
                website_cfg_path = p
                break
        if website_cfg_path:
            import yaml as yaml_mod
            with open(website_cfg_path, encoding="utf-8") as f:
                website_data = yaml_mod.safe_load(f)
                source_name = website_data.get("website", {}).get("name")

    async with ArticleUrlCrawler(
        config_dir=args.config_dir,
        cache_base_directory=args.cache_dir,
    ) as article_crawler:
        _, run_config, _ = article_crawler.load_config(
            target=incremental_urls,
            override_config=website_overrides,
        )
        results = await article_crawler.crawl_articles(incremental_urls, run_config=run_config)

    # Add source field to each result if website config has source name
    if source_name:
        for item in results:
            if "source" not in item or not item["source"]:
                item["source"] = source_name

    success_count = sum(1 for item in results if item.get("success"))
    failed_count = len(results) - success_count

    summary = {
        "source_mode": source_mode,
        "website": args.website,
        "list_url": args.list_url,
        "lists": lists_summary,
        "incremental_url_count": len(incremental_urls),
        "article_success_count": success_count,
        "article_failed_count": failed_count,
        "elapsed_seconds": round(time.time() - start, 2),
        "results": results,
    }

    return summary


def main() -> None:
    args = parse_args()
    summary = asyncio.run(run_e2e(args))

    if args.output:
        output_path = os.path.abspath(args.output)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        payload = json.dumps(summary, ensure_ascii=False, indent=2)
        _write_output(output_path, payload)

    print(
        "e2e done | incremental_urls={incremental} | success={success} | failed={failed} | elapsed={elapsed}s".format(
            incremental=summary["incremental_url_count"],
            success=summary["article_success_count"],
            failed=summary["article_failed_count"],
            elapsed=summary["elapsed_seconds"],
        )
    )


def _write_output(path: str, payload: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(payload)


if __name__ == "__main__":
    main()
