"""
Admin CLI - 管理员命令行界面

提供交互式命令行界面，支持爬取、查询、管理操作。
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.infrastructure.crawler.python_crawler.article_url_crawler import ArticleUrlCrawler
from app.infrastructure.crawler.python_crawler.list_incremental_crawler import ListIncrementalCrawler
from app.infrastructure.storage.lancedb import init_database
from app.infrastructure.ingestion.adapters.crawler import CrawlerAdapter
from app.infrastructure.ingestion.pipeline import IngestionPipeline
from app.infrastructure.retrieval import create_engine

logger = logging.getLogger(__name__)


class AdminCLI:
    """管理员命令行界面"""

    def __init__(self, config_dir: str | None = None, db_path: str = "data/lancedb"):
        """
        初始化管理员CLI

        Args:
            config_dir: 爬虫配置文件目录
            db_path: 数据库路径
        """
        self.config_dir = config_dir or "config"
        self.db_path = db_path

    def run(self) -> None:
        """运行CLI主循环"""
        parser = self._create_parser()
        args = parser.parse_args()

        if hasattr(args, "func"):
            args.func(args)
        else:
            parser.print_help()

    def _create_parser(self) -> argparse.ArgumentParser:
        """创建命令行解析器"""
        parser = argparse.ArgumentParser(
            description="SEU-WuHub 管理员工具",
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        subparsers = parser.add_subparsers(dest="command", help="可用命令")

        # Crawl command
        self._add_crawl_parser(subparsers)

        # Query command
        self._add_query_parser(subparsers)

        # Stats command
        self._add_stats_parser(subparsers)

        # Interactive shell
        self._add_shell_parser(subparsers)

        return parser

    def _add_crawl_parser(self, subparsers: Any) -> None:
        """添加爬取命令"""
        crawl_parser = subparsers.add_parser("crawl", help="爬取网站数据")
        crawl_parser.add_argument(
            "--website",
            "-w",
            type=str,
            default="jwc",
            help="网站配置名称 (默认: jwc)",
        )
        crawl_parser.add_argument(
            "--max-pages",
            "-m",
            type=int,
            default=31,
            help="最大页数 (默认: 31)",
        )
        crawl_parser.add_argument(
            "--output",
            "-o",
            type=str,
            default=None,
            help="输出JSON文件路径",
        )
        crawl_parser.set_defaults(func=self._handle_crawl)

    def _add_query_parser(self, subparsers: Any) -> None:
        """添加查询命令"""
        query_parser = subparsers.add_parser("query", help="查询文章")
        query_parser.add_argument("keyword", type=str, help="搜索关键词")
        query_parser.add_argument(
            "--type",
            "-t",
            type=str,
            choices=["hybrid", "vector", "fulltext"],
            default="hybrid",
            help="搜索类型 (默认: hybrid)",
        )
        query_parser.add_argument(
            "--limit",
            "-l",
            type=int,
            default=10,
            help="返回结果数量 (默认: 10)",
        )
        query_parser.add_argument(
            "--source",
            "-s",
            type=str,
            default=None,
            help="按来源过滤",
        )
        query_parser.set_defaults(func=self._handle_query)

    def _add_stats_parser(self, subparsers: Any) -> None:
        """添加统计命令"""
        stats_parser = subparsers.add_parser("stats", help="查看统计信息")
        stats_parser.set_defaults(func=self._handle_stats)

    def _add_shell_parser(self, subparsers: Any) -> None:
        """添加交互式shell命令"""
        shell_parser = subparsers.add_parser("shell", help="进入交互模式")
        shell_parser.set_defaults(func=self._handle_shell)

    def _handle_crawl(self, args: argparse.Namespace) -> None:
        """处理爬取命令"""
        print(f"\n{'=' * 60}")
        print(f"开始爬取网站: {args.website}")
        print(f"最大页数: {args.max_pages}")
        print(f"{'=' * 60}\n")

        result = asyncio.run(
            self._crawl_website(
                website_name=args.website,
                max_pages=args.max_pages,
            )
        )

        print(f"\n{'=' * 60}")
        print("爬取完成!")
        print(f"增量URL数量: {result.get('incremental_url_count', 0)}")
        print(f"成功文章: {result.get('article_success_count', 0)}")
        print(f"失败文章: {result.get('article_failed_count', 0)}")
        print(f"耗时: {result.get('elapsed_seconds', 0)}秒")
        print(f"{'=' * 60}\n")

        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"结果已保存到: {output_path}")

    async def _crawl_website(
        self,
        website_name: str,
        max_pages: int = 31,
    ) -> dict[str, Any]:
        """执行网站爬取"""
        import time

        start_time = time.time()

        # Step 1: 爬取列表页，获取增量URL
        async with ListIncrementalCrawler(
            config_dir=self.config_dir,
        ) as list_crawler:
            batch_result = await list_crawler.crawl_website_incremental(
                website_name=website_name,
                max_pages=max_pages,
            )

        incremental_urls = batch_result.get("incremental_urls", [])
        website_overrides = batch_result.get("article_overrides", {})
        source_name = batch_result.get("source")

        if not incremental_urls:
            return {
                "website": website_name,
                "incremental_url_count": 0,
                "article_success_count": 0,
                "article_failed_count": 0,
                "elapsed_seconds": time.time() - start_time,
            }

        print(f"发现 {len(incremental_urls)} 个增量文章")

        # Step 2: 爬取文章详情
        async with ArticleUrlCrawler(
            config_dir=self.config_dir,
        ) as article_crawler:
            _, run_config, _ = article_crawler.load_config(
                target=incremental_urls,
                override_config=website_overrides,
            )
            articles_result = await article_crawler.crawl_articles(
                incremental_urls,
                run_config=run_config,
            )

        success_count = sum(1 for r in articles_result if r.get("success"))
        failed_count = len(articles_result) - success_count

        print(f"文章爬取完成: 成功 {success_count}, 失败 {failed_count}")

        # Add source field to each result if source name is available
        if source_name:
            for item in articles_result:
                if "source" not in item or not item["source"]:
                    item["source"] = source_name

        # Step 3: 导入到数据库
        if success_count > 0:
            print("\n开始导入数据到数据库...")
            init_database(self.db_path)
            adapter = CrawlerAdapter()
            articles = adapter.convert_batch(articles_result)

            pipeline = IngestionPipeline(db_path=self.db_path)
            ingest_result = pipeline.process_batch(articles)

            print(
                f"导入结果: {ingest_result.success} 成功, {ingest_result.invalid} 无效, "
                f"{ingest_result.duplicate} 重复, {ingest_result.error} 错误"
            )

        return {
            "website": website_name,
            "incremental_url_count": len(incremental_urls),
            "article_success_count": success_count,
            "article_failed_count": failed_count,
            "elapsed_seconds": time.time() - start_time,
        }

    def _handle_query(self, args: argparse.Namespace) -> None:
        """处理查询命令"""
        print(f"\n{'=' * 60}")
        print(f"搜索关键词: {args.keyword}")
        print(f"搜索类型: {args.type}")
        print(f"返回数量: {args.limit}")
        if args.source:
            print(f"来源过滤: {args.source}")
        print(f"{'=' * 60}\n")

        try:
            engine = create_engine(db_path=self.db_path)

            # 构建过滤条件
            filters = {}
            if args.source:
                filters["source_site"] = args.source

            # 执行搜索
            results = engine.search(
                query=args.keyword,
                search_type=args.type,
                limit=args.limit,
                **filters,
            )

            # 显示结果
            print(f"找到 {results['total']} 个结果\n")

            for i, item in enumerate(results["results"], 1):
                print(f"{i}. {item.get('title', '无标题')[:60]}")
                print(f"   URL: {item.get('url', 'N/A')}")
                print(f"   来源: {item.get('source_site', 'N/A')}")
                print(f"   日期: {item.get('publish_date', 'N/A')}")
                if "_score" in item:
                    print(f"   相关度: {item['_score']:.4f}")
                print()

        except Exception as e:
            print(f"查询失败: {e}")
            logger.exception("Query error")

    def _handle_stats(self, args: argparse.Namespace) -> None:
        """处理统计命令"""
        print(f"\n{'=' * 60}")
        print("数据库统计信息")
        print(f"{'=' * 60}\n")

        try:
            engine = create_engine(db_path=self.db_path)
            stats = engine.get_statistics()

            print(f"总文档数: {stats.get('total_documents', 0)}")

            # 来源分布
            source_dist = stats.get("source_distribution", {})
            if source_dist:
                print("\n来源分布:")
                for source, count in sorted(source_dist.items(), key=lambda x: -x[1]):
                    print(f"  {source}: {count}")

            # 时间范围
            time_range = stats.get("time_range", {})
            if time_range:
                print("\n时间范围:")
                print(f"  最早: {time_range.get('min', 'N/A')}")
                print(f"  最新: {time_range.get('max', 'N/A')}")

            print()

        except Exception as e:
            print(f"获取统计失败: {e}")
            logger.exception("Stats error")

    def _handle_shell(self, args: argparse.Namespace) -> None:
        """处理交互式shell"""
        print(f"\n{'=' * 60}")
        print("SEU-WuHub 管理员交互模式")
        print("输入 'help' 查看可用命令, 'exit' 退出")
        print(f"{'=' * 60}\n")

        while True:
            try:
                user_input = input("admin> ").strip()

                if not user_input:
                    continue

                if user_input in ("exit", "quit", "q"):
                    print("再见!")
                    break

                if user_input in ("help", "h", "?"):
                    self._print_help()
                    continue

                # 解析简单命令
                parts = user_input.split()
                cmd = parts[0].lower()

                if cmd == "crawl":
                    website = parts[1] if len(parts) > 1 else "jwc"
                    max_pages = int(parts[2]) if len(parts) > 2 else 31
                    result = asyncio.run(self._crawl_website(website, max_pages))
                    print(
                        f"完成! 成功 {result['article_success_count']}/{result['incremental_url_count']}"
                    )

                elif cmd == "query":
                    keyword = " ".join(parts[1:]) if len(parts) > 1 else ""
                    if keyword:
                        engine = create_engine(db_path=self.db_path)
                        results = engine.search(keyword, limit=5)
                        print(f"找到 {results['total']} 个结果:")
                        for i, item in enumerate(results["results"], 1):
                            print(f"  {i}. {item.get('title', 'N/A')[:50]}")
                    else:
                        print("请输入搜索关键词")

                elif cmd == "stats":
                    engine = create_engine(db_path=self.db_path)
                    stats = engine.get_statistics()
                    print(f"总文档数: {stats.get('total_documents', 0)}")

                elif cmd == "list":
                    print("可用命令:")
                    print("  crawl [website] [max_pages] - 爬取网站")
                    print("  query <keyword>          - 搜索文章")
                    print("  stats                   - 查看统计")
                    print("  list                    - 显示此帮助")
                    print("  exit                    - 退出")

                else:
                    print(f"未知命令: {cmd}, 输入 'list' 查看可用命令")

            except KeyboardInterrupt:
                print("\n退出...")
                break
            except Exception as e:
                print(f"错误: {e}")

    def _print_help(self) -> None:
        """打印帮助信息"""
        print("""
可用命令:
  crawl [website] [max_pages] - 爬取网站数据
    例: crawl jwc 10
        crawl

  query <keyword>              - 搜索文章
    例: query 人工智能
        query 学术讲座

  stats                        - 查看数据库统计
  list                         - 显示可用命令
  exit                         - 退出交互模式
        """)


def main() -> None:
    """CLI入口函数"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    cli = AdminCLI()
    cli.run()


if __name__ == "__main__":
    main()

