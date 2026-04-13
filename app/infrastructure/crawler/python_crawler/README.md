# Crawler 模块说明

本模块负责以下两阶段流程：

1. 列表页增量发现（按站点配置发现文章 URL，做本地去重）
2. 文章页抓取与标准化输出（内容、markdown、元数据）

## 目录结构

- `src/list_incremental_crawler.py`：列表页增量抓取核心类
- `src/article_url_crawler.py`：文章抓取核心类
- `src/list_to_articles_e2e.py`：端到端 CLI 入口
- `src/crawl4ai_config_utils.py`：Crawl4AI 配置归一化（cache_mode/markdown_generator/content_filter）
- `test/`：单元测试与真实网页集成测试

## 快速开始

在仓库根目录执行：

```powershell
cd backend
.\.venv\Scripts\python.exe crawler\src\list_to_articles_e2e.py --website jwc --max-pages 1 --output ..\tmp\jwc_full_pull.json
```

按单个列表 URL 运行：

```powershell
cd backend
.\.venv\Scripts\python.exe crawler\src\list_to_articles_e2e.py --list-url https://jwc.seu.edu.cn/jwxx/list.htm --max-pages 3 --output ..\tmp\out.json
```

## CLI 接口（list_to_articles_e2e.py）

### 必选输入（二选一）

- `--website`：网站配置名（来自 `config/websites/<name>.yaml`）
- `--list-url`：直接指定列表页 URL

### 常用参数

- `--max-pages`：列表翻页上限，默认 `31`
- `--output`：输出 JSON 文件路径
- `--state-file`：增量状态文件（仅 list-url 模式）
- `--cache-dir`：统一指定 Crawl4AI 缓存根目录（建议显式传，避免多目录缓存）
- `--include-pattern` / `--exclude-pattern`：URL 过滤正则，可重复传入

### 运行时覆盖参数（JSON 字符串）

- `--list-crawler-overrides`：覆盖列表阶段 `CrawlerRunConfig`
- `--article-crawler-overrides`：覆盖文章阶段 `CrawlerRunConfig`
- `--browser-overrides`：覆盖 `BrowserConfig`

示例：强制文章阶段绕过缓存，避免旧缓存影响 markdown 质量

```powershell
cd backend
.\.venv\Scripts\python.exe crawler\src\list_to_articles_e2e.py --website jwc --max-pages 1 --article-crawler-overrides "{\"cache_mode\":\"BYPASS\",\"check_cache_freshness\":false}" --output ..\tmp\jwc_full_pull.json
```

## Python 接口

### ListIncrementalCrawler

文件：`src/list_incremental_crawler.py`

核心方法：

- `crawl_list_incremental(list_url, max_pages=31, include_patterns=None, exclude_patterns=None, state_file_path=None, run_config=None, initialize=True) -> list[str]`
  - 输入：列表页 URL 与过滤规则
  - 输出：本次新增文章 URL 列表（已去重）

- `crawl_website_incremental(website_name, max_pages=None, include_patterns=None, exclude_patterns=None, list_crawler_overrides=None, article_crawler_overrides=None, browser_overrides=None) -> dict`
  - 输入：网站配置名与覆盖参数
  - 输出：
    - `lists`：每个列表源的统计
    - `incremental_urls`：全站增量 URL
    - `article_overrides`：传递给文章阶段的配置（crawler/browser）

### ArticleUrlCrawler

文件：`src/article_url_crawler.py`

核心方法：

- `load_config(target, is_website_config=False, override_config=None) -> (urls, run_config, browser_config)`
- `crawl_article(url, run_config) -> dict`
- `crawl_articles(urls, run_config) -> list[dict]`

文章结果字段（关键）：

- `content`：优先 `fit_html`，否则 `cleaned_html`
- `markdown`：优先 `fit_markdown`；若过短自动回退 `raw_markdown`
- `raw_markdown`：原始 markdown
- `fit_markdown`：过滤后 markdown
- `markdown_with_citations` / `references_markdown`
- `metadata.cache_status`：缓存命中状态（如 `miss`、`hit`、`hit_validated`）

## 网站配置接口（config/websites/<name>.yaml）

示例结构（以 jwc 为例）：

```yaml
website:
  start_urls: [...]
  list_incremental:
    max_pages: 31
    state_file: "tmp/jwc_seen_urls.json"
    cache_base_directory: "tmp/crawl4ai_jwc_cache"
    include_patterns: [...]
    exclude_patterns: [...]

  overrides:
    list_crawler:
      cache_mode: "ENABLED"
      check_cache_freshness: true
      ...

    article_crawler:
      cache_mode: "ENABLED"
      css_selector: ".wp_articlecontent, .Article_Content"
      target_elements: [...]
      markdown_generator:
        type: "default"
        content_source: "cleaned_html"
        options: {...}
        content_filter:
          type: "pruning" # pruning|bm25|llm
          params: {...}

    browser:
      headless: true
      light_mode: true
```

说明：

- `list_crawler` 与 `article_crawler` 已解耦，可独立调优
- `crawl4ai_config_utils.py` 会自动把字符串 `cache_mode`、`markdown_generator`、`content_filter` 转成 Crawl4AI 对象

## 缓存与状态文件

### 增量状态文件（URL 去重）

- 由 `list_incremental.state_file` 指定基名
- 实际会按列表 URL 分片，生成哈希后缀文件
- 作用：决定 `incremental_urls` 是否为 0

### Crawl4AI 缓存目录

可能出现两个目录的原因：

- 列表阶段可从网站配置 `list_incremental.cache_base_directory` 覆盖
- 文章阶段若未传 `--cache-dir`，会使用 `ArticleUrlCrawler` 默认目录

建议：始终传 `--cache-dir`，统一缓存目录，便于排查与清理。

## 常见问题

1. `incremental_urls=0` 但日志显示 discovered>0
- 原因：状态文件中已存在这些 URL（正常增量行为）

2. markdown 质量忽好忽坏
- 常见原因：命中旧缓存
- 排查：查看 `metadata.cache_status`
- 处理：文章阶段临时使用 `cache_mode=BYPASS`

3. 想要更干净正文
- 优先配置 `article_crawler.css_selector`
- 配置 `target_elements`
- 使用 `markdown_generator.content_filter`（Pruning/BM25）

## 测试

在仓库根目录执行：

```powershell
make backend-lint
```

可选：运行 crawler 相关测试（按项目现有 pytest 配置）。
