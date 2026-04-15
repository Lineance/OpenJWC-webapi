from __future__ import annotations

import concurrent.futures
import math
import time
from typing import Any

import pytest

from app.infrastructure.storage.lancedb.connection import get_connection
from app.infrastructure.storage.lancedb.repository import (
    _extract_label,
    _to_notice_item,
)
from app.infrastructure.storage.lancedb.schema import (
    CONTENT_EMBEDDING_DIM,
    TITLE_EMBEDDING_DIM,
    ArticleFields,
)

EMBED_TITLE = [0.0] * TITLE_EMBEDDING_DIM
EMBED_CONTENT = [0.0] * CONTENT_EMBEDDING_DIM


def _legacy_notice_sort_key(record: dict[str, Any]) -> tuple[str, str]:
    publish_date = record.get(ArticleFields.PUBLISH_DATE)
    if publish_date is None:
        date_value = ""
    elif hasattr(publish_date, "isoformat"):
        date_value = str(publish_date.isoformat())
    else:
        date_value = str(publish_date)

    return date_value, str(record.get(ArticleFields.NEWS_ID, ""))


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    rank = int(math.ceil((pct / 100.0) * len(sorted_values))) - 1
    rank = max(0, min(rank, len(sorted_values) - 1))
    return sorted_values[rank]


def _legacy_list_for_notices(
    repo: Any,
    limit: int,
    offset: int,
    label: str | None,
) -> tuple[int, list[dict[str, Any]]]:
    """Simulate pre-optimization baseline: full scan + python sort/filter/page."""
    docs = repo.table.search().to_list()
    if label is not None:
        docs = [doc for doc in docs if _extract_label(doc) == label]

    docs.sort(key=_legacy_notice_sort_key, reverse=True)
    total = len(docs)
    page_docs = docs[offset : offset + limit]
    return total, [_to_notice_item(doc) for doc in page_docs]


def _benchmark(
    func: Any,
    *,
    iterations: int,
    warmup: int,
) -> dict[str, float]:
    for _ in range(warmup):
        func()

    samples_ms: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        samples_ms.append((time.perf_counter() - start) * 1000.0)

    return {
        "p50_ms": _percentile(samples_ms, 50),
        "p95_ms": _percentile(samples_ms, 95),
        "p99_ms": _percentile(samples_ms, 99),
    }


def _seed_articles(repo: Any, *, total: int = 2500) -> None:
    rows: list[dict[str, Any]] = []
    for i in range(total):
        label = f"label-{i % 8}"
        rows.append(
            {
                ArticleFields.NEWS_ID: f"perf-notice-{i:06d}",
                ArticleFields.TITLE: f"性能回归测试资讯 {i}",
                ArticleFields.PUBLISH_DATE: f"2026-04-{(i % 28) + 1:02d}T08:00:00Z",
                ArticleFields.URL: f"https://example.com/perf/{i}",
                ArticleFields.SOURCE_SITE: label,
                ArticleFields.AUTHOR: "perf",
                ArticleFields.TAGS: [label],
                ArticleFields.CONTENT_MARKDOWN: "perf markdown",
                ArticleFields.CONTENT_TEXT: f"perf text {i}",
                ArticleFields.TITLE_EMBEDDING: EMBED_TITLE,
                ArticleFields.CONTENT_EMBEDDING: EMBED_CONTENT,
                ArticleFields.CRAWL_VERSION: 1,
                ArticleFields.METADATA: {
                    "label": label,
                    "detail_url": f"https://example.com/perf/{i}",
                    "is_page": True,
                },
                ArticleFields.ATTACHMENTS: [],
            }
        )

    inserted = repo.add(rows)
    assert inserted == total
    get_connection().rebuild_article_order()


@pytest.mark.slow
def test_notices_perf_large_dataset_and_deep_page(article_repository: Any) -> None:
    """Compare optimized vs legacy baseline under large data and deep pagination."""
    _seed_articles(article_repository, total=2500)

    size = 20
    deep_offset = 20 * 80  # page=81

    optimized_stats = _benchmark(
        lambda: article_repository.list_for_notices(
            limit=size, offset=deep_offset, label=None
        ),
        iterations=40,
        warmup=5,
    )
    legacy_stats = _benchmark(
        lambda: _legacy_list_for_notices(
            article_repository, limit=size, offset=deep_offset, label=None
        ),
        iterations=20,
        warmup=3,
    )

    print("\n[perf][deep-page] optimized:", optimized_stats)
    print("[perf][deep-page] legacy:", legacy_stats)

    assert optimized_stats["p95_ms"] < legacy_stats["p95_ms"]
    assert optimized_stats["p99_ms"] < legacy_stats["p99_ms"]


@pytest.mark.slow
def test_notices_perf_high_concurrency(article_repository: Any) -> None:
    """Compare optimized vs legacy baseline under concurrent notice reads."""
    _seed_articles(article_repository, total=2200)

    limit = 20
    offset = 0
    workers = 12
    requests_per_worker = 12

    def run_concurrent(callable_fn: Any) -> dict[str, float]:
        latencies_ms: list[float] = []

        def one_call() -> None:
            start = time.perf_counter()
            callable_fn(limit=limit, offset=offset, label=None)
            latencies_ms.append((time.perf_counter() - start) * 1000.0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(one_call) for _ in range(workers * requests_per_worker)
            ]
            for future in futures:
                future.result()

        return {
            "p95_ms": _percentile(latencies_ms, 95),
            "p99_ms": _percentile(latencies_ms, 99),
        }

    optimized_stats = run_concurrent(article_repository.list_for_notices)
    legacy_stats = run_concurrent(
        lambda **kwargs: _legacy_list_for_notices(article_repository, **kwargs)
    )

    print("\n[perf][concurrency] optimized:", optimized_stats)
    print("[perf][concurrency] legacy:", legacy_stats)

    assert optimized_stats["p95_ms"] < legacy_stats["p95_ms"]
    assert optimized_stats["p99_ms"] < legacy_stats["p99_ms"]
