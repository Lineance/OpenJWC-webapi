from __future__ import annotations

import argparse
import json
import sqlite3
import time
from typing import Any

from app.core.config import CRAWLER_BIN, NOTICE_JSON, SQLITE_DB_PATH

SQLITE_USER_STATE_TABLES = [
    "api_keys",
    "admin_users",
    "system_settings",
    "mottos",
    "submissions",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run real smoke check for Rust crawler pipeline"
    )
    parser.add_argument(
        "--mode",
        choices=["real"],
        default="real",
        help="Execution mode. Only real mode is supported.",
    )
    parser.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Strict mode: fail on any ingestion error or DB invariant mismatch.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed diagnostics.",
    )
    return parser.parse_args()


def _print_ok(message: str) -> None:
    print(f"[SMOKE][OK] {message}")


def _print_step(message: str) -> None:
    print(f"[SMOKE][STEP] {message}")


def _print_fail(message: str) -> None:
    print(f"[SMOKE][FAIL] {message}")


def _table_exists(cursor: sqlite3.Cursor, table: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1", (table,)
    )
    return cursor.fetchone() is not None


def snapshot_sqlite_user_state() -> dict[str, int]:
    snapshot: dict[str, int] = {}
    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        cursor = conn.cursor()
        for table in SQLITE_USER_STATE_TABLES:
            if not _table_exists(cursor, table):
                snapshot[table] = -1
                continue
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            snapshot[table] = int(cursor.fetchone()[0])
    return snapshot


def snapshot_lancedb_state() -> dict[str, int]:
    from app.infrastructure.storage.lancedb.repository import get_article_repository

    repo = get_article_repository()
    return {"articles": int(repo.count())}


def validate_binary() -> None:
    if not CRAWLER_BIN.exists():
        raise FileNotFoundError(f"Crawler binary not found: {CRAWLER_BIN}")
    if not CRAWLER_BIN.is_file():
        raise FileNotFoundError(f"Crawler binary path is not a file: {CRAWLER_BIN}")
    if not CRAWLER_BIN.stat().st_mode & 0o111:
        raise PermissionError(f"Crawler binary is not executable: {CRAWLER_BIN}")


def validate_output_json() -> list[dict[str, Any]]:
    if not NOTICE_JSON.exists():
        raise FileNotFoundError(f"Crawler output not found: {NOTICE_JSON}")

    data = json.loads(NOTICE_JSON.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Crawler output JSON must be a list")
    return [item for item in data if isinstance(item, dict)]


def run_real_smoke(strict: bool, verbose: bool) -> int:
    from app.infrastructure.crawler import rust_crawler_wrapper as crawler

    started = time.time()

    _print_step("Collecting DB baseline snapshots")
    sqlite_before = snapshot_sqlite_user_state()
    lancedb_before = snapshot_lancedb_state()
    if verbose:
        print(f"[SMOKE][DATA] sqlite_before={sqlite_before}")
        print(f"[SMOKE][DATA] lancedb_before={lancedb_before}")

    try:
        _print_step("Validating crawler binary")
        validate_binary()
        _print_ok(f"Crawler binary ready: {CRAWLER_BIN}")

        _print_step("Executing Rust crawler task")
        result = crawler.execute_crawling_task()
        if verbose:
            stdout_preview = (result.stdout or "").strip()[:400]
            stderr_preview = (result.stderr or "").strip()[:400]
            print(f"[SMOKE][DATA] crawler_stdout={stdout_preview}")
            print(f"[SMOKE][DATA] crawler_stderr={stderr_preview}")
        _print_ok("Rust crawler execution completed")

        _print_step("Validating crawler output JSON")
        raw_docs = validate_output_json()
        _print_ok(f"Output JSON loaded, docs={len(raw_docs)}")

        _print_step("Running ingestion pipeline")
        docs = crawler.adapter.load_from_file(str(NOTICE_JSON))
        pipeline_result = crawler.pipeline.process_batch(docs)
        _print_ok(
            "Ingestion done: "
            f"total={pipeline_result.total}, success={pipeline_result.success}, "
            f"duplicate={pipeline_result.duplicate}, invalid={pipeline_result.invalid}, error={pipeline_result.error}"
        )

        _print_step("Collecting DB snapshots after execution")
        sqlite_after = snapshot_sqlite_user_state()
        lancedb_after = snapshot_lancedb_state()
        if verbose:
            print(f"[SMOKE][DATA] sqlite_after={sqlite_after}")
            print(f"[SMOKE][DATA] lancedb_after={lancedb_after}")

        # SQLite user-state should not be modified by crawler smoke.
        sqlite_changed = {
            key: (sqlite_before[key], sqlite_after.get(key, -1))
            for key in sqlite_before
            if sqlite_before[key] != sqlite_after.get(key, -1)
        }
        if sqlite_changed:
            raise AssertionError(
                f"SQLite user-state changed unexpectedly: {sqlite_changed}"
            )

        # LanceDB invariants
        if lancedb_after["articles"] < lancedb_before["articles"]:
            raise AssertionError(
                "LanceDB articles count regressed: "
                f"before={lancedb_before['articles']} after={lancedb_after['articles']}"
            )

        if strict:
            if pipeline_result.error > 0:
                raise AssertionError(
                    f"Ingestion error count > 0: {pipeline_result.error}"
                )
            if pipeline_result.invalid > 0:
                raise AssertionError(
                    f"Ingestion invalid count > 0 in strict mode: {pipeline_result.invalid}"
                )

        elapsed = time.time() - started
        print(
            "[SMOKE][SUMMARY] "
            + json.dumps(
                {
                    "elapsed_seconds": round(elapsed, 2),
                    "pipeline": {
                        "total": pipeline_result.total,
                        "success": pipeline_result.success,
                        "duplicate": pipeline_result.duplicate,
                        "invalid": pipeline_result.invalid,
                        "error": pipeline_result.error,
                    },
                    "lancedb_delta": {
                        "articles": lancedb_after["articles"]
                        - lancedb_before["articles"],
                    },
                    "sqlite_changed": sqlite_changed,
                },
                ensure_ascii=False,
            )
        )
        _print_ok("Real smoke passed")
        return 0

    except Exception as exc:
        _print_fail(f"{type(exc).__name__}: {exc}")
        return 1


def main() -> int:
    args = parse_args()
    return run_real_smoke(strict=args.strict, verbose=args.verbose)


if __name__ == "__main__":
    raise SystemExit(main())
