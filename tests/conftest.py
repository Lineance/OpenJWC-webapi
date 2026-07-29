from __future__ import annotations

from typing import Any

def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        "--run-real-web",
        action="store_true",
        default=False,
        help="Run real network crawler integration tests marked as real_web.",
    )
