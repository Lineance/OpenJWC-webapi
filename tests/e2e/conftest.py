from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

# Ensure project root is importable when running from tests/e2e.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Keep e2e smoke offline by stubbing optional heavy integrations.
if "app.infrastructure.crawler.rust_crawler_wrapper" not in sys.modules:
    crawler_stub = types.ModuleType("app.infrastructure.crawler.rust_crawler_wrapper")

    def _noop_run_crawler_job() -> None:
        return None

    crawler_stub.run_crawler_job = _noop_run_crawler_job
    sys.modules["app.infrastructure.crawler.rust_crawler_wrapper"] = crawler_stub

if "app.application.chat.ai_service" not in sys.modules:
    chat_stub = types.ModuleType("app.application.chat.ai_service")

    def _noop_reinitialize_client() -> None:
        return None

    chat_stub.reinitialize_client = _noop_reinitialize_client
    sys.modules["app.application.chat.ai_service"] = chat_stub

if "sentence_transformers" not in sys.modules:
    st_stub = types.ModuleType("sentence_transformers")

    class _SentenceTransformerStub:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def encode(self, texts, **kwargs):
            if isinstance(texts, str):
                return [0.1] * 384
            return [[0.1] * 384 for _ in texts]

    st_stub.SentenceTransformer = _SentenceTransformerStub
    sys.modules["sentence_transformers"] = st_stub

from app.api.v1.admin import apikeys as admin_apikeys
from app.api.v1.admin import auth as admin_auth
from app.api.v1.admin import settings as admin_settings
from app.api.v1.admin import submission as admin_submission
from app.api.v1.client import notices as client_notices
from app.api.v1.client import register as client_register
from app.api.v1.client import search as client_search
from app.api.v1.client import submission as client_submission
from app.infrastructure.storage.sqlite.sql_db_service import db


@pytest.fixture
def admin_credentials() -> dict[str, str]:
    return {"username": "e2e_admin", "password": "E2E@12345"}


@pytest.fixture
def isolated_db(tmp_path: Path, admin_credentials: dict[str, str]) -> None:
    original_db_path = db.db_path
    db.db_path = tmp_path / "e2e_smoke.db"
    db.init_db()
    db._sync_settings()
    db.create_admin(admin_credentials["username"], admin_credentials["password"])
    yield
    db.db_path = original_db_path


@pytest.fixture
def test_app(isolated_db: None) -> FastAPI:
    app = FastAPI(title="openjwc-e2e-tests")
    app.include_router(client_register.router, prefix="/api/v1/client")
    app.include_router(client_notices.router, prefix="/api/v1/client")
    app.include_router(client_search.router, prefix="/api/v1/client")
    app.include_router(client_submission.router, prefix="/api/v1/client")

    app.include_router(admin_auth.router, prefix="/api/v1/admin")
    app.include_router(admin_settings.router, prefix="/api/v1/admin")
    app.include_router(admin_apikeys.router, prefix="/api/v1/admin")
    app.include_router(admin_submission.router, prefix="/api/v1/admin")
    return app


@pytest.fixture
async def async_client(test_app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
