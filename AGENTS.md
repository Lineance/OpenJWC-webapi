# OpenJWC WebAPI

## Package Management

**Use `uv`, not pip** - project uses uv.lock.

- Default install (cloud embedding path): `uv pip install -e .`
- Local embedding install (sentence-transformers/torch): `uv pip install -e ".[local]"`

Python 3.12 required (see `.python-version`)

## Running the App

**Dev server:** `uvicorn main:app --reload --host 0.0.0.0 --port 8000`

**Production:** `uvicorn main:app --host 0.0.0.0 --port 8000`

Entry point is `main.py` - includes lifespan handler that syncs admins and checks network health

## CLI Admin Tool

Run `python main.py` directly to enter CLI (not uvicorn) for admin operations:

```
create user <name> <max_devices>  # Create API key
create admin <username> <password> # Create admin
show apikeys                       # List all keys
crawl                              # Run crawler job
sync                               # Sync admins from admins.json
```

See `app/utils/openjwc_cli.py` for full command reference

## Architecture

**Client APIs** (`app/api/v1/client/`): Public endpoints, optional API key auth

- Auth: `verify_api_key` (strict) or `optional_verify_api_key` (respects `notices_auth` setting)
- Headers: `Authorization: Bearer <token>`, `X-Device-ID: <uuid>`

**Admin APIs** (`app/api/v1/admin/`): Protected endpoints with JWT auth

- Auth: `verify_admin_token` via OAuth2PasswordBearer
- Token URL: `/api/v1/admin/auth/login`
- Default admin: see `admins.json` (username: admin, password: Admin@12345)

**Services** (`app/services/`): Business logic layer

- `sql_db_service.py`: SQLite user/system state operations via mixins
- `ai_service.py`: DeepSeek LLM with tenacity retry
- `vector_db_service.py`: ChromaDB with ZhipuAI embeddings

## Key Configuration

- Admin accounts synced from `admins.json` on startup via `db.sync_admins_from_config()`
- System settings stored in DB, see `app/core/config.py` for defaults (`ALLOWED_SETTINGS`)
- LanceDB at `data/lancedb` for crawler/article primary storage
- SQLite DB at `data/jwc_notices.db` for user/system/submission state
- ChromaDB at `data/chroma_db`
- Crawler binary: `bin/jwc-crawler` (external)

## Authentication Patterns

**Client API key flow:** `db.validate_and_use_key()` auto-binds device if under limit
**Admin JWT:** 5-minute expiry, uses `SECRET_KEY` from `app/core/security.py` (change in prod)

## Logging

All routes use `LoggingRoute` class for request logging. Use `setup_logger("log_name")` for consistent structured logging to `logs/app.log` and `logs/error.log`

## Network Dependencies

Startup checks connectivity to `https://api.deepseek.com` and `https://open.bigmodel.cn`. Fails gracefully but logs warning.

## No Tests

No test framework configured. Testing commands not available.
