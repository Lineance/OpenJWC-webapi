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

## CLI

The Typer CLI mirrors every frontend-facing v1/v2 API and also provides local operations:

```bash
uv run openjwc --help
uv run openjwc client-v1 --help
uv run openjwc admin-v1 --help
uv run openjwc client-v2 --help
uv run openjwc admin-v2 --help
uv run openjwc ops --help
```

`python main.py` and the legacy `SQLCLI().cmdloop()` compatibility wrapper forward to the same CLI. The route mapping is in `app/cli/manifest.py`; full usage and migration details are in `docs/REFACTORING.md`.

## Architecture

**Client APIs** (`app/api/v1/client/`): Public endpoints, optional API key auth

- Auth: `verify_api_key` (strict) or `optional_verify_api_key` (respects `notices_auth` setting)
- Headers: `Authorization: Bearer <token>`, `X-Device-ID: <uuid>`

**Admin APIs** (`app/api/v1/admin/`): Protected endpoints with JWT auth

- Auth: `verify_admin_token` via OAuth2PasswordBearer
- Token URL: `/api/v1/admin/auth/login`
- Default admin: see `admins.json` (username: admin, password: Admin@12345)

**Business layers**:

- `app/application/`: application services and use-case orchestration
- `app/domain/`: submission and user-registration domain models/services
- `app/infrastructure/storage/sqlite/`: SQLite user/system repositories and mixins
- `app/infrastructure/storage/lancedb/`: article/tag storage and retrieval
- `app/infrastructure/agent/`: LLM agent, tools, events and chat integration
- `app/cli/`: Typer HTTP client and local operations

## Key Configuration

- Admin accounts synced from `admins.json` on startup via `db.sync_admins_from_config()`
- System settings stored in DB, see `app/core/config.py` for defaults (`ALLOWED_SETTINGS`)
- LanceDB at `data/lancedb` for crawler/article primary storage
- SQLite DB at `data/jwc_notices.db` for user/system/submission state
- Crawler binary: `bin/jwc-crawler` (external)

## Authentication Patterns

**Client API key flow:** `db.validate_and_use_key()` auto-binds device if under limit
**Admin JWT:** 5-minute expiry, uses `SECRET_KEY` from `app/core/security.py` (change in prod)

## Logging

All routes use `LoggingRoute` class for request logging. Use `setup_logger("log_name")` for consistent structured logging to `logs/app.log` and `logs/error.log`

## Code Style

- 一个 `.py` 文件应尽可能只放 1～2 个类的定义，最好只放 1 个；类定义数量过多时必须按职责拆分到不同文件。
- 一个 `.py` 文件应尽量控制在 200 行以内，最多不得超过 300 行；大型类优先使用职责明确的 mixin 拆分。
- 所有注释必须使用中文，且禁止滥用注释。
  - 禁止段中注释，即注释不得与代码出现在同一行。
  - 每个类、函数（方法）或可能引起困惑的变量最多允许 1 行说明性注释。
  - 每个文件开头最多允许 5 行注释。
  - 禁止无意义 banner 或用于视觉分隔的注释。
  - 测试函数允许使用任意行数的注释详细说明测试内容，但仍禁止段中注释。
- 所有函数必须为每个参数和返回值严格声明类型；`self` 与 `cls` 无需声明类型。
- 优先使用 Python 内置装饰器表达语义：工厂方法使用 `@classmethod`，无状态工具函数使用 `@staticmethod`，只读派生属性使用 `@property`；不得为标记目的引入无行为的自定义装饰器。
- 面向用户的数据结构不得要求调用方直接调用构造函数，必须提供能说明创建语义的 `@classmethod` 工厂，例如 `ValidTime.open_ended`、`AtomicFact.from_extraction`、`MemoryGraph.empty`；状态变换使用名称明确的实例方法，例如 `with_confidence`；构造函数仅供类内部实现使用。
- 单个文件夹中的文件最好不超过 8 个；超过时应优先按功能拆入子目录，也可在职责一致时适当合并或移动文件。
- 注释和文档字符串应解释必要的设计意图，不应复述代码本身。

## Tests

- 默认测试：`uv run pytest`
- 代码规范审计：`uv run python tools/quality_audit.py`
- CLI 覆盖测试：`uv run pytest -q tests/cli`
- 仅收集测试：`uv run pytest --collect-only -q`
- 慢速或真实网络测试使用 `slow`、`real_web` 标记，默认配置会排除 `slow` 测试。

## Network Dependencies

Startup checks connectivity to `https://api.deepseek.com` and `https://open.bigmodel.cn`. Fails gracefully but logs warning.
