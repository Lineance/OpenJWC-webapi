# OpenJWC WebAPI 全量重构说明

## 1. 目标与边界

本次重构统一处理了生产代码、测试代码和 CLI，目标是：

- 落实 `AGENTS.md` 中的文件规模、类数量、中文注释、类型标注、内置装饰器和语义工厂规约。
- 保持现有 FastAPI v1/v2 路由、请求参数和响应语义不变。
- 为所有面向 Web 前端的 API 提供一一对应的命令行入口。
- 保留爬虫、网络诊断、管理员同步等无需启动 Web 服务的本地运维能力。

未修改 `.env.example` 的既有工作区改动、真实数据库、日志、密钥、`.venv` 和外部爬虫二进制。

## 2. 架构变化

### 2.1 大型模块拆分

超过规模限制的单体类已按职责拆为 mixin，例如：

- `ReActAgent`：意图识别、工具选择、观察格式化、答案组装和流式执行。
- `ArticleRepository`、`TagRepository`：写入、查询、批处理、索引和统计。
- `LanceStore`、`RetrievalEngine`：索引、检索、混合排序、文档读取和写入。
- 爬虫：配置、状态、页面抓取、结果格式化和内容清洗。
- 摄取管道：单条处理、批处理和处理阶段。

标准化器、验证器、去重组件、数据模型和测试套件则按功能拆入子目录。原模块继续导入并导出原有公开名称，因此既有导入路径保持兼容。

### 2.2 数据模型

Pydantic 请求/响应模型拆入 `app/models/v1/` 和 `app/models/v2/`。它们继承 `SemanticModel`，可使用 `from_payload(...)` 创建，不要求调用方直接依赖构造函数。

领域模型提供语义工厂和明确的状态变换，例如：

- `SubmissionContent.from_text(...)`
- `SubmissionDraft.for_notice(...)`
- `SubmissionRecord.pending(...)`
- `SubmissionRecord.with_review(...)`
- `UserRegistrationRecord.pending(...)`

### 2.3 自动规范审计

`tools/quality_audit.py` 检查以下硬性约束：

- Python 文件不超过 300 行。
- 每个文件最多 2 个类定义，包括嵌套类。
- 每个函数的全部参数和返回值均有类型标注。
- 不存在段中注释，已有注释必须包含中文。
- 单目录不超过 8 个 Python 文件。

运行方式：

```bash
uv run python tools/quality_audit.py
```

## 3. CLI 设计

CLI 使用 Typer 构建，使用 httpx 调用正在运行的 WebAPI。这样 CLI 与前端共享同一鉴权、校验和业务路径，不复制路由业务逻辑。

安装项目后可直接运行：

```bash
uv run openjwc --help
```

也可使用模块入口：

```bash
uv run python -m app.cli.app --help
```

`python main.py` 现在同样进入新 CLI；旧的 `SQLCLI().cmdloop()` 调用由兼容层转发。

### 3.1 全局选项

| 选项 | 环境变量 | 用途 |
|---|---|---|
| `--base-url` | `OPENJWC_BASE_URL` | WebAPI 地址，默认 `http://127.0.0.1:8000` |
| `--token` | `OPENJWC_TOKEN` | v1 API Key 或 v2 客户端 JWT |
| `--admin-token` | `OPENJWC_ADMIN_TOKEN` | 管理员 JWT |
| `--device-id` | `OPENJWC_DEVICE_ID` | 客户端设备 UUID |
| `--client-version` | 无 | 发往管理员 API 的客户端版本 |
| `--timeout` | 无 | HTTP 超时秒数 |
| `--json` | 无 | 输出原始 JSON |

### 3.2 路由与命令覆盖

完整的机器可读映射位于 `app/cli/manifest.py`，并由 `tests/cli/test_manifest.py` 与 FastAPI 实际路由逐项比对。

#### v1 客户端

| 功能 | 命令 |
|---|---|
| 注册设备 | `client-v1 register` |
| 查看/解绑设备 | `client-v1 devices`、`client-v1 unbind` |
| AI 对话 | `client-v1 chat` |
| 通知列表/标签/搜索 | `client-v1 notices`、`client-v1 labels`、`client-v1 search` |
| 投稿/我的投稿 | `client-v1 submit`、`client-v1 submissions` |
| 每日一言 | `client-v1 motto` |

#### v1 管理员

| 功能 | 命令 |
|---|---|
| 登录 | `admin-v1 login` |
| 设置读取/修改/重置 | `admin-v1 settings`、`settings-update`、`settings-reset` |
| 密码、每日一言、爬虫 | `admin-v1 password`、`motto-refresh`、`crawl` |
| 监控 | `admin-v1 stats`、`sysinfo` |
| API Key | `admin-v1 apikey-create`、`apikeys`、`apikey-delete`、`apikey-status` |
| 日志 | `admin-v1 logs`、`log-modules` |
| 通知 | `admin-v1 notices`、`notice-labels`、`notice-delete` |
| 投稿审核 | `admin-v1 submissions`、`submission`、`submission-review` |

#### v2 客户端与管理员

| 功能 | 命令 |
|---|---|
| 注册/登录 | `client-v2 register`、`client-v2 login` |
| 查看/解绑设备 | `client-v2 devices`、`client-v2 unbind` |
| 注册申请 | `admin-v2 registrations`、`registration`、`registration-review` |
| 用户管理 | `admin-v2 users`、`user-status`、`user-delete` |

#### 本地运维

`ops` 命令不经过 HTTP：

- `ops admins-sync`
- `ops settings-sync`
- `ops diagnose`
- `ops crawl`
- `ops db-drop <TABLE>...`

## 4. 使用示例

```bash
# 获取客户端通知
uv run openjwc \
  --token "$OPENJWC_TOKEN" \
  --device-id "$OPENJWC_DEVICE_ID" \
  client-v1 notices --page 1 --size 20

# 混合搜索并输出 JSON
uv run openjwc --json \
  --token "$OPENJWC_TOKEN" \
  --device-id "$OPENJWC_DEVICE_ID" \
  client-v1 search "选课通知" --top-k 10

# 管理员修改设置
uv run openjwc --admin-token "$OPENJWC_ADMIN_TOKEN" \
  admin-v1 settings-update --setting notices_auth=1

# 审核 v2 注册申请
uv run openjwc --admin-token "$OPENJWC_ADMIN_TOKEN" \
  admin-v2 registration-review 42 --action approved --review "审核通过"
```

管理员登录返回的 token 不会自动持久化。可将结果写入安全的环境变量管理方案，不应提交到仓库。

## 5. 迁移说明

- 旧交互式 cmd2 shell 已替换为可组合、可补全、适合脚本和 CI 的 Typer 子命令。
- `cmd2` 与 Linux 专用 `gnureadline` 不再是项目直接依赖。
- setuptools 改为自动发现 `app*` 子包，确保拆分后的模块和 CLI 会被正确安装。
- 原 API 路由和主要 Python 导入路径保留；新内部模块不应作为稳定公共接口依赖。

## 6. 验证命令

```bash
uv lock --check
uv run python tools/quality_audit.py
uv run python -m compileall -q app tests tools main.py
uv run pytest -q tests/cli
uv run pytest
```

CLI 路由清单当前覆盖 FastAPI 的全部 41 个 `/api/` 路由。完整测试的最终结果应以执行环境中的实际命令输出为准。

## 7. 已知限制

- CLI 是 HTTP 客户端；除 `ops` 外，调用前必须启动 WebAPI。
- AI、云嵌入、每日一言和网络诊断仍依赖外部服务、网络和有效凭据。
- 本地嵌入测试需要安装 `.[local]` 可选依赖。
- 流式聊天按服务端 SSE 原始行输出，便于管道处理，不额外重写事件内容。
