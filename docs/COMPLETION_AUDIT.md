# 重构任务完成审计

审计日期：2026-07-29

## 1. 目标拆解

1. 将用户给出的全部代码规约写入 `AGENTS.md`。
2. 重构 `app/`、`main.py`、`tests/`，同时保护用户已有无关改动和运行数据。
3. 保证 Python 文件最多 300 行、每文件最多 2 个类、目录最多 8 个 Python 文件。
4. 保证所有函数参数和返回值显式标注类型。
5. 保证注释使用中文、无段中注释、无 banner 和滥用。
6. 使用内置装饰器表达工厂、工具方法和属性语义；为面向用户的数据结构提供语义工厂和明确状态变换。
7. 使用现代 CLI 覆盖全部面向 Web 前端的 v1/v2 API，并保留本地运维能力。
8. 提供架构、迁移、命令映射、示例、验证证据和限制文档。
9. 完成锁文件、编译、CLI、自动化测试、diff 自查和独立只读复核。

## 2. 提示词到产物检查表

| 要求 | 产物或证据 | 结果 |
|---|---|---|
| 规约写入 AGENTS.md | `AGENTS.md` 的 `Code Style`、`Tests`、`CLI` 章节 | 完成 |
| 全量结构重构 | `app/**/_mixins/`、模型/标准化/验证/去重子模块及拆分后的 `tests/` | 完成 |
| 文件不超过 300 行 | `tools/quality_audit.py`；实际最大文件 274 行 | 通过 |
| 每文件最多 2 个类 | 审计遍历全部 AST，包括嵌套类 | 通过 |
| 目录最多 8 个 Python 文件 | 审计按目录计数；实际最大值 8 | 通过 |
| 函数类型完整 | 审计检查普通、异步、变长和仅关键字参数及返回值 | 通过 |
| 中文、非段中注释 | tokenize 级审计 | 通过 |
| 内置装饰器与语义工厂 | `SemanticModel.from_payload`、`CliConfig.from_options`、`SubmissionContent.from_text`、`SubmissionDraft.for_notice`、`SubmissionRecord.pending/from_storage/with_review`、`UserRegistrationRecord.pending/from_storage` | 通过 |
| 全部前端 API 有 CLI | `app/cli/manifest.py` 与 FastAPI 实际路由集合精确比对 | 41/41 |
| CLI 命令可发现 | 对 manifest 中 41 个命令逐个执行 `--help` | 41/41 |
| 现代 CLI | Typer + httpx；`openjwc` 控制台脚本；`client-v1`、`admin-v1`、`client-v2`、`admin-v2`、`ops` | 完成 |
| 保留本地运维 | `app/cli/ops.py`：管理员同步、设置同步、诊断、爬虫、受限删表 | 完成 |
| CLI 自动化测试 | `tests/cli/test_manifest.py`、`tests/cli/test_commands.py` | 5 项通过并纳入全套测试 |
| 重构与迁移文档 | `docs/REFACTORING.md`、更新后的 `README.md` | 完成 |
| 包发现与入口 | `pyproject.toml` 的 `project.scripts` 和 `tool.setuptools.packages.find` | 完成 |
| 锁文件一致 | `uv lock --check` | 通过 |
| 导入与语法 | `uv run python -m compileall -q app tests tools main.py` | 通过 |
| 默认完整测试 | `uv run pytest -q` | 346 passed、1 skipped、6 deselected |
| 独立复核 | `code_reviewer` 因外部 429 配额失败；随后使用只读 Oracle 完成复核并修复其发现 | 完成 |
| 保护用户改动 | 未编辑 `.env.example` 字符设备、`.venv`、真实数据、日志、密钥和外部二进制 | 完成 |

## 3. 最终执行证据

```text
uv lock --check
Resolved 122 packages

uv run python tools/quality_audit.py
代码规范审计通过：375 个 Python 文件

uv run pytest -q
346 passed, 1 skipped, 6 deselected, 1 warning

CLI route audit
route-cli-coverage: 41/41; help failures: 0

uv run openjwc --help
通过

uv run python main.py --help
通过
```

跳过项是缺少匹配当前 LLM provider 的有效 API Key 的真实 LLM smoke 测试；6 个 deselected 项来自项目默认排除的慢速测试。Passlib 的 `crypt` 弃用警告来自第三方依赖，不是本次重构引入的功能失败。

## 4. 复核后修复

独立复核发现并已处理：

- 消除 `app/domain/submission/models.py` 与 `model_parts` 的循环导入。
- 将领域服务和仓储中的 `SubmissionRecord` 直接构造改为 `pending`/`from_storage` 工厂。
- 将用户注册仓储改为 `UserRegistrationRecord.from_storage`。
- 统一 v2 用户管理 CLI 路径参数类型。
- 对上述变化运行 12 项针对性测试、直接子模块导入检查、编译和规范审计。

## 5. 保留的不确定性

- 未使用真实外部凭据调用 AI、云嵌入和每日一言服务。
- 未运行默认排除的慢速/真实网络测试。
- CLI HTTP 行为由路由清单、请求构造单测和既有 API e2e 测试验证；外部部署环境中的代理、TLS 和凭据配置仍需部署时验证。
- `tools/quality_audit.py` 验证可静态判定的硬约束；语义工厂由单独的反射检查和调用点复核验证，而不是仅依赖规范审计脚本。
