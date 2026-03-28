# OpenJWC WebAPI 服务后端

喵~ 欢迎来到OpenJWC教务通知系统的服务后端喵！🐱

## 项目简介

本项目是OpenJWC教务通知系统的服务后端部分喵~ OpenJWC是一个完整的教务通知系统，包括移动客户端、私有部署服务后端与控制面板喵~

### 主要功能喵

- 🕷️ **定期爬虫**：定时运行爬虫包装器爬取教务通知喵
- 💾 **数据管理**：使用数据库管理资讯信息与系统设置喵
- 📡 **API服务**：接受和处理用户请求喵
- 🤖 **AI对话**：调用LLM API响应用户chat请求喵
- 🧠 **RAG知识库**：使用RAG技术将教务资讯用作LLM的知识库喵
- 👥 **用户功能**：用户可以获取资讯列表、与chat交流，并投稿资讯喵
- 🛠️ **管理功能**：管理员通过控制面板进行配置和用户管理喵
- 🐳 **容器化部署**：服务通过Docker容器运行喵 （docker相关的配置不在这里）

## 项目架构喵

```
OpenJWC-WebAPI/
├── main.py                 # FastAPI应用主入口喵
├── app/
│   ├── api/v1/             # API路由喵
│   │   ├── client/        # 客户端API喵
│   │   └── admin/         # 管理员API喵
│   ├── core/              # 核心配置喵
│   ├── models/            # 数据模型喵
│   ├── services/          # 业务逻辑喵
│   └── utils/             # 工具函数喵
├── data/                  # 数据目录喵
├── bin/                   # 二进制文件目录喵
├── logs/                  # 日志目录喵
└── pyproject.toml         # 项目配置喵
```

## 技术栈喵

- **后端框架**：FastAPI >= 0.135.1 喵
- **数据库**：SQLite + ChromaDB 向量数据库喵
- **AI服务**：DeepSeek API + 智谱AI API喵
- **认证**：JWT + bcrypt加密喵
- **异步HTTP**：httpx + uvicorn喵
- **日志**：结构化日志系统喵
- **错误处理**：Tenacity重试机制喵

## 快速开始喵

### 环境要求喵

- Python >= 3.12 喵
- pip 包管理器喵

### 安装步骤喵

1. **克隆项目喵**

   ```bash
   git clone <repository-url>
   cd OpenJWC-webapi
   ```

2. **安装uv包管理器喵**

   ```bash
   # 如果还没有安装uv喵
   curl -LsSf https://astral.sh/uv/install.sh | sh
   # 添加到PATH喵
   source ~/.bashrc  # 或者重新打开终端喵
   ```

3. **安装项目依赖喵**

   ```bash
   uv pip install -e .  # uv会自动创建虚拟环境并安装依赖喵
   ```

4. **初始化数据库喵**

   ```bash
   uv run python main.py  # 使用uv运行，会自动初始化数据库喵
   ```

### 运行服务喵

```bash
# 开发模式运行喵
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 生产模式运行喵
uvicorn main:app --host 0.0.0.0 --port 8000
```

## API文档喵

启动服务后，访问以下地址查看API文档喵

- **Swagger UI**：<http://localhost:8000/docs> 喵
- **ReDoc**：<http://localhost:8000/redoc> 喵

## 数据库设计喵

### 主要数据表喵

#### notices表喵

- `id` - 通知ID（主键）喵
- `label` - 标签喵
- `title` - 标题喵
- `date` - 日期喵
- `detail_url` - 详情链接喵
- `content_text` - 内容文本喵
- `is_pushed` - 是否已推送喵（推送功能因为技术原因被搁置）

#### api_keys表喵

- `key_string` - API密钥喵
- `owner_name` - 所有者名称喵
- `is_active` - 是否激活喵
- `max_devices` - 最大设备数喵
- `bound_devices` - 绑定设备喵

## 配置说明喵

### 系统设置喵

系统设置存储在system_settings中喵，主要配置项包括喵

```json
{
  "deepseek_api_key": "your_deepseek_api_key",
  "zhipu_api_key": "your_zhipu_api_key",
  "crawler_interval_minutes": 480,
  "crawler_days_gap": 200,
  "search_max_day_diff": 60,
  "prompt_debug": false,
  "notices_auth": false,
  "submission_max_length": 10000,
  "system_prompt": "你的系统提示词喵"
}
```

## 开发指南喵

### 项目结构详解喵

#### app/core/ - 核心配置喵

- `config.py` - 系统配置和常量定义喵
- `security.py` - 安全相关功能喵

#### app/api/v1/ - API路由喵

- `client/` - 客户端API实现喵
  - `notices.py` - 通知相关API喵
  - `chat.py` - 对话功能API喵
  - `submission.py` - 投稿功能API喵
  - `motto.py` - 格言功能API喵
  - `device.py` - 设备管理API喵
  - `register.py` - 注册API喵
- `admin/` - 管理员API实现喵
  - `auth.py` - 认证API喵
  - `settings.py` - 设置管理API喵
  - `monitor.py` - 监控API喵
  - `apikeys.py` - API密钥管理喵
  - `logs.py` - 日志管理API喵
  - `notices.py` - 通知管理API喵
  - `submission.py` - 投稿管理API喵

#### app/services/ - 业务逻辑喵

- `sql_db_service.py` - 数据库服务喵
- `vector_db_service.py` - 向量数据库服务喵
- `ai_service.py` - AI服务喵
- `prompt_engine.py` - 提示词引擎喵
- `submission_service.py` - 投稿服务喵
- `motto_service.py` - 格言服务喵

#### app/utils/ - 工具函数喵

- `logging_manager.py` - 日志管理喵
- `openjwc_cli.py` - 命令行工具喵
- `ping_check.py` - 网络检查喵
- `sysinfo_monitor.py` - 系统监控喵

### 代码规范喵

- 使用异步编程模式喵
- 遵循PEP 8代码规范喵
- 使用类型注解喵
- 编写详细的文档字符串喵

### 生产环境配置喵

- 使用反向代理（如Nginx）喵
- 配置SSL证书喵
- 设置环境变量喵
- 配置日志轮转喵
- 监控服务状态喵

## 监控与日志喵

### 日志系统喵

- 结构化日志输出喵
- 不同级别的日志喵
- 日志文件自动轮转喵

### 监控指标喵

- API响应时间喵
- 错误率统计喵
- 数据库查询性能喵
- AI服务调用状态喵

## 故障排除喵

### 常见问题喵

1. **AI服务连接失败喵**
   - 检查网络连接喵
   - 验证API密钥喵
   - 查看日志文件喵

2. **数据库连接错误喵**
   - 检查数据库文件权限喵
   - 确认数据目录存在喵

3. **爬虫功能异常喵**
   - 检查爬虫二进制文件喵
   - 验证配置参数喵

## 许可证喵

本项目采用MIT许可证喵，详见LICENSE文件喵

## 联系方式喵

如有问题或建议，欢迎提交Issue或Pull Request喵~ 🐱

---

喵~ 感谢使用OpenJWC教务通知系统喵！希望这个README能帮助你快速上手喵~ 如果有任何问题，随时找我喵~ 😊

（后记）：README.md系忘记删测试prompt后vibe产物，感觉和项目调性很吻合遂保留。不代表本人任何立场。
