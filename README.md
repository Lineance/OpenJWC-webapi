# OpenJWC WebAPI 服务后端

欢迎来到OpenJWC教务通知系统的服务后端！

## 项目简介

本项目是OpenJWC教务通知系统的服务后端部分。OpenJWC是一个完整的教务通知系统，包括移动客户端、私有部署服务后端与控制面板

### 主要功能

-  **定期爬虫**：定时运行爬虫包装器爬取教务通知
-  **数据管理**：使用数据库管理资讯信息与系统设置
-  **API服务**：接受和处理用户请求
-  **AI对话**：调用LLM API响应用户chat请求
-  **RAG知识库**：使用RAG技术将教务资讯用作LLM的知识库
-  **用户功能**：用户可以获取资讯列表、与chat交流，并投稿资讯
-  **管理功能**：管理员通过控制面板进行配置和用户管理
-  **容器化部署**：服务通过Docker容器运行 （docker相关的配置不在这里）

## 项目架构

```
OpenJWC-WebAPI/
├── main.py                 # FastAPI应用主入口
├── app/
│   ├── api/v1/             # API路由
│   │   ├── client/        # 客户端API
│   │   └── admin/         # 管理员API
│   ├── core/              # 核心配置
│   ├── models/            # 数据模型
│   ├── services/          # 业务逻辑
│   └── utils/             # 工具函数
├── data/                  # 数据目录
├── bin/                   # 二进制文件目录
├── logs/                  # 日志目录
└── pyproject.toml         # 项目配置
```

## 技术栈

- **后端框架**：FastAPI >= 0.135.1 
- **数据库**：SQLite + ChromaDB 向量数据库
- **AI服务**：DeepSeek API + 智谱AI API
- **认证**：JWT + bcrypt加密
- **异步HTTP**：httpx + uvicorn
- **日志**：结构化日志系统
- **错误处理**：Tenacity重试机制

## 快速开始

### 环境要求

- Python >= 3.12 
- pip 包管理器

### 安装步骤

1. **克隆项目**

   ```bash
   git clone <repository-url>
   cd OpenJWC-webapi
   ```

2. **安装uv包管理器**

   ```bash
   # 如果还没有安装uv
   curl -LsSf https://astral.sh/uv/install.sh | sh
   # 添加到PATH
   source ~/.bashrc  # 或者重新打开终端
   ```

3. **安装项目依赖**

   ```bash
   uv pip install -e .  # uv会自动创建虚拟环境并安装依赖
   ```

4. **初始化数据库**

   ```bash
   uv run python main.py  # 使用uv运行，会自动初始化数据库
   ```

### 运行服务

```bash
# 开发模式运行
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 生产模式运行
uvicorn main:app --host 0.0.0.0 --port 8000
```

## API文档

启动服务后，访问以下地址查看API文档

- **Swagger UI**：<http://localhost:8000/docs> 
- **ReDoc**：<http://localhost:8000/redoc> 

## 数据库设计

### 主要数据表

#### notices表

- `id` - 通知ID（主键）
- `label` - 标签
- `title` - 标题
- `date` - 日期
- `detail_url` - 详情链接
- `content_text` - 内容文本
- `is_pushed` - 是否已推送（推送功能因为技术原因被搁置）

#### api_keys表

- `key_string` - API密钥
- `owner_name` - 所有者名称
- `is_active` - 是否激活
- `max_devices` - 最大设备数
- `bound_devices` - 绑定设备

## 配置说明

### 系统设置

系统设置存储在system_settings中，主要配置项包括

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
  "system_prompt": "你的系统提示词"
}
```

## 开发指南

### 项目结构详解

#### app/core/ - 核心配置

- `config.py` - 系统配置和常量定义
- `security.py` - 安全相关功能

#### app/api/v1/ - API路由

- `client/` - 客户端API实现
  - `notices.py` - 通知相关API
  - `chat.py` - 对话功能API
  - `submission.py` - 投稿功能API
  - `motto.py` - 格言功能API
  - `device.py` - 设备管理API
  - `register.py` - 注册API
- `admin/` - 管理员API实现
  - `auth.py` - 认证API
  - `settings.py` - 设置管理API
  - `monitor.py` - 监控API
  - `apikeys.py` - API密钥管理
  - `logs.py` - 日志管理API
  - `notices.py` - 通知管理API
  - `submission.py` - 投稿管理API

#### app/services/ - 业务逻辑

- `sql_db_service.py` - 数据库服务
- `vector_db_service.py` - 向量数据库服务
- `ai_service.py` - AI服务
- `prompt_engine.py` - 提示词引擎
- `submission_service.py` - 投稿服务
- `motto_service.py` - 格言服务

#### app/utils/ - 工具函数

- `logging_manager.py` - 日志管理
- `openjwc_cli.py` - 命令行工具
- `ping_check.py` - 网络检查
- `sysinfo_monitor.py` - 系统监控

### 代码规范

- 使用异步编程模式
- 遵循PEP 8代码规范
- 使用类型注解
- 编写详细的文档字符串

### 生产环境配置

- 使用反向代理（如Nginx）
- 配置SSL证书
- 设置环境变量
- 配置日志轮转
- 监控服务状态

## 监控与日志

### 日志系统

- 结构化日志输出
- 不同级别的日志
- 日志文件自动轮转

### 监控指标

- API响应时间
- 错误率统计
- 数据库查询性能
- AI服务调用状态

## 故障排除

### 常见问题

1. **AI服务连接失败**
   - 检查网络连接
   - 验证API密钥
   - 查看日志文件

2. **数据库连接错误**
   - 检查数据库文件权限
   - 确认数据目录存在

3. **爬虫功能异常**
   - 检查爬虫二进制文件
   - 验证配置参数

## 许可证

本项目采用MIT许可证，详见LICENSE文件

## 联系方式

如有问题或建议，欢迎提交Issue或Pull Request
