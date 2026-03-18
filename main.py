from fastapi import FastAPI
from app.api.v1.client import chat, notices, unbind, device, get_labels
from app.api.v1.admin import (
    auth,
    sysinfo,
    stats,
    get_apikeys,
    create_apikey,
    toggle_apikey,
    delete_apikey,
    get_settings,
    update_setting,
    reset_settings,
)
from app.services.sql_db_service import SQLCLI
from app.utils.logging_manager import setup_logger
from contextlib import asynccontextmanager
from app.utils.ping_check import diagnose_network_environment
import os


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("正在启动服务...")
    api_targets = ["https://api.deepseek.com", "https://open.bigmodel.cn"]
    is_network_healthy = await diagnose_network_environment(api_targets)
    if not is_network_healthy:
        logger.error("启动警告：关键依赖网络不通，后续 API 调用可能会失败")
    yield  # 让 FastAPI 继续启动并处理请求
    logger.info("服务正在关闭...")


ROOT_DIR = os.getcwd()
DATA_DIR = os.path.join(ROOT_DIR, "data")
BIN_DIR = os.path.join(ROOT_DIR, "bin")

app = FastAPI(title="教务处通知助手", lifespan=lifespan)
logger = setup_logger("main_logs")


# 注册各个模块的路由
app.include_router(chat.router, prefix="/api/v1/client", tags=["AI聊天"])
app.include_router(notices.router, prefix="/api/v1/client", tags=["资讯管理"])
app.include_router(unbind.router, prefix="/api/v1/client/device", tags=["客户端解绑"])
app.include_router(auth.router, prefix="/api/v1/admin/auth", tags=["管理员登录"])
app.include_router(get_settings.router, prefix="/api/v1/admin", tags=["获取系统设置"])
app.include_router(
    sysinfo.router, prefix="/api/v1/admin/monitor", tags=["系统基本信息"]
)
app.include_router(stats.router, prefix="/api/v1/admin/monitor", tags=["基本业务信息"])
app.include_router(device.router, prefix="/api/v1/client", tags=["设备绑定信息"])
app.include_router(
    get_apikeys.router, prefix="/api/v1/admin", tags=["获取所有apikeys信息"]
)
app.include_router(
    create_apikey.router, prefix="/api/v1/admin", tags=["创建新的apikey"]
)
app.include_router(
    toggle_apikey.router, prefix="/api/v1/admin/apikeys", tags=["启停apikey"]
)
app.include_router(
    delete_apikey.router, prefix="/api/v1/admin/apikeys", tags=["删除apikey"]
)
app.include_router(update_setting.router, prefix="/api/v1/admin", tags=["修改系统设置"])
app.include_router(
    get_labels.router, prefix="/api/v1/client/notices", tags=["获取所有标签"]
)
app.include_router(
    reset_settings.router, prefix="/api/v1/admin/settings", tags=["重置系统设置"]
)


@app.get("/")
def root():
    return {"message": "Server is running!"}


if __name__ == "__main__":
    SQLCLI().cmdloop()
