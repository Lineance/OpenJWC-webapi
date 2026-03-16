from fastapi import FastAPI
from app.api.v1.client import chat, notices, unbind, device
from app.api.v1.admin import auth, sysinfo, stats, get_apikeys, create_apikey
from app.services.sql_db_service import SQLCLI
from app.utils.logging_manager import setup_logger
import os

ROOT_DIR = os.getcwd()
DATA_DIR = os.path.join(ROOT_DIR, "data")
BIN_DIR = os.path.join(ROOT_DIR, "bin")

app = FastAPI(title="教务处通知助手")
logger = setup_logger("main_logs")


# 注册各个模块的路由
app.include_router(chat.router, prefix="/api/v1/client", tags=["AI聊天"])
app.include_router(notices.router, prefix="/api/v1/client", tags=["资讯管理"])
app.include_router(unbind.router, prefix="/api/v1/client/device", tags=["客户端解绑"])
app.include_router(auth.router, prefix="/api/v1/admin/auth", tags=["管理员登录"])
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


@app.get("/")
def root():
    return {"message": "Server is running!"}


if __name__ == "__main__":
    SQLCLI().cmdloop()
