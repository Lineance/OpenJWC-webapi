from fastapi import FastAPI
from app.api.v1.api_router import client_router, admin_router
from app.api.v2.api_router import v2_client_router
from app.utils.openjwc_cli import SQLCLI
from app.utils.logging_manager import setup_logger
from contextlib import asynccontextmanager
from app.utils.ping_check import diagnose_network_environment
from app.infrastructure.storage.sqlite.sql_db_service import db
import os


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("正在启动服务...")
    db.sync_admins_from_config()
    api_targets = ["https://api.deepseek.com", "https://open.bigmodel.cn"]
    is_network_healthy = await diagnose_network_environment(api_targets)
    if not is_network_healthy:
        logger.error("启动警告：关键依赖网络不通，后续 API 调用可能会失败")
    yield
    logger.info("服务正在关闭...")


ROOT_DIR = os.getcwd()
DATA_DIR = os.path.join(ROOT_DIR, "data")
BIN_DIR = os.path.join(ROOT_DIR, "bin")

app = FastAPI(title="教务处通知助手", lifespan=lifespan)
logger = setup_logger("main_logs")

app.include_router(client_router, prefix="/api/v1", tags=["客户端"])
app.include_router(admin_router, prefix="/api/v1", tags=["管理员", "控制面板"])
app.include_router(v2_client_router, prefix="/api/v2", tags=["客户端 v2"])


@app.get("/")
def root():
    return {"message": "Server is running!"}


if __name__ == "__main__":
    SQLCLI().cmdloop()
