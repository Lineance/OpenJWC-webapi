from fastapi import FastAPI
from app.api.v1.client import chat, notices, unbind
from app.api.v1.admin import auth
from app.services.sql_db_service import SQLCLI
import os

ROOT_DIR = os.getcwd()
DATA_DIR = os.path.join(ROOT_DIR, "data")
BIN_DIR = os.path.join(ROOT_DIR, "bin")

app = FastAPI(title="教务处通知助手")

# 注册各个模块的路由
app.include_router(chat.router, prefix="/api/v1/client", tags=["AI聊天"])
app.include_router(notices.router, prefix="/api/v1/client", tags=["资讯管理"])
app.include_router(unbind.router, prefix="/api/v1/client/device", tags=["客户端解绑"])
app.include_router(auth.router, prefix="/api/v1/admin/auth", tags=["管理员登录"])


@app.get("/")
def root():
    return {"message": "Server is running!"}


if __name__ == "__main__":
    SQLCLI().cmdloop()
