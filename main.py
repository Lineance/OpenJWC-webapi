from fastapi import FastAPI
from app.api import chat, notices
import os

ROOT_DIR = os.getcwd()
DATA_DIR = os.path.join(ROOT_DIR, "data")
BIN_DIR = os.path.join(ROOT_DIR, "bin")

app = FastAPI(title="教务处通知助手")

# 注册各个模块的路由
app.include_router(chat.router, prefix="/api", tags=["AI聊天"])
app.include_router(notices.router, prefix="/api", tags=["资讯管理"])


@app.get("/")
def root():
    return {"message": "Server is running!"}
