from fastapi import FastAPI
from app.api import chat

app = FastAPI(title="教务处通知助手")

# 注册各个模块的路由
app.include_router(chat.router, prefix="/api", tags=["Chat"])
# app.include_router(notices.router, prefix="/api", tags=["Notices"])


@app.get("/")
def root():
    return {"message": "Server is running!"}
