from pydantic import BaseModel


# 定义请求格式的结构 (Schema)
class ChatRequest(BaseModel):
    notice_id: str
    user_query: str
    stream: bool = False
