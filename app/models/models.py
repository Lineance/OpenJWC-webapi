from pydantic import BaseModel


class SysinfoData(BaseModel):
    """返回的一堆数据"""

    """CPU使用率"""
    cpu_percent: str
    """服务器总内存"""
    ram_total_mb: str
    """服务占用内存"""
    ram_used_mb: str
    """服务运行时间"""
    uptime_seconds: str
