import time
import os
import psutil
from app.models.models import SysinfoData
from app.models.schemas import ResponseModel
from app.utils.logging_manager import setup_logger

# 记录程序启动时间，用于计算 uptime
START_TIME = time.time()

logger = setup_logger("sysinfo_monitor_logs")


class SystemMonitor:
    """系统监控工具类"""

    @staticmethod
    def get_stats() -> SysinfoData:
        cpu_p = psutil.cpu_percent(interval=0.1)
        total_ram_gb = psutil.virtual_memory().total / (1024 * 1024)
        process = psutil.Process(os.getpid())
        process_ram_mb = process.memory_info().rss / (1024 * 1024)
        uptime = time.time() - START_TIME

        return SysinfoData(
            cpu_percent=f"{cpu_p}%",
            ram_total_mb=f"{total_ram_gb:.2f} MB",
            ram_used_mb=f"{process_ram_mb:.2f} MB",
            uptime_seconds=f"{int(uptime)}s",
        )


def get_server_status():
    try:
        data = SystemMonitor.get_stats()
        return ResponseModel(data=data, msg="success")
    except Exception as e:
        return ResponseModel(data=None, msg=f"error: {str(e)}")


if __name__ == "__main__":
    res = get_server_status()
    print(res)
