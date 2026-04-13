import logging
from logging.handlers import RotatingFileHandler
from app.core.config import LOGS_DIR
from rich.logging import RichHandler
import re

_SHARED_HANDLERS = {}

LOG_PATTERN = re.compile(r"^\[(.*?)\] \[(.*?)\] \[(.*?)\] \[(.*?)\] - (.*)$")


def _get_shared_handlers():
    """初始化并获取共享的日志处理器"""
    if _SHARED_HANDLERS:
        return _SHARED_HANDLERS

    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    console_handler = RichHandler(
        level=logging.INFO,
        rich_tracebacks=True,
        tracebacks_show_locals=True,
        show_time=True,
        show_level=True,
        show_path=True,
        markup=True,
    )
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    file_formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)-8s] [%(name)s] [%(filename)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    app_log_path = LOGS_DIR / "app.log"
    app_handler = RotatingFileHandler(
        filename=app_log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    app_handler.setLevel(logging.DEBUG)
    app_handler.setFormatter(file_formatter)
    error_log_path = LOGS_DIR / "error.log"
    error_handler = RotatingFileHandler(
        filename=error_log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    _SHARED_HANDLERS["console"] = console_handler
    _SHARED_HANDLERS["app"] = app_handler
    _SHARED_HANDLERS["error"] = error_handler

    return _SHARED_HANDLERS


def setup_logger(logger_name: str) -> logging.Logger:
    """
    :param logger_name: 日志器名称（模块名）
    :return: 配置好的标准 logging.Logger 实例
    """
    logger = logging.getLogger(logger_name)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    handlers = _get_shared_handlers()
    logger.addHandler(handlers["console"])
    logger.addHandler(handlers["app"])
    logger.addHandler(handlers["error"])

    return logger


def parse_logs(level: str = None, module: str = None, keyword: str = None):
    """读取并解析日志文件，处理多行 Traceback，并应用过滤条件"""
    log_file_path = LOGS_DIR / "app.log"
    if not log_file_path.exists():
        return []
    parsed_logs = []
    current_log = None
    with open(log_file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            match = LOG_PATTERN.match(line)
            if match:
                if current_log:
                    parsed_logs.append(current_log)
                current_log = {
                    "timestamp": match.group(1),
                    "level": match.group(2).strip(),
                    "module": match.group(3),
                    "location": match.group(4),
                    "message": match.group(5),
                }
            else:
                if current_log:
                    current_log["message"] += f"\n{line}"
        if current_log:
            parsed_logs.append(current_log)
    parsed_logs.reverse()
    filtered_logs = []
    for log in parsed_logs:
        if level and log["level"] != level.upper():
            continue
        if module and log["module"] != module:
            continue
        if keyword and keyword.lower() not in log["message"].lower():
            continue
        filtered_logs.append(log)
    return filtered_logs
