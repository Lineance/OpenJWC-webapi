import sqlite3
from typing import Protocol

from app.core.config import SQLITE_DB_PATH
from app.utils.logging_manager import setup_logger

logger = setup_logger("sql_db_logs")


class DBInterface(Protocol):
    def __init__(self, db_path=SQLITE_DB_PATH) -> None: ...

    def get_connection(self) -> sqlite3.Connection: ...

    def init_db(self) -> None: ...
