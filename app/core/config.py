from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT_DIR / "data"
BIN_DIR = ROOT_DIR / "bin"
NOTICE_DB = DATA_DIR / "jwc_notices.db"
NOTICE_JSON = DATA_DIR / "output.json"
CRAWLER_BIN = BIN_DIR / "jwc-crawler"
OUTPUT_JSON = DATA_DIR / "output.json"
LOGS_DIR = ROOT_DIR / "logs"
MAX_DAY_DIFF = 300
ACCESS_TOKEN_EXPIRE_MINUTES = 30
