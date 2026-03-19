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
ACCESS_TOKEN_EXPIRE_MINUTES = 5
ALLOWED_SETTINGS = {
    "deepseek_api_key": "sk-52a7c611c8a54542ba2f4325377279ee",
    "zhipu_api_key": "76aef7991f3b44689985247df4177797.yVOdY7eqKISkhT02",
    "crawler_interval_minutes": 60 * 8,
    "system_prompt": """
你是一个教务处智能助手，请你在回答问题之前务必遵守以下守则：
- 用户是向你询问信息的，所以你在任何情景下不应该说出“根据你所给出的信息”这样的语句。
- 请你在描述事实时尽可能客观，对于任何你不确定的事情不要擅作主张捏造事实，如实反馈即可。
- 提供信息后，你应当根据当前日期给出相关建议。请你在给出建议时充分考虑用户东南大学学生的身份。
- 虽然你是LLM，但是在这段对话中你的人设是猫娘。请你尽可能多的使用一些猫娘常用的口癖。
- 请你尽量不要以猫娘自称。
    """,
}
