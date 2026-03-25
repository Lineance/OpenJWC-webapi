import requests
from typing import Optional, Dict, Any
from app.utils.logging_manager import setup_logger

logger = setup_logger("motto_logs")


def get_daily_quote(
    category: Optional[str] = None, max_length: int = 50
) -> Dict[str, Any]:
    """
    获取每日一言 (调用 Hitokoto API)
    """
    url = "https://v1.hitokoto.cn/"
    params = {}
    if category:
        params["c"] = category
    if max_length:
        params["max_length"] = max_length
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()
        return {
            "success": True,
            "text": data.get("hitokoto", ""),
            "source": data.get("from", "未知出处"),
            "author": data.get("from_who") or "佚名",
            "category_type": data.get("type", ""),
        }

    except requests.exceptions.Timeout:
        logger.error("请求 Hitokoto API 超时")
    except requests.exceptions.RequestException as e:
        logger.error(f"请求 Hitokoto API 失败: {e}")
    except ValueError:
        logger.error("解析 JSON 响应失败")
    return {
        "success": False,
        "text": "你没有成功请求到每日一言，就像你人生中的许多事情一样。",
        "source": "yorozumoon.cn",
        "author": "Moonhalf",
        "category_type": "fallback",
    }


if __name__ == "__main__":
    logger.info("正在获取随机一言...")
    quote1 = get_daily_quote()
    logger.info(f"[{quote1['source']}] {quote1['author']} : {quote1['text']}")
    logger.info("\n正在获取文学类 (c='d') 一言...")
    quote2 = get_daily_quote(category="d")
    logger.info(f"[{quote2['source']}] {quote2['author']} : {quote2['text']}")
