import os
import asyncio
import socket
import httpx
from typing import Dict, List, Tuple
from app.utils.logging_manager import setup_logger

logger = setup_logger("ping_check_logs")


async def check_tcp_connection(
    host: str, port: int = 443, timeout: int = 3
) -> Tuple[bool, str]:
    """
    检查底层的 DNS 解析和 TCP 连通性（不涉及 HTTP 层和代理）
    """
    try:
        # asyncio.open_connection 会处理 DNS 解析和 TCP 握手
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        return True, "TCP 连通正常 (DNS解析成功)"
    except asyncio.TimeoutError:
        return False, f"TCP 连接超时 ({timeout}s)"
    except socket.gaierror:
        return False, "DNS 解析失败"
    except Exception as e:
        return False, f"TCP 连接异常: {str(e)}"


async def check_http_connection(url: str, timeout: int = 5) -> Tuple[bool, str]:
    """
    检查 HTTP 层面的连通性（会受到系统代理环境变量的影响）
    """
    try:
        # 使用 httpx，模拟应用层真实的请求环境
        async with httpx.AsyncClient(timeout=timeout) as client:
            # 只发 HEAD 请求，节省带宽，测试连通性
            response = await client.head(url)
            # 只要能拿到 HTTP 状态码（即使是401未授权），就说明网络是通的
            return True, f"HTTP 通信正常 (Status: {response.status_code})"
    except httpx.ConnectError as e:
        return False, f"HTTP 连接失败 (可能是代理无响应或网络阻断): {str(e)}"
    except httpx.TimeoutException:
        return False, f"HTTP 请求超时 ({timeout}s)"
    except Exception as e:
        return False, f"HTTP 异常: {str(e)}"


def get_proxy_env_vars() -> Dict[str, str]:
    """
    获取当前系统中可能影响网络请求的代理环境变量
    """
    proxy_vars = [
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ]
    found_proxies = {var: os.environ[var] for var in proxy_vars if var in os.environ}
    return found_proxies


async def diagnose_network_environment(target_urls: List[str] = None) -> bool:
    """
    综合网络环境诊断主函数。
    返回 True 表示一切正常，返回 False 表示存在潜在网络风险。
    """
    if target_urls is None:
        target_urls = [
            "https://api.deepseek.com",
            "https://open.bigmodel.cn",
        ]

    logger.info("开始进行服务器网络环境诊断...")
    all_passed = True

    proxies = get_proxy_env_vars()
    if proxies:
        logger.warning("检测到系统配置了代理环境变量！这可能会导致外部 API 无法访问:")
        for k, v in proxies.items():
            logger.warning(f"   - {k} = {v}")
        logger.warning(
            "建议：如果国内直连大模型 API，请在代码中显式禁用代理 (proxies=None)。"
        )
    else:
        logger.info("未检测到系统级代理环境变量。")

    from urllib.parse import urlparse

    for url in target_urls:
        parsed_url = urlparse(url)
        host = parsed_url.hostname
        logger.info(f"\n 正在测试目标: {host}")

        tcp_ok, tcp_msg = await check_tcp_connection(host)
        if tcp_ok:
            logger.info(f"  [TCP]通过{tcp_msg}")
        else:
            logger.error(f"  [TCP]未通过{tcp_msg}")
            all_passed = False

        http_ok, http_msg = await check_http_connection(url)
        if http_ok:
            logger.info(f"  [HTTP]通过{http_msg}")
        else:
            logger.error(f"  [HTTP]未通过{http_msg}")
            all_passed = False

    if all_passed:
        logger.info("网络环境诊断完成：所有外部依赖通信正常。")
    else:
        logger.error("网络环境诊断完成：存在异常，请检查服务器网络或代理配置！")

    return all_passed
