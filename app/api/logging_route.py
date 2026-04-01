import logging
from fastapi import Request, Response
from fastapi.routing import APIRoute
from typing import Callable
from app.utils.logging_manager import setup_logger

logger = setup_logger("logging_route_logs")


class LoggingRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            body_bytes = await request.body()
            try:
                raw_http = "\nIncoming Request\n"
                query_string = f"?{request.url.query}" if request.url.query else ""
                raw_http += (
                    f"{request.method} {request.url.path}{query_string} HTTP/1.1\n"
                )
                for name, value in request.headers.items():
                    header_name = name.title()
                    if header_name == "Authorization":
                        value = (
                            f"{value[:10]}...[HIDDEN]...{value[-5:]}"
                            if len(value) > 15
                            else "***"
                        )
                    raw_http += f"{header_name}: {value}\n"
                raw_http += "\n"

                if body_bytes:
                    decoded_body = body_bytes.decode("utf-8", errors="replace")
                    if "password=" in decoded_body and "/login" in request.url.path:
                        decoded_body = (
                            " [Form Data Contains Password - Masked for Security] "
                        )
                    raw_http += decoded_body

                logger.info(raw_http)
            except Exception as e:
                logger.error(f"解析请求日志时出错: {e}", exc_info=True)
            response = await original_route_handler(request)
            return response

        return custom_route_handler
