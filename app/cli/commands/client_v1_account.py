from pathlib import Path
from typing import Annotated, Any

import typer

from app.cli.output import get_client, invoke, stream
from app.cli.parsing import json_value

app = typer.Typer(help="v1 客户端账号、设备、对话与每日一言接口。")


@app.command("register")
def register(context: typer.Context) -> None:
    invoke(context, "POST", "/api/v1/client/register")


@app.command("devices")
def devices(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v1/client/device")


@app.command("unbind")
def unbind(context: typer.Context) -> None:
    invoke(context, "POST", "/api/v1/client/device/unbind")


@app.command("motto")
def motto(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v1/client/motto")


@app.command("chat")
def chat(
    context: typer.Context,
    query: Annotated[str, typer.Argument(help="发送给助手的问题。")],
    notice_id: Annotated[list[str] | None, typer.Option("--notice-id")] = None,
    history_json: Annotated[str | None, typer.Option("--history-json")] = None,
    use_stream: Annotated[bool, typer.Option("--stream/--no-stream")] = False,
) -> None:
    history = json_value(history_json, default=[])
    if not isinstance(history, list):
        raise typer.BadParameter("history-json 必须是 JSON 数组")
    body: dict[str, Any] = {
        "notice_ids": notice_id or None,
        "user_query": query,
        "stream": use_stream,
        "history": history,
    }
    if use_stream:
        stream(context, "/api/v1/client/chat", body)
        return
    invoke(context, "POST", "/api/v1/client/chat", json_body=body)
