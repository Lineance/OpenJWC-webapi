import json
from collections.abc import Mapping
from typing import Any, NoReturn, cast

import httpx
import typer
from rich.console import Console
from rich.pretty import Pretty

from app.cli.client import ApiClient

console = Console()
error_console = Console(stderr=True)


def get_client(context: typer.Context) -> ApiClient:
    return cast(ApiClient, context.obj)


def render(payload: Any, *, as_json: bool) -> None:
    if as_json:
        console.print_json(json.dumps(payload, ensure_ascii=False, default=str))
        return
    console.print(Pretty(payload, expand_all=True))


def fail(error: Exception) -> NoReturn:
    if isinstance(error, httpx.HTTPStatusError):
        response = error.response
        try:
            detail = response.json()
        except ValueError:
            detail = response.text
        error_console.print(f"[bold red]HTTP {response.status_code}[/bold red]", detail)
    else:
        error_console.print(f"[bold red]请求失败[/bold red]：{error}")
    raise typer.Exit(code=1)


def invoke(
    context: typer.Context,
    method: str,
    path: str,
    *,
    admin: bool = False,
    params: Mapping[str, Any] | None = None,
    json_body: Any = None,
    form: Mapping[str, Any] | None = None,
) -> None:
    client = get_client(context)
    try:
        payload = client.request(
            method,
            path,
            admin=admin,
            params=params,
            json_body=json_body,
            form=form,
        )
    except (httpx.HTTPError, ValueError) as error:
        fail(error)
    render(payload, as_json=client.config.json_output)


def stream(context: typer.Context, path: str, body: Any) -> None:
    client = get_client(context)
    try:
        for line in client.stream("POST", path, json_body=body):
            if line:
                console.print(line)
    except httpx.HTTPError as error:
        fail(error)
