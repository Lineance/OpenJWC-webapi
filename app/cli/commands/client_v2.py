from typing import Annotated

import typer

from app.cli.output import invoke

app = typer.Typer(help="v2 客户端注册、登录与设备接口。")


@app.command("register")
def register(
    context: typer.Context,
    username: Annotated[str, typer.Argument()],
    email: Annotated[str, typer.Option()],
    password_hash: Annotated[str, typer.Option(prompt=True, hide_input=True)],
) -> None:
    body = {"username": username, "email": email, "password_hash": password_hash}
    invoke(context, "POST", "/api/v2/client/auth/register", json_body=body)


@app.command("login")
def login(
    context: typer.Context,
    account: Annotated[str, typer.Argument()],
    device_name: Annotated[str, typer.Option()],
    password_hash: Annotated[str, typer.Option(prompt=True, hide_input=True)],
) -> None:
    body = {
        "account": account,
        "password_hash": password_hash,
        "device_name": device_name,
    }
    invoke(context, "POST", "/api/v2/client/auth/login", json_body=body)


@app.command("devices")
def devices(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v2/client/device")


@app.command("unbind")
def unbind(
    context: typer.Context,
    device_uuid: Annotated[str, typer.Argument()],
) -> None:
    invoke(
        context,
        "POST",
        "/api/v2/client/device/unbind",
        json_body={"device_uuid": device_uuid},
    )
