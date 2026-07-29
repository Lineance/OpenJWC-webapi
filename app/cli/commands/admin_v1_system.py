from typing import Annotated

import typer

from app.cli.output import invoke
from app.cli.parsing import setting_values

app = typer.Typer(help="v1 管理员认证、设置、监控与日志接口。")


@app.command("login")
def login(
    context: typer.Context,
    username: Annotated[str, typer.Argument()],
    password: Annotated[str, typer.Option(prompt=True, hide_input=True)],
) -> None:
    invoke(
        context,
        "POST",
        "/api/v1/admin/auth/login",
        form={"username": username, "password": password},
    )


@app.command("settings")
def settings(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v1/admin/settings", admin=True)


@app.command("settings-update")
def settings_update(
    context: typer.Context,
    setting: Annotated[list[str], typer.Option("--setting", help="使用 key=value 格式。")],
) -> None:
    invoke(
        context,
        "PUT",
        "/api/v1/admin/settings",
        admin=True,
        json_body={"settings": setting_values(setting)},
    )


@app.command("settings-reset")
def settings_reset(
    context: typer.Context,
    key: Annotated[list[str] | None, typer.Option("--key")] = None,
) -> None:
    invoke(
        context,
        "PUT",
        "/api/v1/admin/settings/reset",
        admin=True,
        json_body=key or [],
    )


@app.command("password")
def password(
    context: typer.Context,
    old_password: Annotated[str, typer.Option(prompt=True, hide_input=True)],
    new_password: Annotated[str, typer.Option(prompt=True, hide_input=True)],
) -> None:
    invoke(
        context,
        "PUT",
        "/api/v1/admin/settings/password",
        admin=True,
        json_body={"old_password": old_password, "new_password": new_password},
    )


@app.command("motto-refresh")
def motto_refresh(context: typer.Context) -> None:
    invoke(context, "PUT", "/api/v1/admin/settings/motto", admin=True)


@app.command("crawl")
def crawl(context: typer.Context) -> None:
    invoke(context, "PUT", "/api/v1/admin/settings/crawler", admin=True)


@app.command("stats")
def stats(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v1/admin/monitor/stats", admin=True)


@app.command("sysinfo")
def sysinfo(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v1/admin/monitor/sysinfo", admin=True)


@app.command("logs")
def logs(
    context: typer.Context,
    level: Annotated[str | None, typer.Option()] = None,
    module: Annotated[str | None, typer.Option()] = None,
    keyword: Annotated[str | None, typer.Option()] = None,
    page: Annotated[int, typer.Option(min=1)] = 1,
    size: Annotated[int, typer.Option(min=1)] = 20,
) -> None:
    params = {
        "level": level,
        "module": module,
        "keyword": keyword,
        "page": page,
        "size": size,
    }
    invoke(context, "GET", "/api/v1/admin/logs/", admin=True, params=params)


@app.command("log-modules")
def log_modules(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v1/admin/logs/modules", admin=True)
