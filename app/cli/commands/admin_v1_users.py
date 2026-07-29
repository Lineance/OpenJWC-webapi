from typing import Annotated

import typer

from app.cli.output import invoke

app = typer.Typer(help="v1 管理员 API Key 管理接口。")


@app.command("apikey-create")
def apikey_create(
    context: typer.Context,
    owner_name: Annotated[str, typer.Argument()],
    max_devices: Annotated[int, typer.Option(min=1)] = 1,
) -> None:
    body = {"owner_name": owner_name, "max_devices": max_devices}
    invoke(context, "POST", "/api/v1/admin/apikeys", admin=True, json_body=body)


@app.command("apikeys")
def apikeys(
    context: typer.Context,
    page: Annotated[int, typer.Option(min=1)] = 1,
    size: Annotated[int, typer.Option(min=1, max=50)] = 20,
    keyword: Annotated[str | None, typer.Option()] = None,
) -> None:
    invoke(
        context,
        "GET",
        "/api/v1/admin/apikeys",
        admin=True,
        params={"page": page, "size": size, "keyword": keyword},
    )


@app.command("apikey-delete")
def apikey_delete(
    context: typer.Context,
    key_id: Annotated[int, typer.Argument()],
) -> None:
    invoke(context, "DELETE", f"/api/v1/admin/apikeys/{key_id}", admin=True)


@app.command("apikey-status")
def apikey_status(
    context: typer.Context,
    key_id: Annotated[int, typer.Argument()],
    active: Annotated[bool, typer.Option("--active/--inactive")],
) -> None:
    invoke(
        context,
        "PUT",
        f"/api/v1/admin/apikeys/{key_id}/status",
        admin=True,
        json_body={"is_active": active},
    )
