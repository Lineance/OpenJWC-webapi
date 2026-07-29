from typing import Annotated

import typer

from app.cli.output import invoke

app = typer.Typer(help="v2 管理员注册审核与用户管理接口。")


@app.command("registrations")
def registrations(
    context: typer.Context,
    status: Annotated[str | None, typer.Option()] = None,
    page: Annotated[int, typer.Option(min=1)] = 1,
    size: Annotated[int, typer.Option(min=1)] = 20,
) -> None:
    invoke(
        context,
        "GET",
        "/api/v2/admin/user-registrations",
        admin=True,
        params={"status": status, "page": page, "size": size},
    )


@app.command("registration")
def registration(
    context: typer.Context,
    registration_id: Annotated[str, typer.Argument()],
) -> None:
    invoke(
        context,
        "GET",
        f"/api/v2/admin/user-registrations/{registration_id}",
        admin=True,
    )


@app.command("registration-review")
def registration_review(
    context: typer.Context,
    registration_id: Annotated[str, typer.Argument()],
    action: Annotated[str, typer.Option()],
    review: Annotated[str, typer.Option()] = "",
) -> None:
    invoke(
        context,
        "POST",
        f"/api/v2/admin/user-registrations/{registration_id}/review",
        admin=True,
        json_body={"action": action, "review": review},
    )


@app.command("users")
def users(
    context: typer.Context,
    active: Annotated[bool | None, typer.Option("--active/--inactive")] = None,
    page: Annotated[int, typer.Option(min=1)] = 1,
    size: Annotated[int, typer.Option(min=1)] = 20,
) -> None:
    invoke(
        context,
        "GET",
        "/api/v2/admin/users",
        admin=True,
        params={"is_active": active, "page": page, "size": size},
    )


@app.command("user-status")
def user_status(
    context: typer.Context,
    user_id: Annotated[str, typer.Argument()],
    active: Annotated[bool, typer.Option("--active/--inactive")],
) -> None:
    invoke(
        context,
        "POST",
        f"/api/v2/admin/users/{user_id}/status",
        admin=True,
        json_body={"is_active": active},
    )


@app.command("user-delete")
def user_delete(
    context: typer.Context,
    user_id: Annotated[str, typer.Argument()],
) -> None:
    invoke(context, "DELETE", f"/api/v2/admin/users/{user_id}", admin=True)
