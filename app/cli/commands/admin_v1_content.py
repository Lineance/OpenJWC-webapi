from typing import Annotated

import typer

from app.cli.output import invoke

app = typer.Typer(help="v1 管理员通知与投稿审核接口。")


@app.command("notices")
def notices(
    context: typer.Context,
    label: Annotated[str | None, typer.Option()] = None,
    page: Annotated[int, typer.Option(min=1)] = 1,
    size: Annotated[int, typer.Option(min=1, max=50)] = 20,
) -> None:
    invoke(
        context,
        "GET",
        "/api/v1/admin/notices",
        admin=True,
        params={"label": label, "page": page, "size": size},
    )


@app.command("notice-labels")
def notice_labels(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v1/admin/notices/labels", admin=True)


@app.command("notice-delete")
def notice_delete(
    context: typer.Context,
    notice_id: Annotated[str, typer.Argument()],
) -> None:
    invoke(context, "DELETE", f"/api/v1/admin/notices/{notice_id}", admin=True)


@app.command("submissions")
def submissions(
    context: typer.Context,
    status: Annotated[str | None, typer.Option()] = None,
    page: Annotated[int, typer.Option(min=1)] = 1,
    size: Annotated[int, typer.Option(min=1)] = 20,
) -> None:
    invoke(
        context,
        "GET",
        "/api/v1/admin/submissions",
        admin=True,
        params={"status": status, "page": page, "size": size},
    )


@app.command("submission")
def submission(
    context: typer.Context,
    submission_id: Annotated[str, typer.Argument()],
) -> None:
    invoke(
        context,
        "GET",
        f"/api/v1/admin/submissions/{submission_id}",
        admin=True,
    )


@app.command("submission-review")
def submission_review(
    context: typer.Context,
    submission_id: Annotated[str, typer.Argument()],
    action: Annotated[str, typer.Option()],
    review: Annotated[str, typer.Option()] = "",
) -> None:
    invoke(
        context,
        "POST",
        f"/api/v1/admin/submissions/{submission_id}/review",
        admin=True,
        json_body={"action": action, "review": review},
    )
