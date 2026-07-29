from pathlib import Path
from typing import Annotated

import typer

from app.cli.output import invoke
from app.cli.parsing import text_value

app = typer.Typer(help="v1 客户端通知、搜索与投稿接口。")


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
        "/api/v1/client/notices",
        params={"label": label, "page": page, "size": size},
    )


@app.command("labels")
def labels(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v1/client/notices/labels")


@app.command("search")
def search(
    context: typer.Context,
    query: Annotated[str, typer.Argument()],
    top_k: Annotated[int, typer.Option(min=1, max=20)] = 5,
    min_similarity: Annotated[float | None, typer.Option(min=0.0, max=1.0)] = None,
) -> None:
    body = {"query": query, "top_k": top_k, "min_similarity": min_similarity}
    invoke(context, "POST", "/api/v1/client/notices/search", json_body=body)


@app.command("submit")
def submit(
    context: typer.Context,
    label: Annotated[str, typer.Option()],
    title: Annotated[str, typer.Option()],
    date: Annotated[str, typer.Option()],
    is_page: Annotated[bool, typer.Option("--page/--attachment")] = True,
    detail_url: Annotated[str | None, typer.Option()] = None,
    content: Annotated[str | None, typer.Option()] = None,
    content_file: Annotated[Path | None, typer.Option(exists=True, dir_okay=False)] = None,
    attachment_url: Annotated[list[str] | None, typer.Option("--attachment-url")] = None,
) -> None:
    body = {
        "label": label,
        "title": title,
        "date": date,
        "detail_url": detail_url,
        "is_page": is_page,
        "content": {
            "text": text_value(content, content_file),
            "attachment_urls": attachment_url or [],
        },
    }
    invoke(context, "POST", "/api/v1/client/submissions", json_body=body)


@app.command("submissions")
def submissions(context: typer.Context) -> None:
    invoke(context, "GET", "/api/v1/client/submissions/my")
