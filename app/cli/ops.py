from typing import Annotated

import typer

app = typer.Typer(help="无需 Web 服务的本地运维命令。")


@app.command("admins-sync")
def admins_sync() -> None:
    from app.infrastructure.storage.sqlite.sql_db_service import db

    typer.echo(db.sync_admins_from_config())


@app.command("settings-sync")
def settings_sync() -> None:
    from app.infrastructure.storage.sqlite.sql_db_service import db

    db._sync_settings()
    typer.echo("设置同步完成")


@app.command("diagnose")
def diagnose() -> None:
    import asyncio

    from app.utils.ping_check import diagnose_network_environment

    asyncio.run(diagnose_network_environment())


@app.command("crawl")
def crawl() -> None:
    from app.infrastructure.crawler.rust_crawler_wrapper import run_crawler_job

    run_crawler_job()


@app.command("db-drop")
def db_drop(
    table: Annotated[list[str], typer.Argument(help="仅允许删除白名单中的 SQLite 表。")],
) -> None:
    from app.infrastructure.storage.sqlite.sql_db_service import db

    for name in table:
        db.drop_table(name)
        typer.echo(f"已删除表：{name}")
