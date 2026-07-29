from typing import Annotated

import typer

from app.cli.client import ApiClient
from app.cli.commands import (
    admin_v1_content,
    admin_v1_system,
    admin_v1_users,
    admin_v2,
    client_v1_account,
    client_v1_content,
    client_v2,
)
from app.cli.config import CliConfig
from app.cli.ops import app as ops_app

cli = typer.Typer(
    name="openjwc",
    help="OpenJWC WebAPI 的现代命令行客户端与本地运维工具。",
    no_args_is_help=True,
)
client_v1 = typer.Typer(help="调用 v1 客户端接口。")
admin_v1 = typer.Typer(help="调用 v1 管理员接口。")
client_v1.add_typer(client_v1_account.app)
client_v1.add_typer(client_v1_content.app)
admin_v1.add_typer(admin_v1_system.app)
admin_v1.add_typer(admin_v1_content.app)
admin_v1.add_typer(admin_v1_users.app)
cli.add_typer(client_v1, name="client-v1")
cli.add_typer(admin_v1, name="admin-v1")
cli.add_typer(client_v2.app, name="client-v2")
cli.add_typer(admin_v2.app, name="admin-v2")
cli.add_typer(ops_app, name="ops")


@cli.callback()
def configure(
    context: typer.Context,
    base_url: Annotated[
        str,
        typer.Option(envvar="OPENJWC_BASE_URL", help="WebAPI 服务地址。"),
    ] = "http://127.0.0.1:8000",
    token: Annotated[
        str | None,
        typer.Option(envvar="OPENJWC_TOKEN", help="客户端 API Key 或 JWT。"),
    ] = None,
    admin_token: Annotated[
        str | None,
        typer.Option(envvar="OPENJWC_ADMIN_TOKEN", help="管理员 JWT。"),
    ] = None,
    device_id: Annotated[
        str | None,
        typer.Option(envvar="OPENJWC_DEVICE_ID", help="客户端设备 UUID。"),
    ] = None,
    client_version: Annotated[str, typer.Option()] = "cli-1.0.0",
    timeout: Annotated[float, typer.Option(min=0.1)] = 30.0,
    json_output: Annotated[bool, typer.Option("--json", help="输出原始 JSON。")]=False,
) -> None:
    config = CliConfig.from_options(
        base_url=base_url,
        token=token,
        admin_token=admin_token,
        device_id=device_id,
        client_version=client_version,
        timeout=timeout,
        json_output=json_output,
    )
    context.obj = ApiClient(config)


def run() -> None:
    cli()


if __name__ == "__main__":
    run()
