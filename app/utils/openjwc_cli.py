from app.cli.app import run


class SQLCLI:
    """兼容旧入口并转发到新的 Typer CLI。"""

    def cmdloop(self) -> None:
        run()
