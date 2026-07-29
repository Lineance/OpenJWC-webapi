from fastapi.routing import APIRoute
from typer.testing import CliRunner

from app.cli.app import cli
from app.cli.manifest import ROUTE_COMMANDS
from main import app


def test_manifest_covers_all_frontend_routes() -> None:
    routes = {
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute) and route.path.startswith("/api/")
        for method in route.methods
        if method not in {"HEAD", "OPTIONS"}
    }
    mapped = {(method, path) for method, path, _command in ROUTE_COMMANDS}
    assert mapped == routes


def test_every_manifest_command_is_discoverable() -> None:
    runner = CliRunner()
    for _method, _path, command in ROUTE_COMMANDS:
        result = runner.invoke(cli, [*command.split(), "--help"])
        assert result.exit_code == 0, f"{command}: {result.output}"
