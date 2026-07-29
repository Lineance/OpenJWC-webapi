import json
from pathlib import Path
from typing import Any

import typer


def json_value(value: str | None, *, default: Any) -> Any:
    if value is None:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError as error:
        raise typer.BadParameter(f"JSON 格式无效：{error.msg}") from error


def text_value(value: str | None, file: Path | None) -> str:
    if file is not None:
        return file.read_text(encoding="utf-8")
    if value is None:
        raise typer.BadParameter("必须提供文本参数或文件参数")
    return value


def setting_values(items: list[str]) -> list[dict[str, str]]:
    settings: list[dict[str, str]] = []
    for item in items:
        key, separator, value = item.partition("=")
        if not separator or not key:
            raise typer.BadParameter(f"设置必须使用 key=value 格式：{item}")
        settings.append({"key": key, "value": value})
    return settings
