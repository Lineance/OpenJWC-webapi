import ast
import io
import tokenize
from pathlib import Path

ROOTS = (Path("app"), Path("tests"), Path("tools"))
MAX_LINES = 300
MAX_CLASSES = 2
MAX_FILES_PER_DIRECTORY = 8


def has_chinese(text: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in text)


def python_files() -> list[Path]:
    files = [Path("main.py")]
    for root in ROOTS:
        files.extend(root.rglob("*.py"))
    return sorted(files)


def audit_file(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    rows = text.splitlines()
    errors: list[str] = []
    if len(rows) > MAX_LINES:
        errors.append(f"{path}: 文件共 {len(rows)} 行，超过 {MAX_LINES} 行")
    tree = ast.parse(text)
    class_count = sum(isinstance(node, ast.ClassDef) for node in ast.walk(tree))
    if class_count > MAX_CLASSES:
        errors.append(f"{path}: 包含 {class_count} 个类，超过 {MAX_CLASSES} 个")
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        if node.args.vararg:
            arguments.append(node.args.vararg)
        if node.args.kwarg:
            arguments.append(node.args.kwarg)
        missing = [
            argument.arg
            for argument in arguments
            if argument.arg not in {"self", "cls"} and argument.annotation is None
        ]
        if missing:
            errors.append(f"{path}:{node.lineno}: 参数缺少类型：{', '.join(missing)}")
        if node.returns is None:
            errors.append(f"{path}:{node.lineno}: {node.name} 缺少返回类型")
    for token in tokenize.generate_tokens(io.StringIO(text).readline):
        if token.type != tokenize.COMMENT:
            continue
        before = rows[token.start[0] - 1][: token.start[1]]
        content = token.string.lstrip("#").strip()
        if before.strip():
            errors.append(f"{path}:{token.start[0]}: 禁止段中注释")
        if content and not has_chinese(content):
            errors.append(f"{path}:{token.start[0]}: 注释必须使用中文")
    return errors


def audit_directories(files: list[Path]) -> list[str]:
    counts: dict[Path, int] = {}
    for path in files:
        counts[path.parent] = counts.get(path.parent, 0) + 1
    return [
        f"{directory}: 包含 {count} 个 Python 文件，超过 {MAX_FILES_PER_DIRECTORY} 个"
        for directory, count in sorted(counts.items())
        if count > MAX_FILES_PER_DIRECTORY
    ]


def main() -> int:
    files = python_files()
    errors = audit_directories(files)
    for path in files:
        errors.extend(audit_file(path))
    if errors:
        print("\n".join(errors))
        return 1
    print(f"代码规范审计通过：{len(files)} 个 Python 文件")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
