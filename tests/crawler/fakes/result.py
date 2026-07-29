from __future__ import annotations

class _FakeResult:
    def __init__(self, success: bool, links: dict[str, list[str]] | None = None) -> None:
        self.success = success
        self.links = links or {"internal": []}
