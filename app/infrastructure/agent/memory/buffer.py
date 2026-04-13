"""In-memory conversation window for agent sessions."""

from collections import defaultdict, deque
from typing import Any


class ConversationBuffer:
    def __init__(self, window_size: int = 5) -> None:
        self._window_size = window_size
        self._buffer: dict[str, deque[dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self._window_size)
        )

    def append(self, session_id: str, role: str, content: str) -> None:
        self._buffer[session_id].append({"role": role, "content": content})

    def read(self, session_id: str) -> list[dict[str, Any]]:
        return list(self._buffer.get(session_id, []))

    def clear(self, session_id: str) -> None:
        if session_id in self._buffer:
            del self._buffer[session_id]
