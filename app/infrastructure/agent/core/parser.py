"""Parser for extracting and validating tool actions from model output."""

import json
import re
from typing import Any


def _iter_json_candidates(text: str) -> list[str]:
    decoder = json.JSONDecoder()
    candidates: list[str] = []

    for idx, ch in enumerate(text):
        if ch != "{":
            continue
        try:
            _, end = decoder.raw_decode(text[idx:])
            snippet = text[idx : idx + end].strip()
            if snippet:
                candidates.append(snippet)
        except json.JSONDecodeError:
            continue

    fence_pattern = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)
    for match in fence_pattern.finditer(text):
        snippet = match.group(1).strip()
        if snippet:
            candidates.append(snippet)

    return candidates


def _repair_candidate(snippet: str) -> str:
    repaired = snippet.strip()
    repaired = repaired.replace("\u201c", '"').replace("\u201d", '"')
    repaired = repaired.replace("\u2018", "'").replace("\u2019", "'")
    repaired = re.sub(r",\s*([}\]])", r"\1", repaired)
    if '"' not in repaired and "'" in repaired:
        repaired = repaired.replace("'", '"')
    return repaired


def _normalize_action(
    payload: Any,
    *,
    available_tools: list[str] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(payload, dict):
        return None, "payload is not an object"

    normalized = dict(payload)

    if "tool" not in normalized and "action" in normalized:
        normalized["tool"] = normalized.pop("action")

    tool_name = str(normalized.get("tool", "")).strip()
    if not tool_name:
        return None, "missing tool"

    raw_input = normalized.get("input")
    if raw_input is None:
        raw_input = {}
    if not isinstance(raw_input, dict):
        return None, "input must be an object"

    if tool_name == "finish" and "answer" in normalized and "answer" not in raw_input:
        raw_input["answer"] = normalized.get("answer")

    if available_tools is not None and tool_name not in available_tools and tool_name != "finish":
        return None, f"tool '{tool_name}' is not available"

    normalized["tool"] = tool_name
    normalized["input"] = raw_input
    return normalized, None


def parse_action_detailed(
    text: str,
    *,
    available_tools: list[str] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Parse action JSON with strict schema validation and candidate repair."""
    if not text:
        return None, "empty response"

    errors: list[str] = []
    candidates = _iter_json_candidates(text)
    if not candidates:
        return None, "no json candidate found"

    for candidate in candidates:
        for snippet in (candidate, _repair_candidate(candidate)):
            try:
                payload = json.loads(snippet)
            except json.JSONDecodeError as exc:
                errors.append(f"json decode failed: {exc.msg}")
                continue

            normalized, error = _normalize_action(payload, available_tools=available_tools)
            if normalized is not None:
                return normalized, None
            if error:
                errors.append(error)

    return None, "; ".join(errors[-3:]) if errors else "action parse failed"


def parse_action(text: str) -> dict[str, Any] | None:
    """Backward-compatible parser returning action or None."""
    action, _ = parse_action_detailed(text)
    return action
