from app.infrastructure.agent.core.parser import parse_action, parse_action_detailed


def test_parse_action_with_valid_json() -> None:
    text = 'before {"tool": "search_keyword", "input": {"query": "补考"}} after'
    payload = parse_action(text)
    assert payload is not None
    assert payload["tool"] == "search_keyword"
    assert payload["input"]["query"] == "补考"


def test_parse_action_with_invalid_payload() -> None:
    text = "no action here"
    assert parse_action(text) is None


def test_parse_action_with_code_fence_and_trailing_comma() -> None:
    text = """
    ```json
    {
      "tool": "search_keyword",
      "input": {"query": "奖学金",},
    }
    ```
    """
    payload, error = parse_action_detailed(text, available_tools=["search_keyword"])
    assert error is None
    assert payload is not None
    assert payload["tool"] == "search_keyword"
    assert payload["input"]["query"] == "奖学金"


def test_parse_action_rejects_unavailable_tool() -> None:
    text = '{"tool":"sql_service","input":{"limit":1}}'
    payload, error = parse_action_detailed(text, available_tools=["search_keyword"])
    assert payload is None
    assert error is not None
    assert "not available" in error
