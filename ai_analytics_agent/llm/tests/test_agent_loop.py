import json
from unittest.mock import patch

from ai_analytics_agent.llm import agent_loop
from ai_analytics_agent.llm.agent_loop import run_agent, SYSTEM_PROMPT, MAX_ITERATIONS


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_returns_content_when_no_tool_calls(mock_call_llm):
    mock_call_llm.return_value = {"role": "assistant", "content": "Hello there", "tool_calls": None}

    answer, messages, chart = run_agent([{"role": "user", "content": "hi"}])

    assert answer == "Hello there"
    assert messages[-1]["content"] == "Hello there"
    assert chart is None


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_prepends_system_prompt_when_missing(mock_call_llm):
    mock_call_llm.return_value = {"role": "assistant", "content": "Hello there", "tool_calls": None}

    _, messages, _ = run_agent([{"role": "user", "content": "hi"}])

    assert messages[0] == SYSTEM_PROMPT


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_does_not_duplicate_system_prompt(mock_call_llm):
    mock_call_llm.return_value = {"role": "assistant", "content": "Hello there", "tool_calls": None}

    _, messages, _ = run_agent([SYSTEM_PROMPT, {"role": "user", "content": "hi"}])

    assert messages.count(SYSTEM_PROMPT) == 1


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_executes_tool_call_and_returns_final_answer(mock_call_llm):
    tool_call_message = {
        "role": "assistant",
        "content": "",
        "tool_calls": [{"function": {"name": "get_sales_metric", "arguments": {"metrics": ["revenue"]}}}],
    }
    final_message = {"role": "assistant", "content": "Revenue is 100", "tool_calls": None}
    mock_call_llm.side_effect = [tool_call_message, final_message]

    fake_result = {"rows": [{"revenue": 100}], "truncated": False}
    with patch.dict(agent_loop.AVAILABLE_FUNCTIONS, {"get_sales_metric": lambda **kwargs: fake_result}):
        answer, messages, chart = run_agent([{"role": "user", "content": "what's revenue?"}])

    assert answer == "Revenue is 100"
    tool_messages = [m for m in messages if m.get("role") == "tool"]
    assert json.loads(tool_messages[0]["content"]) == fake_result
    assert chart is None


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_captures_tool_exception_as_error(mock_call_llm):
    tool_call_message = {
        "role": "assistant",
        "content": "",
        "tool_calls": [{"function": {"name": "get_sales_metric", "arguments": {"metrics": ["revenue"]}}}],
    }
    final_message = {"role": "assistant", "content": "Something went wrong", "tool_calls": None}
    mock_call_llm.side_effect = [tool_call_message, final_message]

    def failing_tool(**kwargs):
        raise ValueError("boom")

    with patch.dict(agent_loop.AVAILABLE_FUNCTIONS, {"get_sales_metric": failing_tool}):
        answer, messages, chart = run_agent([{"role": "user", "content": "what's revenue?"}])

    assert answer == "Something went wrong"
    tool_messages = [m for m in messages if m.get("role") == "tool"]
    assert json.loads(tool_messages[0]["content"]) == {"error": "boom"}
    assert chart is None


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_stops_after_max_iterations(mock_call_llm):
    tool_call_message = {
        "role": "assistant",
        "content": "",
        "tool_calls": [{"function": {"name": "get_sales_metric", "arguments": {"metrics": ["revenue"]}}}],
    }
    mock_call_llm.return_value = tool_call_message

    with patch.dict(agent_loop.AVAILABLE_FUNCTIONS, {"get_sales_metric": lambda **kwargs: {"rows": [], "truncated": False}}):
        answer, messages, chart = run_agent([{"role": "user", "content": "what's revenue?"}])

    assert answer == "No response was generated after all allowed iterations"
    assert mock_call_llm.call_count == MAX_ITERATIONS
    assert chart is None


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_strips_visualize_before_calling_tool(mock_call_llm):
    tool_call_message = {
        "role": "assistant",
        "content": "",
        "tool_calls": [{
            "function": {
                "name": "get_sales_metric",
                "arguments": {"metrics": ["revenue"], "group_by": ["category"], "visualize": True},
            }
        }],
    }
    final_message = {"role": "assistant", "content": "Here's revenue by category", "tool_calls": None}
    mock_call_llm.side_effect = [tool_call_message, final_message]

    received_kwargs = {}

    def spy_tool(**kwargs):
        received_kwargs.update(kwargs)
        return {"rows": [{"category": "Snacks", "revenue": 100}], "truncated": False}

    with patch.dict(agent_loop.AVAILABLE_FUNCTIONS, {"get_sales_metric": spy_tool}):
        answer, messages, chart = run_agent([{"role": "user", "content": "revenue by category, show me a chart"}])

    assert "visualize" not in received_kwargs
    assert chart == {
        "domain": "sales",
        "metrics": ["revenue"],
        "group_by": ["category"],
        "rows": {"rows": [{"category": "Snacks", "revenue": 100}], "truncated": False},
    }


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_no_chart_when_visualize_not_set(mock_call_llm):
    tool_call_message = {
        "role": "assistant",
        "content": "",
        "tool_calls": [{
            "function": {
                "name": "get_sales_metric",
                "arguments": {"metrics": ["revenue"], "group_by": ["category"]},
            }
        }],
    }
    final_message = {"role": "assistant", "content": "Revenue by category", "tool_calls": None}
    mock_call_llm.side_effect = [tool_call_message, final_message]

    with patch.dict(agent_loop.AVAILABLE_FUNCTIONS, {
        "get_sales_metric": lambda **kwargs: {"rows": [{"category": "Snacks", "revenue": 100}], "truncated": False}
    }):
        _, _, chart = run_agent([{"role": "user", "content": "revenue by category"}])

    assert chart is None


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_no_chart_when_multiple_tool_calls(mock_call_llm):
    tool_call_message = {
        "role": "assistant",
        "content": "",
        "tool_calls": [
            {"function": {"name": "get_sales_metric", "arguments": {
                "metrics": ["revenue"], "group_by": ["category"], "visualize": True,
            }}},
            {"function": {"name": "get_sales_metric", "arguments": {
                "metrics": ["revenue"], "group_by": ["category"], "filters": {"month": 1},
            }}},
        ],
    }
    final_message = {"role": "assistant", "content": "Compared", "tool_calls": None}
    mock_call_llm.side_effect = [tool_call_message, final_message]

    with patch.dict(agent_loop.AVAILABLE_FUNCTIONS, {
        "get_sales_metric": lambda **kwargs: {"rows": [{"category": "Snacks", "revenue": 100}], "truncated": False}
    }):
        _, _, chart = run_agent([{"role": "user", "content": "compare Dec vs Jan"}])

    assert chart is None


@patch("ai_analytics_agent.llm.agent_loop.call_llm")
def test_run_agent_no_chart_when_group_by_empty(mock_call_llm):
    tool_call_message = {
        "role": "assistant",
        "content": "",
        "tool_calls": [{
            "function": {
                "name": "get_sales_metric",
                "arguments": {"metrics": ["revenue"], "visualize": True},
            }
        }],
    }
    final_message = {"role": "assistant", "content": "Total revenue is 100", "tool_calls": None}
    mock_call_llm.side_effect = [tool_call_message, final_message]

    with patch.dict(agent_loop.AVAILABLE_FUNCTIONS, {
        "get_sales_metric": lambda **kwargs: {"rows": [{"revenue": 100}], "truncated": False}
    }):
        _, _, chart = run_agent([{"role": "user", "content": "what's total revenue?"}])

    assert chart is None
