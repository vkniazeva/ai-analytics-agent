from unittest.mock import patch, MagicMock

from ai_analytics_agent.llm.client import (
    build_sales_tool_schema,
    build_wastage_tool_schema,
    build_flight_catalog_tool_schema,
    build_product_catalog_tool_schema,
    build_pax_sales_catalog_tool_schema,
    has_tool_calls,
    call_llm,
)


def test_build_sales_tool_schema():
    schema = build_sales_tool_schema()
    assert schema["function"]["name"] == "get_sales_metric"
    metric_enum = schema["function"]["parameters"]["properties"]["metrics"]["items"]["enum"]
    assert "revenue" in metric_enum
    dimension_enum = schema["function"]["parameters"]["properties"]["group_by"]["items"]["enum"]
    assert "year" in dimension_enum


def test_build_wastage_tool_schema():
    schema = build_wastage_tool_schema()
    assert schema["function"]["name"] == "get_wastage_metric"
    metric_enum = schema["function"]["parameters"]["properties"]["metrics"]["items"]["enum"]
    assert len(metric_enum) > 0


def test_build_flight_catalog_tool_schema():
    schema = build_flight_catalog_tool_schema()
    assert schema["function"]["name"] == "get_flight_catalog_metric"
    metric_enum = schema["function"]["parameters"]["properties"]["metrics"]["items"]["enum"]
    assert "flight_count" in metric_enum


def test_build_product_catalog_tool_schema():
    schema = build_product_catalog_tool_schema()
    assert schema["function"]["name"] == "get_product_catalog_metric"
    metric_enum = schema["function"]["parameters"]["properties"]["metrics"]["items"]["enum"]
    assert "item_count" in metric_enum


def test_build_pax_sales_catalog_tool_schema():
    schema = build_pax_sales_catalog_tool_schema()
    assert schema["function"]["name"] == "get_pax_sales_metric"
    metric_enum = schema["function"]["parameters"]["properties"]["metrics"]["items"]["enum"]
    assert "avg_sales_per_passenger" in metric_enum
    assert "avg_items_per_passenger" in metric_enum


def test_has_tool_calls_true():
    assert has_tool_calls({"tool_calls": [{"function": {"name": "get_sales_metric"}}]}) is True


def test_has_tool_calls_false_when_missing():
    assert has_tool_calls({"content": "hello"}) is False


def test_has_tool_calls_false_when_empty_list():
    assert has_tool_calls({"tool_calls": []}) is False


@patch("ai_analytics_agent.llm.client.ollama")
def test_call_llm_calls_ollama_chat_with_expected_args(mock_ollama):
    mock_ollama.chat.return_value = {"message": {"role": "assistant", "content": "hi"}}
    messages = [{"role": "user", "content": "hi"}]
    tools = [{"type": "function", "function": {"name": "get_sales_metric"}}]

    result = call_llm(messages, tools=tools, model="gemma4", options={"temperature": 0.2})

    mock_ollama.chat.assert_called_once_with(
        model="gemma4", messages=messages, tools=tools, think=False, options={"temperature": 0.2}
    )
    assert result == {"role": "assistant", "content": "hi"}
