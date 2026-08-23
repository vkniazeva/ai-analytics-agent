from ai_analytics_agent.llm.bedrock_provider import (
    _split_system_and_messages,
    _to_anthropic_tools,
    _to_anthropic_messages,
    _from_anthropic_response,
    call_llm,
)
from unittest.mock import patch, MagicMock

def test_split_system_and_messages_extracts_system():
    messages = [
        {"role": "system", "content": "be helpful"},
        {"role": "user", "content": "hi"},
    ]
    system_text, rest = _split_system_and_messages(messages)
    assert system_text == "be helpful"
    assert rest == [{"role": "user", "content": "hi"}]


def test_split_system_and_messages_no_system():
    messages = [{"role": "user", "content": "hi"}]
    system_text, rest = _split_system_and_messages(messages)
    assert system_text is None
    assert rest == messages


def test_to_anthropic_tools_converts_function_schema():
    tools = [{
        "type": "function",
        "function": {
            "name": "get_sales_metric",
            "description": "Get sales metrics",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    }]
    result = _to_anthropic_tools(tools)
    assert result == [{
        "name": "get_sales_metric",
        "description": "Get sales metrics",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    }]


def test_to_anthropic_messages_passes_through_plain_text():
    messages = [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hello", "tool_calls": None},
    ]
    result = _to_anthropic_messages(messages)
    assert result == [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hello"},
    ]

def test_to_anthropic_messages_converts_tool_call_and_result():
    messages = [
        {"role": "user", "content": "what's revenue?"},
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [{"function": {"name": "get_sales_metric", "arguments": {"metrics": ["revenue"]}}}],
        },
        {"role": "tool", "content": '{"revenue": 100}'},
    ]
    result = _to_anthropic_messages(messages)

    assert result[0] == {"role": "user", "content": "what's revenue?"}
    assert result[1]["role"] == "assistant"
    tool_use_block = result[1]["content"][0]
    assert tool_use_block["type"] == "tool_use"
    assert tool_use_block["name"] == "get_sales_metric"
    assert tool_use_block["input"] == {"metrics": ["revenue"]}

    assert result[2]["role"] == "user"
    tool_result_block = result[2]["content"][0]
    assert tool_result_block["type"] == "tool_result"
    assert tool_result_block["tool_use_id"] == tool_use_block["id"]
    assert tool_result_block["content"] == '{"revenue": 100}'


def test_to_anthropic_messages_matches_multiple_tool_calls_in_order():
    messages = [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {"function": {"name": "get_sales_metric", "arguments": {"metrics": ["revenue"]}}},
                {"function": {"name": "get_wastage_metric", "arguments": {"metrics": ["wastage"]}}},
            ],
        },
        {"role": "tool", "content": '{"revenue": 100}'},
        {"role": "tool", "content": '{"wastage": 5}'},
    ]
    result = _to_anthropic_messages(messages)

    first_id = result[0]["content"][0]["id"]
    second_id = result[0]["content"][1]["id"]
    assert result[1]["content"][0]["tool_use_id"] == first_id
    assert result[1]["content"][0]["content"] == '{"revenue": 100}'
    assert result[2]["content"][0]["tool_use_id"] == second_id
    assert result[2]["content"][0]["content"] == '{"wastage": 5}'


def test_from_anthropic_response_text_only():
    block = MagicMock(type="text", text="hello there")
    response = MagicMock(content=[block])

    result = _from_anthropic_response(response)

    assert result == {"role": "assistant", "content": "hello there", "tool_calls": None}


def test_from_anthropic_response_with_tool_use():
    tool_block = MagicMock(type="tool_use", input={"metrics": ["revenue"]})
    tool_block.name = "get_sales_metric"
    response = MagicMock(content=[tool_block])

    result = _from_anthropic_response(response)

    assert result["content"] == ""
    assert result["tool_calls"] == [{"function": {"name": "get_sales_metric", "arguments": {"metrics": ["revenue"]}}}]


@patch("ai_analytics_agent.llm.bedrock_provider.AnthropicBedrockMantle")
def test_call_llm_invokes_bedrock_client_and_normalizes_response(mock_mantle_cls):
    mock_client = MagicMock()
    mock_mantle_cls.return_value = mock_client
    mock_client.messages.create.return_value = MagicMock(content=[MagicMock(type="text", text="hi")])

    messages = [{"role": "system", "content": "be helpful"}, {"role": "user", "content": "hi"}]
    tools = [{"type": "function", "function": {"name": "get_sales_metric", "description": "d", "parameters": {}}}]

    result = call_llm(messages, tools, model="anthropic.claude-haiku-4-5-v1:0", options={"temperature": 0.2})

    mock_mantle_cls.assert_called_once()
    _, call_kwargs = mock_client.messages.create.call_args
    assert call_kwargs["model"] == "anthropic.claude-haiku-4-5-v1:0"
    assert call_kwargs["system"] == "be helpful"
    assert call_kwargs["temperature"] == 0.2
    assert call_kwargs["messages"] == [{"role": "user", "content": "hi"}]
    assert result == {"role": "assistant", "content": "hi", "tool_calls": None}