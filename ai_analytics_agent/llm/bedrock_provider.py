import os

from anthropic import AnthropicBedrockMantle

DEFAULT_MAX_TOKENS = 2048


def call_llm(messages: list[dict], tools: list[dict], model: str, options: dict | None = None) -> dict:
    options = options or {}
    system_text, rest = _split_system_and_messages(messages)
    client = AnthropicBedrockMantle(aws_region=os.environ.get("AWS_REGION", "eu-central-1"))

    kwargs = {}
    if system_text is not None:
        kwargs["system"] = system_text
    if "temperature" in options:
        kwargs["temperature"] = options["temperature"]

    response = client.messages.create(
        model=model,
        max_tokens=DEFAULT_MAX_TOKENS,
        messages=_to_anthropic_messages(rest),
        tools=_to_anthropic_tools(tools),
        **kwargs,
    )

    return _from_anthropic_response(response)


def _split_system_and_messages(messages: list[dict]) -> tuple[str | None, list[dict]]:
    system_text = None
    rest = []
    for message in messages:
        if message.get("role") == "system":
            system_text = message["content"]
        else:
            rest.append(message)
    return system_text, rest

def _to_anthropic_tools(tools: list[dict]) -> list[dict]:
    anthropic_tools = []
    for tool in tools:
        fn = tool["function"]
        anthropic_tools.append({
            "name": fn["name"],
            "description": fn["description"],
            "input_schema": fn["parameters"],
        })
    return anthropic_tools



def _to_anthropic_messages(messages: list[dict]) -> list[dict]:
    anthropic_messages = []
    pending_tool_use_ids = []

    for message in messages:
        role = message.get("role")

        if role == "tool":
            tool_use_id = pending_tool_use_ids.pop(0)
            anthropic_messages.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": message["content"],
                }],
            })
            continue

        if role == "assistant" and message.get("tool_calls"):
            content = []
            if message.get("content"):
                content.append({"type": "text", "text": message["content"]})
            for i, call in enumerate(message["tool_calls"]):
                tool_use_id = f"toolu_{len(anthropic_messages)}_{i}"
                pending_tool_use_ids.append(tool_use_id)
                content.append({
                    "type": "tool_use",
                    "id": tool_use_id,
                    "name": call["function"]["name"],
                    "input": call["function"]["arguments"],
                })
            anthropic_messages.append({"role": "assistant", "content": content})
            continue

        anthropic_messages.append({"role": role, "content": message.get("content", "")})

    return anthropic_messages

def _from_anthropic_response(response) -> dict:
    text_parts = []
    tool_calls = []

    for block in response.content:
        if block.type == "text":
            text_parts.append(block.text)
        elif block.type == "tool_use":
            tool_calls.append({"function": {"name": block.name, "arguments": block.input}})

    return {
        "role": "assistant",
        "content": "".join(text_parts),
        "tool_calls": tool_calls or None,
    }