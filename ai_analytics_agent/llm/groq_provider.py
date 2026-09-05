import json
import os

from groq import Groq

DEFAULT_MAX_TOKENS = 2048


def call_llm(messages: list[dict], tools: list[dict], model: str, options: dict | None = None) -> dict:
    options = options or {}
    client = Groq(api_key=os.environ["GROQ_API_KEY"])

    kwargs = {
        "model": model,
        "messages": _to_openai_messages(messages),
        "tools": tools,
        "max_tokens": DEFAULT_MAX_TOKENS,
    }
    if "temperature" in options:
        kwargs["temperature"] = options["temperature"]

    response = client.chat.completions.create(**kwargs)
    return _from_openai_response(response)


def _to_openai_messages(messages: list[dict]) -> list[dict]:
    result = []
    pending_tool_call_ids = []

    for message in messages:
        role = message.get("role")

        if role == "tool":
            tool_call_id = pending_tool_call_ids.pop(0)
            result.append({
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": message["content"],
            })
            continue

        if role == "assistant" and message.get("tool_calls"):
            tool_calls_openai = []
            for i, call in enumerate(message["tool_calls"]):
                tool_call_id = f"call_{len(result)}_{i}"
                pending_tool_call_ids.append(tool_call_id)
                arguments = call["function"]["arguments"]
                if not isinstance(arguments, str):
                    arguments = json.dumps(arguments)
                tool_calls_openai.append({
                    "id": tool_call_id,
                    "type": "function",
                    "function": {
                        "name": call["function"]["name"],
                        "arguments": arguments,
                    },
                })
            result.append({
                "role": "assistant",
                "content": message.get("content") or "",
                "tool_calls": tool_calls_openai,
            })
            continue

        result.append({"role": role, "content": message.get("content", "")})

    return result


def _from_openai_response(response) -> dict:
    message = response.choices[0].message
    tool_calls = []
    if message.tool_calls:
        for call in message.tool_calls:
            arguments = call.function.arguments
            if isinstance(arguments, str):
                arguments = json.loads(arguments)
            tool_calls.append({
                "function": {
                    "name": call.function.name,
                    "arguments": arguments,
                },
            })

    return {
        "role": "assistant",
        "content": message.content or "",
        "tool_calls": tool_calls or None,
    }
