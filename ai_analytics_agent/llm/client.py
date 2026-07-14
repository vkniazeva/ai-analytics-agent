import json

from ai_analytics_agent.utils.config_handler import get_semantic_layer
import ollama

MODEL = "qwen2.5:14b-instruct"

def build_sales_tool_schema(semantic_layer: dict) -> dict:
    metric_names = list(semantic_layer["metrics"].keys())
    dimension_names = list(semantic_layer["dimensions"].keys())

    return {
        "type": "function",
        "function": {
            "name": "get_sales_metric",
            "description": "Get sales metrics (revenue, quantity, discounts, etc.), optionally grouped and filtered.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "description": "List of metrics to calculate.",
                        "items": {
                            "type": "string",
                            "enum": metric_names,
                        },
                    },
                    "group_by": {
                        "type": "array",
                        "description": "Optional list of dimensions to group results by.",
                        "items": {
                            "type": "string",
                            "enum": dimension_names
                        },
                    },
                    "filters": {
                        "type": "object",
                        "description": (
                                "Optional filters. Allowed keys: "
                                + ", ".join(dimension_names)
                                + ". Example: {\"year\": 2025}"
                        ),
                    },
                },
                "required": ["metrics"],
            },
        },
    }

def has_tool_calls(message) -> bool:
    return bool(message.get("tool_calls"))

def call_llm(messages, tools, model=MODEL):
    response = ollama.chat(model=model, messages=messages, tools=tools)
    return response["message"]

semantic_layer = get_semantic_layer()
# print(json.dumps(build_sales_tool_schema(semantic_layer), indent=2))

