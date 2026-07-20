import json
import logging

from ai_analytics_agent.utils.config_handler import get_semantic_layer, ROW_LIMIT, SALES_METRIC, WASTAGE_METRIC, \
    FLIGHT_METRIC, PRODUCT_METRIC, PAX_SALES_METRIC
import ollama

# MODEL = "qwen2.5:14b-instruct"
# MODEL = "qwen3:14b"
MODEL = "gemma4"

def build_sales_tool_schema() -> dict:
    function_name = "get_sales_metric"
    description = "Get sales metrics (revenue, quantity, discounts, etc.), optionally grouped and filtered."
    return _build_tool_schema(SALES_METRIC, function_name, description)

def build_wastage_tool_schema() -> dict:
    function_name = "get_wastage_metric"
    description = "Get wastage metrics (load, wastage, fresh wastage, etc.), optionally grouped and filtered."
    return _build_tool_schema(WASTAGE_METRIC, function_name, description)

def build_flight_catalog_tool_schema() -> dict:
    function_name = "get_flight_catalog_metric"
    description = "Get flight catalog data (flight counts), usually grouped by date, day_period etc."
    return _build_tool_schema(FLIGHT_METRIC, function_name, description)

def build_product_catalog_tool_schema() -> dict:
    function_name = "get_product_catalog_metric"
    description = "Get product catalog data (items counts), usually grouped by category or item_type"
    return _build_tool_schema(PRODUCT_METRIC, function_name, description)

def build_pax_sales_catalog_tool_schema() -> dict:
    function_name = "get_pax_sales_metric"
    description = "Get passenger and sales data (like average sale per passenger,  optionally grouped and filtered."
    return _build_tool_schema(PAX_SALES_METRIC, function_name, description)


def _build_tool_schema(domain: str, function_name: str, description: str) -> dict:
    logging.debug(f"CALLING TOOL {domain}")
    semantic_layer = get_semantic_layer(domain)
    metric_names = list(semantic_layer["metrics"].keys())
    dimension_names = list(semantic_layer["dimensions"].keys())

    return {
        "type": "function",
        "function": {
            "name": function_name,
            "description": description,
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
                    "order_by": {
                        "type": "object",
                        "description": (
                            "Optional sorting. Keys must be one of the requested metrics or group_by "
                            "dimensions, values must be 'asc' or 'desc'. Example: {\"revenue\": \"desc\"}"
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Optional: return only top N rows (e.g. top 10 best selling items).",
                        "minimum": 1,
                        "maximum": ROW_LIMIT,
                    },
                },
                "required": ["metrics"],
            },
        },
    }



def has_tool_calls(message) -> bool:
    return bool(message.get("tool_calls"))

def call_llm(messages, tools, model=MODEL, options=None):
    response = ollama.chat(model=model, messages=messages, tools=tools, think=False, options=options)
    return response["message"]


