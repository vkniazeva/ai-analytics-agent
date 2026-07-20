import json
from ai_analytics_agent.llm.client import call_llm, has_tool_calls, build_sales_tool_schema, build_wastage_tool_schema, \
    build_flight_catalog_tool_schema, build_product_catalog_tool_schema, build_pax_sales_catalog_tool_schema
from ai_analytics_agent.tools.flight_catalog_tools import get_flight_catalog_metric
from ai_analytics_agent.tools.pax_sales_tools import get_pax_sales_metric
from ai_analytics_agent.tools.product_catalog_tools import get_product_catalog_metric
from ai_analytics_agent.tools.sales_tools import get_sales_metric
from ai_analytics_agent.tools.wastage_tools import get_wastage_metric


AVAILABLE_FUNCTIONS = {"get_sales_metric": get_sales_metric,
                       "get_wastage_metric": get_wastage_metric,
                       "get_product_catalog_metric": get_product_catalog_metric,
                       "get_flight_catalog_metric": get_flight_catalog_metric,
                       "get_pax_sales_metric": get_pax_sales_metric
                       }

SYSTEM_PROMPT = {
    "role": "system",
    "content": (
        "You have tools to query sales, wastage, flight, product, and passenger data. "
        "Call the appropriate tool directly using the metrics and filters the user actually mentioned. "
        "Do not ask the user for a time period, filters, or grouping they did not request — "
        "if none are given, call the tool with no filters/group_by to return an all-time aggregate."
        "After a tool returns results, you must always write a natural-language answer summarizing them — never return an empty response."
    ),
}

MAX_ITERATIONS = 5


def run_agent(messages: list[dict]) -> tuple[str, list[dict]]:
    tools = [build_sales_tool_schema(), build_wastage_tool_schema(), build_flight_catalog_tool_schema(),
             build_product_catalog_tool_schema(), build_pax_sales_catalog_tool_schema()]

    if not messages or messages[0].get("role") != "system":
        messages = [SYSTEM_PROMPT] + messages

    for _ in range(MAX_ITERATIONS):
        message = call_llm(messages, tools=tools, options={"temperature": 0.2} )
        messages.append(message)

        if not has_tool_calls(message):
            return message["content"], messages

        for call in message["tool_calls"]:
            fn_name = call["function"]["name"]
            fn_args = call["function"]["arguments"]

            fn = AVAILABLE_FUNCTIONS[fn_name]
            try:
                result = fn(**fn_args)
            except Exception as e:
                result = {"error": str(e)}
            messages.append({"role": "tool", "content": json.dumps(result)})

    return "No response was generated after all allowed iterations", messages

# print(run_agent([{"role": "user", "content":
#     "Can you give me 10 worst selling products in category Cold Beverags in December 2025, sort from bottom to top? "
#     "And compare them with January 2026"}]))