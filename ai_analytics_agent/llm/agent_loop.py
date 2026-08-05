import json
from ai_analytics_agent.llm.client import call_llm, has_tool_calls, build_sales_tool_schema, build_wastage_tool_schema, \
    build_flight_catalog_tool_schema, build_product_catalog_tool_schema, build_pax_sales_catalog_tool_schema
from ai_analytics_agent.tools.flight_catalog_tools import get_flight_catalog_metric
from ai_analytics_agent.tools.pax_sales_tools import get_pax_sales_metric
from ai_analytics_agent.tools.product_catalog_tools import get_product_catalog_metric
from ai_analytics_agent.tools.sales_tools import get_sales_metric
from ai_analytics_agent.tools.wastage_tools import get_wastage_metric
from ai_analytics_agent.utils.config_handler import SALES_METRIC, WASTAGE_METRIC, FLIGHT_METRIC, PRODUCT_METRIC, \
    PAX_SALES_METRIC


AVAILABLE_FUNCTIONS = {"get_sales_metric": get_sales_metric,
                       "get_wastage_metric": get_wastage_metric,
                       "get_product_catalog_metric": get_product_catalog_metric,
                       "get_flight_catalog_metric": get_flight_catalog_metric,
                       "get_pax_sales_metric": get_pax_sales_metric
                       }

FUNCTION_DOMAINS = {"get_sales_metric": SALES_METRIC,
                     "get_wastage_metric": WASTAGE_METRIC,
                     "get_product_catalog_metric": PRODUCT_METRIC,
                     "get_flight_catalog_metric": FLIGHT_METRIC,
                     "get_pax_sales_metric": PAX_SALES_METRIC
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


def run_agent(messages: list[dict]) -> tuple[str, list[dict], dict | None]:
    tools = [build_sales_tool_schema(), build_wastage_tool_schema(), build_flight_catalog_tool_schema(),
             build_product_catalog_tool_schema(), build_pax_sales_catalog_tool_schema()]

    tool_call_count = 0
    chart_candidate = None

    if not messages or messages[0].get("role") != "system":
        messages = [SYSTEM_PROMPT] + messages

    for _ in range(MAX_ITERATIONS):
        message = call_llm(messages, tools=tools, options={"temperature": 0.2} )
        messages.append(message)

        if not has_tool_calls(message):
            chart = chart_candidate if tool_call_count == 1 and chart_candidate and chart_candidate["group_by"] else None
            return message["content"], messages, chart

        for call in message["tool_calls"]:
            tool_call_count += 1
            fn_name = call["function"]["name"]
            fn_args = call["function"]["arguments"]
            requested_chart = fn_args.pop("visualize", False)

            fn = AVAILABLE_FUNCTIONS[fn_name]
            try:
                result = fn(**fn_args)
                print(result)
            except Exception as e:
                result = {"error": str(e)}
                requested_chart = False

            if requested_chart:
                chart_candidate = {
                    "domain": FUNCTION_DOMAINS.get(fn_name),
                    "metrics": fn_args.get("metrics", []),
                    "group_by": fn_args.get("group_by", []),
                    "rows": result,
                }

            messages.append({"role": "tool", "content": json.dumps(result)})

    return "No response was generated after all allowed iterations", messages, None

# print(run_agent([{"role": "user", "content":
#     "Can you give me 10 worst selling products in category Cold Beverags in December 2025, sort from bottom to top? "
#     "And compare them with January 2026"}]))