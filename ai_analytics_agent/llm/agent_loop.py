import json
import logging
from ai_analytics_agent.llm.client import call_llm, has_tool_calls, build_sales_tool_schema, build_wastage_tool_schema, \
    build_flight_catalog_tool_schema, build_product_catalog_tool_schema, build_pax_sales_catalog_tool_schema
from ai_analytics_agent.tools.flight_catalog_tools import get_flight_catalog_metric
from ai_analytics_agent.tools.pax_sales_tools import get_pax_sales_metric
from ai_analytics_agent.tools.product_catalog_tools import get_product_catalog_metric
from ai_analytics_agent.tools.sales_tools import get_sales_metric
from ai_analytics_agent.tools.wastage_tools import get_wastage_metric
from ai_analytics_agent.utils.config_handler import SALES_METRIC, WASTAGE_METRIC, FLIGHT_METRIC, PRODUCT_METRIC, \
    PAX_SALES_METRIC

logger = logging.getLogger(__name__)


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

INITIAL_SYSTEM_PROMPT = {
    "role": "system",
    "content": (
        "You are an analytics assistant with tools to query sales, wastage, flight, product, and passenger data.\n\n"

        "## CORE RULES:\n"
        "1. Call tools directly - do NOT ask for clarification on filters/time periods unless the request is genuinely ambiguous\n"
        "2. If the request is ambiguous or can be interpreted multiple ways, present 2-3 specific options and ask user to choose\n"
        "3. After receiving tool results, ALWAYS write a natural-language summary - never return empty response\n"
        "4. Use the exact metrics and dimensions the user mentioned\n\n"

        "## COMMON PATTERNS:\n"
        "- 'best/top/worst N items' → group_by=['item_id'], order_by={metric: 'desc'/'asc'}, limit=N\n"
        "- 'by category/route/month' → add to group_by list\n"
        "- 'show/chart/visualize' → set visualize=true\n"
        "- 'compare X and Y' → use filters for each period, call tool twice\n"
        "- No time period specified → omit filters (all-time aggregate)\n\n"

        "## EXAMPLES:\n"
        "USER: 'show me 10 best selling items by revenue and sales quantity'\n"
        "YOU: Call get_sales_metric(metrics=['revenue', 'sales_quantity'], group_by=['item_id'], "
        "order_by={'revenue': 'desc'}, limit=10, visualize=true)\n\n"

        "USER: 'what were total sales in December 2025?'\n"
        "YOU: Call get_sales_metric(metrics=['revenue'], filters={'month_year': 'Dec 2025'})\n\n"

        "USER: 'top 5 routes by revenue'\n"
        "YOU: Call get_sales_metric(metrics=['revenue'], group_by=['route'], "
        "order_by={'revenue': 'desc'}, limit=5)\n\n"

        "USER: 'show wastage by category'\n"
        "YOU: If unclear (all categories? specific period?), ask: 'Would you like: "
        "1) All categories with total wastage, 2) Top 10 categories by wastage, or 3) Wastage trend by category over time?'\n\n"

        "Now answer user queries following these rules."
    ),
}

SYSTEM_PROMPT = {
    "role": "system",
    "content": (
        "You have tools to query sales, wastage, flight, product, and passenger data. "
        "Call the appropriate tool directly using the metrics and filters the user mentioned. "
        "For 'best/top/worst N items': use group_by=['item_id'], order_by, and limit. "
        "If the request is ambiguous, offer 2-3 specific options before calling tools. "
        "After tool returns results, always write a natural-language summary."
    ),
}

MAX_ITERATIONS = 5


def run_agent(messages: list[dict]) -> tuple[str, list[dict], dict | None]:
    tools = [build_sales_tool_schema(), build_wastage_tool_schema(), build_flight_catalog_tool_schema(),
             build_product_catalog_tool_schema(), build_pax_sales_catalog_tool_schema()]

    tool_call_count = 0
    chart_candidate = None

    # Use detailed prompt for first user query, shorter for subsequent
    if not messages or messages[0].get("role") != "system":
        # Check if this is the first query (only user messages, no assistant/tool messages)
        is_first_query = all(msg.get("role") == "user" for msg in messages)
        prompt = INITIAL_SYSTEM_PROMPT if is_first_query else SYSTEM_PROMPT
        messages = [prompt] + messages

    for iteration in range(MAX_ITERATIONS):
        logger.info(f"🤖 LLM call (iteration {iteration + 1}/{MAX_ITERATIONS})")
        message = call_llm(messages, tools=tools, options={"temperature": 0.2} )
        messages.append(message)

        if not has_tool_calls(message):
            logger.info(f"✅ Final response: {message['content'][:100]}...")
            chart = chart_candidate if tool_call_count == 1 and chart_candidate and chart_candidate["group_by"] else None
            return message["content"], messages, chart

        logger.info(f"🔧 Tool calls: {len(message['tool_calls'])}")
        for call in message["tool_calls"]:
            tool_call_count += 1
            fn_name = call["function"]["name"]
            fn_args = call["function"]["arguments"]
            requested_chart = fn_args.pop("visualize", False)

            logger.info(f"📞 Calling {fn_name}({json.dumps(fn_args, indent=2)})")

            fn = AVAILABLE_FUNCTIONS[fn_name]
            try:
                result = fn(**fn_args)
                logger.info(f"✓ Tool result: {len(result.get('rows', []))} rows")
                print(result)
            except Exception as e:
                logger.error(f"❌ Tool error: {str(e)}")
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