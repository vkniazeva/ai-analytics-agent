import json
from ai_analytics_agent.llm.client import call_llm, has_tool_calls, build_sales_tool_schema
from ai_analytics_agent.tools.sales_tools import get_sales_metric
from ai_analytics_agent.utils.config_handler import get_semantic_layer

AVAILABLE_FUNCTIONS = {"get_sales_metric": get_sales_metric}
MAX_ITERATIONS = 5


def run_agent(messages: list[dict]) -> tuple[str, list[dict]]:
    semantic_layer = get_semantic_layer()
    tools = [build_sales_tool_schema(semantic_layer)]

    for _ in range(MAX_ITERATIONS):
        message = call_llm(messages, tools=tools)
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