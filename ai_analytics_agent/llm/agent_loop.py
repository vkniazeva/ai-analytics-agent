import json
from ai_analytics_agent.llm.client import call_llm, has_tool_calls, build_sales_tool_schema
from ai_analytics_agent.tools.sales_tools import get_sales_metric
from ai_analytics_agent.utils.config_handler import get_semantic_layer

AVAILABLE_FUNCTIONS = {"get_sales_metric": get_sales_metric}
MAX_ITERATIONS = 5


def run_agent(question: str) -> str:
    semantic_layer = get_semantic_layer()
    tools = [build_sales_tool_schema(semantic_layer)]
    messages = [{"role": "user", "content": question}]

    for _ in range(MAX_ITERATIONS):
        message = call_llm(messages, tools=tools)

        if not has_tool_calls(message):
            return message["content"]

        messages.append(message)

        for call in message["tool_calls"]:
            fn_name = call["function"]["name"]
            fn_args = call["function"]["arguments"]

            fn = AVAILABLE_FUNCTIONS[fn_name]
            result = fn(**fn_args)

            messages.append({"role": "tool", "content": json.dumps(result)})

    return "No response was generated after all allowed iterations"

print(run_agent("Can you compute sales from December 2025 by items?"))