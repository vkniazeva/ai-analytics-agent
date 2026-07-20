from ai_analytics_agent.tools.query_engine import get_metric
from ai_analytics_agent.utils.config_handler import PAX_SALES_METRIC


def get_pax_sales_metric(metrics: list[str], group_by: list[str] = None, filters: dict = None, order_by: dict = None, limit: int = None):
    return get_metric(PAX_SALES_METRIC, metrics, group_by, filters, order_by, limit)